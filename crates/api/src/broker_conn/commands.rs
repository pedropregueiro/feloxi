//! Broker command execution.
//!
//! Publishes Celery tasks (retry) and broadcasts control commands (revoke, shutdown)
//! directly to the broker.

use base64::Engine;

use fred::prelude::*;
use serde::Serialize;
use utoipa::ToSchema;
use uuid::Uuid;

/// Queue depth information.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct QueueInfo {
    pub queue_name: String,
    pub depth: u64,
}

// ─── Redis Commands ──────────────────────────────────────────────────────────

/// Publish a new task to a Redis broker queue using the Kombu envelope format.
pub async fn redis_publish_task(
    connection_url: &str,
    task_name: &str,
    task_id: &str,
    args: &serde_json::Value,
    kwargs: &serde_json::Value,
    queue: &str,
    parent_id: Option<&str>,
    root_id: Option<&str>,
) -> Result<(), String> {
    let effective_root = root_id.unwrap_or(task_id);

    let body = serde_json::json!([
        args,
        kwargs,
        {"callbacks": null, "errbacks": null, "chain": null, "chord": null}
    ]);
    let body_json = serde_json::to_string(&body).map_err(|e| e.to_string())?;
    // Kombu's Redis transport decodes the body per `properties.body_encoding`,
    // so the body has to actually be base64 when we say it is.
    let body_str = base64::engine::general_purpose::STANDARD.encode(body_json.as_bytes());

    let envelope = serde_json::json!({
        "body": body_str,
        "content-encoding": "utf-8",
        "content-type": "application/json",
        "headers": {
            "lang": "py",
            "task": task_name,
            "id": task_id,
            "root_id": effective_root,
            "parent_id": parent_id,
            "group": null,
            "origin": "feloxi",
            "argsrepr": serde_json::to_string(args).unwrap_or_default(),
            "kwargsrepr": serde_json::to_string(kwargs).unwrap_or_default(),
        },
        "properties": {
            "correlation_id": task_id,
            "reply_to": "",
            "delivery_mode": 2,
            "delivery_info": {
                "exchange": "",
                "routing_key": queue,
            },
            "priority": 0,
            "body_encoding": "base64",
            "delivery_tag": Uuid::new_v4().to_string(),
        }
    });

    let msg = serde_json::to_string(&envelope).map_err(|e| e.to_string())?;
    let (client, broker) = connect_redis(connection_url).await?;
    let result = client
        .lpush::<(), _, _>(broker.key(queue), msg.as_str())
        .await
        .map_err(|e| format!("Failed to LPUSH task: {e}"));
    let _ = client.quit().await;

    result
}

/// Broadcast a Celery remote control command via Redis PUBLISH to the pidbox.
async fn redis_broadcast_control(
    connection_url: &str,
    method: &str,
    arguments: serde_json::Value,
    destination: Option<&[&str]>,
) -> Result<(), String> {
    let control_msg = serde_json::json!({
        "method": method,
        "arguments": arguments,
        "destination": destination,
        "pattern": null,
        "matcher": null,
    });

    let envelope = redis_control_envelope(&control_msg, &Uuid::new_v4().to_string())?;
    let msg = serde_json::to_string(&envelope).map_err(|e| e.to_string())?;
    let (client, broker) = connect_redis(connection_url).await?;
    let channel = broker.key(&redis_pidbox_channel(parse_redis_db(&broker.url)));
    let result = client
        .publish::<(), _, _>(channel.as_str(), msg.as_str())
        .await
        .map_err(|e| format!("Failed to PUBLISH control command: {e}"));
    let _ = client.quit().await;

    result
}

/// Pub/sub channel Kombu's Redis transport uses for the `celery.pidbox`
/// fanout exchange: `keyprefix_fanout` (`/{db}.`) followed by the exchange name.
fn redis_pidbox_channel(db: u32) -> String {
    format!("/{db}.celery.pidbox")
}

/// Wrap a pidbox control message in the Kombu envelope the worker's Redis
/// transport expects on fanout channels (it reads `properties.delivery_tag`
/// and decodes the body per `properties.body_encoding`).
fn redis_control_envelope(
    control_msg: &serde_json::Value,
    delivery_tag: &str,
) -> Result<serde_json::Value, String> {
    let body_json = serde_json::to_string(control_msg).map_err(|e| e.to_string())?;
    let body_str = base64::engine::general_purpose::STANDARD.encode(body_json.as_bytes());

    Ok(serde_json::json!({
        "body": body_str,
        "content-encoding": "utf-8",
        "content-type": "application/json",
        "headers": {
            "clock": 0,
            "expires": 0,
        },
        "properties": {
            "delivery_info": {
                "exchange": "celery.pidbox",
                "routing_key": "",
            },
            "priority": 0,
            "body_encoding": "base64",
            "delivery_tag": delivery_tag,
        }
    }))
}

/// Revoke a task via Redis pidbox broadcast.
pub async fn redis_revoke_task(
    connection_url: &str,
    task_id: &str,
    terminate: bool,
) -> Result<(), String> {
    let signal = if terminate { "SIGTERM" } else { "SIGQUIT" };
    redis_broadcast_control(
        connection_url,
        "revoke",
        serde_json::json!({
            "task_id": task_id,
            "terminate": terminate,
            "signal": signal,
        }),
        None,
    )
    .await
}

/// Shutdown a worker via Redis pidbox broadcast.
pub async fn redis_shutdown_worker(connection_url: &str, hostname: &str) -> Result<(), String> {
    redis_broadcast_control(connection_url, "shutdown", serde_json::json!({}), Some(&[hostname]))
        .await
}

/// Get queue depths from a Redis broker.
///
/// Celery + Kombu register each queue binding in a set at
/// `_kombu.binding.<exchange>`. We SCAN for all of those keys, drop the
/// well-known internal exchanges (event bus, pidbox, replies) that shouldn't
/// surface in the task-queue list, and read the queue names from the members.
pub async fn redis_queue_stats(connection_url: &str) -> Result<Vec<QueueInfo>, String> {
    use futures::StreamExt;

    const BINDING_PREFIX: &str = "_kombu.binding.";

    let (client, broker) = connect_redis(connection_url).await?;
    let binding_prefix = broker.key(BINDING_PREFIX);

    let mut binding_keys: Vec<String> = Vec::new();
    let mut stream = client.scan_buffered(format!("{binding_prefix}*"), Some(200), None);
    while let Some(item) = stream.next().await {
        let Ok(key) = item else { continue };
        let key_str = key.as_str_lossy();
        if let Some(exchange) = key_str.strip_prefix(binding_prefix.as_str()) {
            if !is_internal_binding(exchange) {
                binding_keys.push(key_str.to_string());
            }
        }
    }

    let mut queue_names: Vec<String> = Vec::new();
    for key in &binding_keys {
        let members: Vec<String> = client.smembers(key.as_str()).await.unwrap_or_default();
        queue_names
            .extend(members.iter().filter_map(|m| binding_member_queue(m)).map(String::from));
    }

    queue_names.sort();
    queue_names.dedup();

    let mut stats = Vec::with_capacity(queue_names.len());
    for name in &queue_names {
        let depth: u64 = client.llen::<u64, _>(broker.key(name)).await.unwrap_or(0);
        stats.push(QueueInfo { queue_name: name.clone(), depth });
    }
    let _ = client.quit().await;

    Ok(stats)
}

/// Kombu binding names that correspond to Celery's internal control plane
/// rather than user-visible task queues.
fn is_internal_binding(name: &str) -> bool {
    name == "celeryev" || name.ends_with(".pidbox") || name.starts_with("reply.")
}

/// Queue name from a `_kombu.binding.<exchange>` set member, which Kombu
/// stores as `routing_key`, `pattern` and `queue` joined by `\x06\x16`.
pub(crate) fn binding_member_queue(member: &str) -> Option<&str> {
    member.split("\x06\x16").nth(2).filter(|q| !q.is_empty())
}

// ─── AMQP Commands ───────────────────────────────────────────────────────────

/// Publish a new task to an AMQP broker queue.
pub async fn amqp_publish_task(
    connection_url: &str,
    task_name: &str,
    task_id: &str,
    args: &serde_json::Value,
    kwargs: &serde_json::Value,
    queue: &str,
    parent_id: Option<&str>,
    root_id: Option<&str>,
) -> Result<(), String> {
    let conn = connect_amqp(connection_url).await?;
    let channel =
        conn.create_channel().await.map_err(|e| format!("Failed to create AMQP channel: {e}"))?;

    let body = serde_json::json!([
        args,
        kwargs,
        {"callbacks": null, "errbacks": null, "chain": null, "chord": null}
    ]);
    let payload = serde_json::to_vec(&body).map_err(|e| e.to_string())?;

    let properties = lapin::BasicProperties::default()
        .with_correlation_id(task_id.into())
        .with_content_type("application/json".into())
        .with_content_encoding("utf-8".into())
        .with_delivery_mode(2) // persistent
        .with_headers({
            let mut headers = lapin::types::FieldTable::default();
            headers.insert("task".into(), lapin::types::AMQPValue::LongString(task_name.into()));
            headers.insert("id".into(), lapin::types::AMQPValue::LongString(task_id.into()));
            headers.insert(
                "root_id".into(),
                lapin::types::AMQPValue::LongString(root_id.unwrap_or(task_id).into()),
            );
            if let Some(pid) = parent_id {
                headers.insert("parent_id".into(), lapin::types::AMQPValue::LongString(pid.into()));
            }
            headers.insert("lang".into(), lapin::types::AMQPValue::LongString("py".into()));
            headers.insert("origin".into(), lapin::types::AMQPValue::LongString("feloxi".into()));
            headers
        });

    channel
        .basic_publish(
            "", // default exchange
            queue,
            lapin::options::BasicPublishOptions::default(),
            &payload,
            properties,
        )
        .await
        .map_err(|e| format!("Failed to publish task: {e}"))?
        .await
        .map_err(|e| format!("Publisher confirm failed: {e}"))?;

    let _ = channel.close(200, "done").await;
    let _ = conn.close(200, "done").await;

    Ok(())
}

/// Broadcast a Celery remote control command via AMQP fanout exchange (pidbox).
async fn amqp_broadcast_control(
    connection_url: &str,
    method: &str,
    arguments: serde_json::Value,
    destination: Option<&[&str]>,
) -> Result<(), String> {
    let conn = connect_amqp(connection_url).await?;
    let channel =
        conn.create_channel().await.map_err(|e| format!("Failed to create AMQP channel: {e}"))?;

    // Declare the pidbox fanout exchange
    channel
        .exchange_declare(
            "celery.pidbox",
            lapin::ExchangeKind::Fanout,
            lapin::options::ExchangeDeclareOptions::default(),
            lapin::types::FieldTable::default(),
        )
        .await
        .map_err(|e| format!("Failed to declare pidbox exchange: {e}"))?;

    let control_msg = serde_json::json!({
        "method": method,
        "arguments": arguments,
        "destination": destination,
        "pattern": null,
        "matcher": null,
    });

    let payload = serde_json::to_vec(&control_msg).map_err(|e| e.to_string())?;

    channel
        .basic_publish(
            "celery.pidbox",
            "",
            lapin::options::BasicPublishOptions::default(),
            &payload,
            lapin::BasicProperties::default().with_content_type("application/json".into()),
        )
        .await
        .map_err(|e| format!("Failed to publish control command: {e}"))?;

    let _ = channel.close(200, "done").await;
    let _ = conn.close(200, "done").await;

    Ok(())
}

/// Revoke a task via AMQP pidbox broadcast.
pub async fn amqp_revoke_task(
    connection_url: &str,
    task_id: &str,
    terminate: bool,
) -> Result<(), String> {
    let signal = if terminate { "SIGTERM" } else { "SIGQUIT" };
    amqp_broadcast_control(
        connection_url,
        "revoke",
        serde_json::json!({
            "task_id": task_id,
            "terminate": terminate,
            "signal": signal,
        }),
        None,
    )
    .await
}

/// Shutdown a worker via AMQP pidbox broadcast.
pub async fn amqp_shutdown_worker(connection_url: &str, hostname: &str) -> Result<(), String> {
    amqp_broadcast_control(connection_url, "shutdown", serde_json::json!({}), Some(&[hostname]))
        .await
}

// ─── Unified API ─────────────────────────────────────────────────────────────

/// Publish a task to the correct broker based on type.
#[allow(clippy::too_many_arguments)]
pub async fn publish_task(
    broker_type: &str,
    connection_url: &str,
    task_name: &str,
    task_id: &str,
    args: &serde_json::Value,
    kwargs: &serde_json::Value,
    queue: &str,
    parent_id: Option<&str>,
    root_id: Option<&str>,
) -> Result<(), String> {
    match broker_type {
        "redis" => {
            redis_publish_task(
                connection_url,
                task_name,
                task_id,
                args,
                kwargs,
                queue,
                parent_id,
                root_id,
            )
            .await
        }
        "rabbitmq" | "amqp" => {
            amqp_publish_task(
                connection_url,
                task_name,
                task_id,
                args,
                kwargs,
                queue,
                parent_id,
                root_id,
            )
            .await
        }
        other => Err(format!("Unsupported broker type: {other}")),
    }
}

/// Revoke a task via the correct broker.
pub async fn revoke_task(
    broker_type: &str,
    connection_url: &str,
    task_id: &str,
    terminate: bool,
) -> Result<(), String> {
    match broker_type {
        "redis" => redis_revoke_task(connection_url, task_id, terminate).await,
        "rabbitmq" | "amqp" => amqp_revoke_task(connection_url, task_id, terminate).await,
        other => Err(format!("Unsupported broker type: {other}")),
    }
}

/// Shutdown a worker via the correct broker.
pub async fn shutdown_worker(
    broker_type: &str,
    connection_url: &str,
    hostname: &str,
) -> Result<(), String> {
    match broker_type {
        "redis" => redis_shutdown_worker(connection_url, hostname).await,
        "rabbitmq" | "amqp" => amqp_shutdown_worker(connection_url, hostname).await,
        other => Err(format!("Unsupported broker type: {other}")),
    }
}

/// Get queue stats from the correct broker.
///
/// Redis enumerates queues itself (kombu binding keys). AMQP has no
/// list-queues operation in the protocol, so `candidate_queues` supplies the
/// names to probe (event-derived + previously observed); unknown names are
/// skipped, existing ones report their live depth.
pub async fn queue_stats(
    broker_type: &str,
    connection_url: &str,
    candidate_queues: &[String],
) -> Result<Vec<QueueInfo>, String> {
    match broker_type {
        "redis" => redis_queue_stats(connection_url).await,
        "rabbitmq" | "amqp" => amqp_queue_stats(connection_url, candidate_queues).await,
        other => Err(format!("Unsupported broker type: {other}")),
    }
}

/// AMQP queue depths via passive declare (works on any RabbitMQ, no
/// management plugin needed). A failed passive declare closes the channel by
/// protocol, so each probe uses its own channel on the shared connection.
async fn amqp_queue_stats(
    connection_url: &str,
    candidate_queues: &[String],
) -> Result<Vec<QueueInfo>, String> {
    // Always probe Celery's default queue so a fresh install shows something
    // even before any names have been observed.
    let mut names: Vec<&str> = candidate_queues.iter().map(String::as_str).collect();
    if !names.contains(&"celery") {
        names.push("celery");
    }
    names.sort_unstable();
    names.dedup();

    let conn = connect_amqp(connection_url).await?;
    let mut stats = Vec::new();
    for name in names {
        let Ok(channel) = conn.create_channel().await else { break };
        let opts = lapin::options::QueueDeclareOptions { passive: true, ..Default::default() };
        // A failed passive declare means the queue doesn't exist (or no
        // access): skip it. The failure also closes the channel by protocol,
        // which is why each probe gets its own.
        if let Ok(q) = channel.queue_declare(name, opts, lapin::types::FieldTable::default()).await
        {
            stats.push(QueueInfo { queue_name: name.to_string(), depth: q.message_count() as u64 });
            let _ = channel.close(200, "done").await;
        }
    }
    let _ = conn.close(200, "done").await;
    Ok(stats)
}

/// Purge all messages from a queue (Redis: DEL, AMQP: queue.purge).
pub async fn purge_queue(
    broker_type: &str,
    connection_url: &str,
    queue_name: &str,
) -> Result<u64, String> {
    match broker_type {
        "redis" => redis_purge_queue(connection_url, queue_name).await,
        "rabbitmq" | "amqp" => amqp_purge_queue(connection_url, queue_name).await,
        other => Err(format!("Unsupported broker type: {other}")),
    }
}

async fn redis_purge_queue(connection_url: &str, queue_name: &str) -> Result<u64, String> {
    let (client, broker) = connect_redis(connection_url).await?;
    let key = broker.key(queue_name);
    let count: u64 = client.llen(&key).await.unwrap_or(0);
    let result = client.del::<(), _>(&key).await.map_err(|e| format!("Failed to DEL queue: {e}"));
    let _ = client.quit().await;
    result.map(|()| count)
}

async fn amqp_purge_queue(connection_url: &str, queue_name: &str) -> Result<u64, String> {
    let conn = connect_amqp(connection_url).await?;
    let channel =
        conn.create_channel().await.map_err(|e| format!("Failed to create AMQP channel: {e}"))?;
    let purged: u32 = channel
        .queue_purge(queue_name, lapin::options::QueuePurgeOptions::default())
        .await
        .map_err(|e| format!("Failed to purge AMQP queue: {e}"))?;
    let _ = channel.close(200, "done").await;
    let _ = conn.close(200, "done").await;
    Ok(purged as u64)
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Callers must `quit()` the client when done: with unlimited reconnects, fred
/// keeps the connection open until QUIT, even after the client is dropped.
async fn connect_redis(url: &str) -> Result<(Client, RedisBrokerUrl), String> {
    let broker = RedisBrokerUrl::parse(url);
    let config =
        Config::from_url(&broker.url).map_err(|e| redact(format!("Invalid Redis URL: {e}")))?;
    let mut builder = Builder::from_config(config);
    builder.set_policy(ReconnectPolicy::new_constant(0, 3000));
    let client =
        builder.build().map_err(|e| redact(format!("Failed to build Redis client: {e}")))?;

    let Ok(init) = tokio::time::timeout(std::time::Duration::from_secs(5), client.init()).await
    else {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), client.quit()).await;
        return Err("Redis connection timed out".to_string());
    };
    init.map_err(|e| redact(format!("Failed to connect to Redis: {e}")))?;

    Ok((client, broker))
}

async fn connect_amqp(url: &str) -> Result<lapin::Connection, String> {
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        lapin::Connection::connect(url, lapin::ConnectionProperties::default()),
    )
    .await
    .map_err(|_| "AMQP connection timed out".to_string())?
    .map_err(|e| redact(format!("Failed to connect to AMQP: {e}")))
}

fn redact(message: String) -> String {
    common::redact::redact_url_credentials(&message)
}

pub(crate) fn parse_redis_db(url: &str) -> u32 {
    url.rsplit('/')
        .next()
        .map(|s| s.split('?').next().unwrap_or(s))
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0)
}

/// A Redis broker URL split into the part fred understands and Kombu's
/// `global_keyprefix` transport option, which Feloxi reads from the URL query
/// (`redis://host:6379/0?global_keyprefix=myapp.`). Kombu prepends the prefix
/// to every key and pub/sub channel it uses, so Feloxi has to as well.
pub(crate) struct RedisBrokerUrl {
    /// The URL with `global_keyprefix` removed.
    pub url: String,
    /// The key prefix, empty when not set.
    pub prefix: String,
}

impl RedisBrokerUrl {
    pub fn parse(raw: &str) -> Self {
        const PARAM: &str = "global_keyprefix";

        let Some((base, query)) = raw.split_once('?') else {
            return Self { url: raw.to_string(), prefix: String::new() };
        };

        let mut prefix = String::new();
        let mut kept: Vec<&str> = Vec::new();
        for pair in query.split('&') {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            if key == PARAM {
                prefix = value.to_string();
            } else {
                kept.push(pair);
            }
        }

        let url =
            if kept.is_empty() { base.to_string() } else { format!("{base}?{}", kept.join("&")) };
        Self { url, prefix }
    }

    /// The Redis key (or channel) Kombu uses for `name`.
    pub fn key(&self, name: &str) -> String {
        format!("{}{name}", self.prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_redis_db_default() {
        assert_eq!(parse_redis_db("redis://localhost:6379"), 0);
    }

    #[test]
    fn parse_redis_db_explicit() {
        assert_eq!(parse_redis_db("redis://localhost:6379/1"), 1);
        assert_eq!(parse_redis_db("redis://localhost:6379/15"), 15);
    }

    #[test]
    fn parse_redis_db_with_params() {
        // When there's a query string, the "db" portion won't parse as u8
        assert_eq!(parse_redis_db("redis://localhost:6379/0?timeout=5"), 0);
    }

    #[test]
    fn redis_broker_url_without_prefix_is_unchanged() {
        // No global_keyprefix: the URL passes through as-is and keys are unprefixed
        for raw in ["redis://localhost:6379/0", "redis://:pw@localhost:6379/1?timeout=5"] {
            let broker = RedisBrokerUrl::parse(raw);
            assert_eq!(broker.url, raw);
            assert_eq!(broker.prefix, "");
            assert_eq!(broker.key("emails"), "emails");
        }
    }

    #[test]
    fn redis_broker_url_strips_prefix() {
        // The option is removed so fred and parse_redis_db only see the plain URL
        let broker = RedisBrokerUrl::parse("redis://localhost:6379/2?global_keyprefix=myapp.");
        assert_eq!(broker.url, "redis://localhost:6379/2");
        assert_eq!(broker.prefix, "myapp.");
        assert_eq!(parse_redis_db(&broker.url), 2);
    }

    #[test]
    fn redis_broker_url_keeps_other_params() {
        // Other query params survive in their original order
        let broker =
            RedisBrokerUrl::parse("redis://localhost:6379/0?timeout=5&global_keyprefix=myapp:&a=b");
        assert_eq!(broker.url, "redis://localhost:6379/0?timeout=5&a=b");
        assert_eq!(broker.prefix, "myapp:");
    }

    #[test]
    fn redis_broker_url_empty_prefix_behaves_like_none() {
        // An empty value is stripped and leaves keys unprefixed
        let broker = RedisBrokerUrl::parse("redis://localhost:6379/0?global_keyprefix=");
        assert_eq!(broker.url, "redis://localhost:6379/0");
        assert_eq!(broker.key("emails"), "emails");
    }

    #[test]
    fn redis_broker_url_prefixes_keys_and_channels() {
        // Kombu prepends the prefix to list keys, binding keys and pub/sub channels alike
        let broker = RedisBrokerUrl::parse("redis://localhost:6379/0?global_keyprefix=myapp.");
        assert_eq!(broker.key("emails"), "myapp.emails");
        assert_eq!(broker.key("_kombu.binding.celeryev"), "myapp._kombu.binding.celeryev");
        assert_eq!(broker.key(&redis_pidbox_channel(0)), "myapp./0.celery.pidbox");
    }

    #[test]
    fn binding_member_queue_reads_queue_field() {
        // Members are routing_key, pattern and queue; the queue need not match the routing key
        assert_eq!(binding_member_queue("email.welcome\x06\x16\x06\x16emails"), Some("emails"));
        assert_eq!(
            binding_member_queue("worker.#\x06\x16\x06\x16celeryev.1234"),
            Some("celeryev.1234")
        );
    }

    #[test]
    fn binding_member_queue_rejects_malformed_members() {
        // A bare name (no separators) or an empty queue field yields no queue
        assert_eq!(binding_member_queue("emails"), None);
        assert_eq!(binding_member_queue("default\x06\x16\x06\x16"), None);
    }

    #[test]
    fn redis_pidbox_channel_matches_kombu_fanout_topic() {
        assert_eq!(redis_pidbox_channel(0), "/0.celery.pidbox");
        assert_eq!(redis_pidbox_channel(3), "/3.celery.pidbox");
    }

    #[test]
    fn redis_control_envelope_round_trips() {
        let control_msg = serde_json::json!({
            "method": "revoke",
            "arguments": {"task_id": "abc", "terminate": false, "signal": "SIGQUIT"},
            "destination": null,
            "pattern": null,
            "matcher": null,
        });
        let envelope = redis_control_envelope(&control_msg, "tag-1").unwrap();

        assert_eq!(envelope["properties"]["delivery_tag"], "tag-1");
        assert_eq!(envelope["properties"]["delivery_info"]["exchange"], "celery.pidbox");
        let raw = serde_json::to_string(&envelope).unwrap();
        assert_eq!(crate::broker_conn::kombu::decode_kombu_body(&raw).unwrap(), control_msg);
    }
}
