use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};
use tracing::{error, info, warn};

// Configuration constants
const RUSTMQ_HOST: &str = "rustmq-0.rustmq-headless.data-pipeline.svc.cluster.local";
const RUSTMQ_PORT: u16 = 9092;
const COINBASE_WS_URL: &str = "wss://ws-feed.exchange.coinbase.com";

// Default symbols to track (can be overridden by SYMBOLS env var)
const DEFAULT_SYMBOLS: &[&str] = &[
    "BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD", 
    "DOGE-USD", "MATIC-USD", "AVAX-USD", "DOT-USD",
    "LINK-USD", "UNI-USD", "ATOM-USD", "LTC-USD",
];

// Get symbols from env var or use defaults
fn get_symbols() -> Vec<String> {
    match std::env::var("SYMBOLS") {
        Ok(symbols_str) => {
            symbols_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        }
        Err(_) => DEFAULT_SYMBOLS.iter().map(|s| s.to_string()).collect(),
    }
}

// RustMQ Message structure (must match RustMQ's message.rs)
#[derive(Debug, Serialize, Deserialize)]
struct RustMQMessage {
    key: Vec<u8>,
    value: Vec<u8>,
    timestamp: u64,
    #[serde(default)]
    headers: Vec<(String, String)>,
}

impl RustMQMessage {
    fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        
        Self {
            key,
            value,
            timestamp,
            headers: Vec::new(),
        }
    }
}

// Coinbase WebSocket messages
#[derive(Debug, Serialize)]
struct SubscribeMessage {
    r#type: String,
    product_ids: Vec<String>,
    channels: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CoinbaseMatch {
    r#type: String,
    #[serde(default)]
    product_id: String,
    #[serde(default)]
    price: String,
    #[serde(default)]
    size: String,
    #[serde(default)]
    time: String,
}

// Output event structure
#[derive(Debug, Serialize)]
struct CryptoTradeEvent {
    event_type: String,
    source: String,
    data: CryptoTradeData,
    metadata: EventMetadata,
}

#[derive(Debug, Serialize)]
struct CryptoTradeData {
    symbol: String,
    price: f64,
    size: f64,
    timestamp: String,
}

#[derive(Debug, Serialize)]
struct EventMetadata {
    producer_timestamp: f64,
    event_time: f64,
    source_exchange: String,
}

// RustMQ producer helper function
async fn send_to_rustmq(
    stream: &mut TcpStream,
    key: &[u8],
    value: &[u8],
) -> Result<u64, Box<dyn std::error::Error>> {
    // Create RustMQ message
    let message = RustMQMessage::new(key.to_vec(), value.to_vec());

    // Serialize message using bincode
    let msg_bytes = bincode::serialize(&message)?;
    let msg_len = msg_bytes.len() as u32;

    // Send length (u32, big-endian)
    stream.write_u32(msg_len).await?;

    // Send message bytes
    stream.write_all(&msg_bytes).await?;

    // Read offset response (u64, big-endian)
    let offset = stream.read_u64().await?;

    if offset == u64::MAX {
        return Err("Server returned error (u64::MAX)".into());
    }

    Ok(offset)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("crypto_websocket_producer=info")
        .init();

    // Get symbols from environment or use defaults
    let symbols = get_symbols();
    
    info!("🚀 Crypto WebSocket Producer (Rust) starting...");
    info!("📡 Connecting to: {}", COINBASE_WS_URL);
    info!("📤 RustMQ host: {}:{}", RUSTMQ_HOST, RUSTMQ_PORT);
    info!("💱 Tracking {} symbols: {:?}", symbols.len(), symbols);

    // Connect to RustMQ
    let rustmq_addr = format!("{}:{}", RUSTMQ_HOST, RUSTMQ_PORT);
    let mut rustmq_stream = TcpStream::connect(&rustmq_addr)
        .await
        .expect("Failed to connect to RustMQ");

    info!("✅ Connected to RustMQ at {}", rustmq_addr);

    // Connect to Coinbase WebSocket
    let (ws_stream, _) = connect_async(COINBASE_WS_URL)
        .await
        .expect("Failed to connect to WebSocket");

    info!("✅ Connected to Coinbase WebSocket");

    let (mut write, mut read) = ws_stream.split();

    // Subscribe to matches AND ticker channels for all symbols
    let subscribe_msg = SubscribeMessage {
        r#type: "subscribe".to_string(),
        product_ids: symbols.clone(),
        channels: vec!["matches".to_string(), "ticker".to_string()],
    };

    let subscribe_json = serde_json::to_string(&subscribe_msg)?;
    write.send(WsMessage::Text(subscribe_json)).await?;

    info!("✅ Subscribed to {} symbols", symbols.len());
    info!("⏳ Waiting for trade messages...\n");

    // Statistics
    let mut messages_sent = 0u64;
    let mut errors = 0u64;
    let mut last_status = std::time::Instant::now();

    // Process incoming messages
    while let Some(message) = read.next().await {
        match message {
            Ok(WsMessage::Text(text)) => {
                // Parse Coinbase message
                match serde_json::from_str::<CoinbaseMatch>(&text) {
                    Ok(coinbase_msg) => {
                        // Only process "match" messages (actual trades)
                        if coinbase_msg.r#type == "match" && !coinbase_msg.product_id.is_empty() {
                            // Parse price
                            if let Ok(price) = coinbase_msg.price.parse::<f64>() {
                                if let Ok(size) = coinbase_msg.size.parse::<f64>() {
                                    let now = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_secs_f64();

                                    // Create event
                                    let event = CryptoTradeEvent {
                                        event_type: "crypto_trade".to_string(),
                                        source: "coinbase_websocket_rust".to_string(),
                                        data: CryptoTradeData {
                                            symbol: coinbase_msg.product_id.clone(),
                                            price,
                                            size,
                                            timestamp: coinbase_msg.time,
                                        },
                                        metadata: EventMetadata {
                                            producer_timestamp: now,
                                            event_time: now,
                                            source_exchange: "coinbase".to_string(),
                                        },
                                    };

                                    // Serialize and send to RustMQ
                                    match serde_json::to_string(&event) {
                                        Ok(event_json) => {
                                            let key = coinbase_msg.product_id.as_bytes();
                                            let value = event_json.as_bytes();

                                            match send_to_rustmq(&mut rustmq_stream, key, value).await {
                                                Ok(_) => {
                                                    messages_sent += 1;
                                                }
                                                Err(e) => {
                                                    errors += 1;
                                                    error!("Failed to send to RustMQ: {:?}", e);
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            errors += 1;
                                            warn!("Failed to serialize event: {}", e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => {
                        // Ignore parse errors (subscription confirmations, heartbeats, etc.)
                    }
                }
            }
            Ok(WsMessage::Ping(data)) => {
                write.send(WsMessage::Pong(data)).await?;
            }
            Ok(WsMessage::Close(_)) => {
                warn!("WebSocket closed by server");
                break;
            }
            Err(e) => {
                error!("WebSocket error: {:?}", e);
                break;
            }
            _ => {}
        }

        // Status update every 10 seconds
        if last_status.elapsed().as_secs() >= 10 {
            let rate = messages_sent as f64 / last_status.elapsed().as_secs_f64();
            info!(
                "📊 Sent: {} msgs | Rate: {:.1} msg/s | Errors: {}",
                messages_sent, rate, errors
            );
            last_status = std::time::Instant::now();
        }
    }

    info!("🛑 Producer shutting down");
    info!("📊 Final stats - Sent: {} | Errors: {}", messages_sent, errors);

    Ok(())
}
