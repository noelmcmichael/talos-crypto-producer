# Crypto WebSocket Producer for RustMQ

Real-time cryptocurrency trade data producer, streaming from Coinbase WebSocket to RustMQ message queue.

## Overview

This Rust application connects to Coinbase WebSocket API and streams live trade data for 12 cryptocurrency pairs to RustMQ using a custom TCP+bincode protocol.

## Tracked Symbols

- BTC-USD, ETH-USD, SOL-USD, ADA-USD
- DOGE-USD, MATIC-USD, AVAX-USD, DOT-USD
- LINK-USD, UNI-USD, ATOM-USD, LTC-USD

## Architecture

```
Coinbase WebSocket → Crypto Producer → RustMQ (TCP 9092)
```

## Protocol

Uses RustMQ's custom protocol:
- Transport: TCP
- Serialization: bincode
- Message: key (symbol) + value (JSON event)

NOT compatible with Kafka clients.

## Deployment

### Current Configuration
- **Replicas:** 1
- **Resources:**
  - CPU: 50m request, 200m limit
  - Memory: 64Mi request, 256Mi limit
- **Namespace:** data-pipeline

### RustMQ Connection
- **Host:** rustmq-0.rustmq-headless.data-pipeline.svc.cluster.local
- **Port:** 9092 (TCP)

## Message Format

### Key
```
Symbol (e.g., "BTC-USD")
```

### Value
```json
{
  "event_type": "crypto_trade",
  "source": "coinbase_websocket_rust",
  "data": {
    "symbol": "BTC-USD",
    "price": 45123.45,
    "size": 0.001,
    "timestamp": "2026-01-02T17:30:45.123Z"
  },
  "metadata": {
    "producer_timestamp": 1704216645.123,
    "event_time": 1704216645.123,
    "source_exchange": "coinbase"
  }
}
```

## Development

### Build Locally
```bash
cargo build --release
```

### Run Locally (requires RustMQ connection)
```bash
RUST_LOG=crypto_websocket_producer=info cargo run
```

### Docker Build
```bash
docker build -t crypto-producer:local .
```

## Dependencies

- **tokio:** Async runtime
- **tokio-tungstenite:** WebSocket client
- **bincode:** RustMQ message serialization
- **serde/serde_json:** Data serialization
- **tracing:** Logging

## Monitoring

Producer logs statistics every 10 seconds:
- Messages sent
- Send rate (msg/s)
- Error count

## Refactoring from Kafka

**Original:** Used rdkafka library  
**Updated:** Direct TCP connection with bincode serialization  

**Changes Made:**
1. Replaced rdkafka with bincode + tokio TCP
2. Implemented `send_to_rustmq()` helper function
3. Updated connection to RustMQ DNS name
4. Message format: bincode-serialized key-value pairs

## Status

- ✅ Refactored for RustMQ protocol
- ✅ Updated for AMD64 (AWS EC2)
- ✅ Kubernetes manifests ready
- ⏳ Awaiting deployment

## Links

- **RustMQ:** https://github.com/noelmcmichael/talos-rustmq
- **Harbor:** harbor.int-talos-poc.pocketcove.net/library/crypto-producer
- **Namespace:** data-pipeline
- **Coinbase API:** https://docs.cloud.coinbase.com/exchange/docs/websocket-overview

---

**Status:** Ready to deploy  
**Platform:** Talos Kubernetes v1.31.2  
**Last Updated:** January 2, 2026
