# android-libp2p

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![API](https://img.shields.io/badge/API-24%2B-brightgreen.svg)](https://android-arsenal.com/api?level=24)
[![Platform](https://img.shields.io/badge/Platform-Android-green.svg)](https://developer.android.com)

A high-performance, production-ready **peer-to-peer networking library and engine** for Android applications.  
Built on [go-libp2p](https://github.com/libp2p/go-libp2p) and compiled for Android via [Gomobile](https://pkg.go.dev/golang.org/x/mobile/cmd/gomobile).

**Zero servers. Zero accounts. 100% Peer-to-Peer.**

---

## ✨ Core & Advanced Features

| Feature | Description |
|---|---|
| 🔗 **Direct Messaging** | Unicast text messages over QUIC/TCP streams between peers |
| 🔒 **End-to-End Encryption (E2EE)** | Built-in AES-256-GCM encryption helpers (`Libp2pCrypto`) |
| 📩 **Delivery & Read Receipts** | WhatsApp-style ACK protocol (`DELIVERED` & `READ` status flows) |
| ✍️ **Typing Indicators** | Transient real-time typing events for 1-on-1 and topic chats |
| ⏱️ **Latency Ping / RTT** | Live peer connection pinging with Round-Trip Time measurement in ms |
| 📡 **GossipSub Pub/Sub** | Scalable multicast group channels via epidemic gossip protocol |
| 🔍 **DHT Discovery** | Find peers globally via Kademlia DHT and IPFS bootstrap nodes |
| 🌐 **NAT Traversal** | ICE/STUN hole-punching with automatic Circuit Relay v2 fallback |
| 🔄 **Smart Auto-Reconnect** | Automatic re-dialing with exponential backoff on network transitions |
| 📦 **File Streaming & Cancel** | 200 KB chunked media transfer with live progress and cancel support |
| 📊 **Bandwidth Telemetry** | Real-time upload/download rate monitoring via Kotlin StateFlows |
| 📱 **Android Native** | minSdk 24, Kotlin coroutines, zero native build steps required |

---

## 🚀 Quick Start

### 1. Add the Library

**Option A: Gradle Module (Recommended)**

```kotlin
// settings.gradle.kts
include(":libp2p")

// app/build.gradle.kts
dependencies {
    implementation(project(":libp2p"))
}
```

**Option B: Standalone Pure Kotlin File**

Copy `P2PSystem.kt` directly into your Android project source tree for a zero-wrapper, raw native P2P engine.

---

## 💡 Code Examples

### 1. Start a P2P Node

```kotlin
val client = Libp2pClient.getInstance(context)

lifecycleScope.launch {
    val peerId = client.start().getOrThrow()
    Log.i("P2P", "My Peer ID: $peerId")
}
```

### 2. End-to-End Encrypted Direct Messaging (E2EE)

```kotlin
// Generate a 256-bit AES shared key or exchange via ECDH
val sharedKey = Libp2pCrypto.generateKey()

lifecycleScope.launch {
    client.sendEncryptedDirectMessage(
        targetPeerId = "12D3KooW...",
        text = "Confidential P2P message",
        aesKeyBase64 = sharedKey
    )
}
```

### 3. Delivery Receipts (Single/Double Checkmarks)

```kotlin
// Observe message delivery receipts
client.deliveryReceipts.onEach { receipt ->
    when (receipt.status) {
        Libp2pReceiptStatus.DELIVERED -> Log.i("P2P", "Message ${receipt.originalMessageId} delivered! (✓✓)")
        Libp2pReceiptStatus.READ -> Log.i("P2P", "Message ${receipt.originalMessageId} read! (✓✓ blue)")
    }
}.launchIn(lifecycleScope)

// Send read receipt when user opens chat
lifecycleScope.launch {
    client.sendReadReceipt(targetPeerId = "12D3KooW...", originalMessageId = "msg-123")
}
```

### 4. Real-Time Typing Indicators

```kotlin
// Notify peer that user is typing
lifecycleScope.launch {
    client.sendTyping(targetPeerId = "12D3KooW...", isTyping = true)
}

// Observe typing state in UI
client.typingEvents.onEach { event ->
    if (event.isTyping) {
        Log.i("P2P", "${event.peerId} is typing...")
    }
}.launchIn(lifecycleScope)
```

### 5. Peer Latency Ping (RTT)

```kotlin
lifecycleScope.launch {
    val rttMs = client.pingPeer("12D3KooW...").getOrNull()
    Log.i("P2P", "Ping latency: ${rttMs}ms")
}

// Observe live presence & latency of connected peers
client.peerPresence.onEach { presenceMap ->
    presenceMap.forEach { (peerId, presence) ->
        Log.i("P2P", "Peer $peerId: Online=${presence.isOnline}, RTT=${presence.rttLatencyMs}ms")
    }
}.launchIn(lifecycleScope)
```

### 6. Stream Files with Cancellation

```kotlin
val photoBytes = File("/path/to/photo.jpg").readBytes()

lifecycleScope.launch {
    val transferId = client.sendDirectMedia(
        targetPeerId = "12D3KooW...",
        fileName = "photo.jpg",
        contentType = "image/jpeg",
        fileBytes = photoBytes,
        onProgress = { sent, total ->
            Log.i("P2P", "Progress: ${sent * 100 / total}%")
        }
    ).getOrThrow()

    // Cancel anytime if needed:
    // client.cancelMediaTransfer(transferId)
}
```

---

## 📖 API Reference

### `Libp2pClient` Methods

| Method | Description |
|---|---|
| `getInstance(context, config)` | Get singleton client instance |
| `start(privateKey?)` | Start P2P node, returns `Result<PeerId>` |
| `stop()` | Stop node and release sockets |
| `sendDirectMessage(peerId, text, nickname?)` | Send plaintext DM |
| `sendEncryptedDirectMessage(peerId, text, aesKey, nickname?)` | Send AES-256-GCM encrypted DM |
| `sendDirectMedia(peerId, name, type, bytes, nickname?, onProgress?)` | Stream file in 200 KB chunks |
| `cancelMediaTransfer(chunkId)` | Abort active media transfer |
| `sendTyping(peerId, isTyping, topicName?)` | Send typing status |
| `sendReadReceipt(peerId, messageId)` | Send message read ACK |
| `pingPeer(peerId)` | Measure round-trip latency in ms |
| `subscribeTopic(name)` | Subscribe to GossipSub topic, returns `Flow<Message>` |
| `publishTopic(name, text, nickname?)` | Broadcast text to topic |
| `connectPeer(multiaddr)` | Dial a peer directly |
| `disconnectPeer(peerId)` | Disconnect a peer |
| `addBootstrapPeer(multiaddr)` | Add custom bootstrap node |

### Observable Flows

| Flow | Type | Description |
|---|---|---|
| `nodeStatus` | `StateFlow<Libp2pNodeStatus>` | `STOPPED`, `STARTING`, `RUNNING`, `ERROR` |
| `peerId` | `StateFlow<String?>` | Local peer ID |
| `connectedPeers` | `StateFlow<List<String>>` | Actively connected peer IDs |
| `listenAddresses` | `StateFlow<List<String>>` | Local listening multiaddresses |
| `incomingMessages` | `SharedFlow<Libp2pMessage>` | Stream of incoming DMs and media |
| `deliveryReceipts` | `SharedFlow<Libp2pDeliveryReceipt>` | Live delivery and read ACKs |
| `typingEvents` | `SharedFlow<Libp2pTypingEvent>` | Real-time typing indicators |
| `peerPresence` | `StateFlow<Map<String, Libp2pPeerPresence>>` | Peer online status and RTT latency |
| `bandwidthStats` | `StateFlow<Libp2pBandwidthStats>` | Real-time transfer rates & telemetry |

---

## ⚙️ Configuration Options

```kotlin
val config = Libp2pConfig(
    listenPort = 0,                   // 0 = auto-assign
    enableRelay = true,               // Circuit Relay v2 fallback
    enableDht = true,                 // Kademlia DHT discovery
    mediaChunkSizeBytes = 200 * 1024, // 200 KB chunks
    autoReconnect = true,             // Auto-reconnect on network change
    bandwidthLimitBytesPerSec = 0L,   // 0 = unlimited
    bootstrapPeers = listOf(
        "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN"
    ),
    iceServers = listOf(
        "stun:stun.l.google.com:19302"
    )
)
```

---

## 🧪 Unit Testing

```bash
./gradlew :libp2p:test
```

Tests cover:
- ✅ End-to-End Encryption (AES-256-GCM keygen, encrypt, decrypt, wrong-key failure)
- ✅ Media Chunking & Transfer Cancellation
- ✅ Delivery Receipt (ACK) Wire Serialization & Enums
- ✅ Typing Indicator & Latency Ping/Pong Protocols
- ✅ Configuration Defaults & Dynamic Overrides
- ✅ Data Model Integrity

---

## 📄 License

```
Copyright 2025 khushi28503

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
```
