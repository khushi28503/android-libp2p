# android-libp2p

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![API](https://img.shields.io/badge/API-24%2B-brightgreen.svg)](https://android-arsenal.com/api?level=24)
[![Platform](https://img.shields.io/badge/Platform-Android-green.svg)](https://developer.android.com)

A high-performance, enterprise-ready **peer-to-peer networking engine and Android library**.  
Built on [go-libp2p](https://github.com/libp2p/go-libp2p) and compiled for Android via [Gomobile](https://pkg.go.dev/golang.org/x/mobile/cmd/gomobile).

**Zero servers. Zero centralized databases. 100% Direct P2P.**

---

## ✨ Features

| Category | Feature | Description |
|---|---|---|
| 💬 **Messaging** | Direct Messaging | Unicast text & binary streams over QUIC/TCP |
| 📬 **Reliability** | Persistent Outbox | Store-and-forward offline message queue with auto-flush on connect |
| 👥 **Multicast** | Encrypted Group Rooms | GossipSub topic multicast with symmetric AES-256-GCM room keys |
| 🎙️ **Rich Media** | Voice Note Streaming | Stream Opus/AAC voice notes with duration and waveform peaks |
| 🛡️ **Security** | End-to-End Encryption | Built-in AES-256-GCM cipher suite (`Libp2pCrypto`) |
| 📩 **Receipts** | Delivery & Read ACKs | WhatsApp-style message status tracking (`DELIVERED` & `READ`) |
| ✍️ **Presence** | Typing Indicators | Real-time typing status for 1-on-1 and topic chats |
| 📶 **Diagnostics** | Connection Health Score | Live signal health (0–100) & RTT latency in ms (`EXCELLENT` → `POOR`) |
| 🌐 **Discovery** | Kademlia DHT | Global peer lookup & IPFS bootstrap integration |
| 📦 **Transfers** | SHA-256 Verified Streaming | 200 KB chunked streaming with file integrity verification & cancellation |
| ⚡ **Performance** | LRU Deduplication | Eliminates echo loops and duplicate packet triggers |
| 🔄 **Resilience** | Exponential Backoff | Smart reconnect dialer with randomized jitter |
| 🔋 **Efficiency** | Battery-Adaptive Polling | Adaptive 15s/45s polling to save mobile battery |

---

## 🚀 Quick Start

### 1. Add Library

```kotlin
// settings.gradle.kts
include(":libp2p")

// app/build.gradle.kts
dependencies {
    implementation(project(":libp2p"))
}
```

Or copy `P2PSystem.kt` directly into your source tree for a zero-wrapper standalone engine.

---

## 💡 Code Examples

### 1. Persistent Offline Message Delivery (Outbox)

```kotlin
val client = Libp2pClient.getInstance(context)

lifecycleScope.launch {
    // Guarantees delivery: sends immediately if online, or queues locally and delivers the moment the peer reconnects!
    client.sendDirectMessageQueued(
        targetPeerId = "12D3KooW...",
        text = "Hello! Delivered whenever you come back online."
    )
}

// Observe pending outbox count
client.pendingOutboxCount.onEach { count ->
    Log.i("P2P", "Outbox pending: $count messages")
}.launchIn(lifecycleScope)
```

### 2. Encrypted GossipSub Group Topic Rooms

```kotlin
val roomKey = Libp2pCrypto.generateKey()

// 1. Subscribe and automatically decrypt messages
client.subscribeEncryptedTopic(topicName = "secret-squad", groupKeyBase64 = roomKey)
    .onEach { message ->
        Log.i("P2P", "Decrypted group chat: ${message.content}")
    }
    .launchIn(lifecycleScope)

// 2. Publish encrypted message to group
lifecycleScope.launch {
    client.publishEncryptedTopic(
        topicName = "secret-squad",
        text = "Confidential squad strategy update",
        groupKeyBase64 = roomKey
    )
}
```

### 3. Voice Note Streaming with Waveforms

```kotlin
val voiceBytes = File("/path/to/voice_note.opus").readBytes()
val waveformPeaks = listOf(15, 45, 80, 95, 70, 30, 10) // 0-100 amplitude bars

lifecycleScope.launch {
    client.sendVoiceNote(
        targetPeerId = "12D3KooW...",
        audioBytes = voiceBytes,
        durationMs = 4500L,
        waveformAmplitudes = waveformPeaks
    )
}

// Receiving voice notes
client.incomingMessages.onEach { msg ->
    if (msg.isVoiceNote) {
        val meta = msg.voiceNoteMeta!!
        Log.i("P2P", "Voice note received: ${meta.durationMs}ms with ${meta.waveformAmplitudes.size} waveform bars")
    }
}.launchIn(lifecycleScope)
```

### 4. Multi-Peer Parallel Broadcast

```kotlin
val peerIds = listOf("12D3KooWA...", "12D3KooWB...", "12D3KooWC...")

lifecycleScope.launch {
    val results = client.broadcastToPeers(peerIds, "Emergency Announcement!")
    results.forEach { (peerId, result) ->
        Log.i("P2P", "Peer $peerId delivery: ${result.isSuccess}")
    }
}
```

### 5. Connection Health & Latency Quality (0–100 Score)

```kotlin
client.peerPresence.onEach { presenceMap ->
    presenceMap.forEach { (peerId, presence) ->
        Log.i("P2P", "Peer $peerId: Quality=${presence.quality}, Health=${presence.healthScore}/100, RTT=${presence.rttLatencyMs}ms")
    }
}.launchIn(lifecycleScope)
```

---

## 📖 API Reference

### `Libp2pClient` Public Methods

| Method | Description |
|---|---|
| `getInstance(context, config)` | Get singleton client instance |
| `start(privateKey?)` | Start P2P node, returns `Result<PeerId>` |
| `stop()` | Stop node and release sockets |
| `sendDirectMessage(peerId, text, nickname?)` | Send plaintext DM |
| `sendDirectMessageQueued(peerId, text, nickname?)` | Send message with offline outbox auto-flush |
| `sendEncryptedDirectMessage(peerId, text, key, nickname?)` | Send AES-256-GCM encrypted DM |
| `sendVoiceNote(peerId, bytes, duration, waveforms, nickname?)` | Stream voice note with waveform metadata |
| `broadcastToPeers(peerIds, text, nickname?)` | Parallel broadcast to list of peers |
| `sendDirectMedia(peerId, name, type, bytes, onProgress?)` | Stream file with SHA-256 verification |
| `cancelMediaTransfer(chunkId)` | Abort active media transfer |
| `sendTyping(peerId, isTyping, topicName?)` | Send typing status |
| `sendReadReceipt(peerId, messageId)` | Send message read ACK |
| `pingPeer(peerId)` | Measure round-trip latency in ms |
| `subscribeTopic(name)` | Subscribe to GossipSub topic |
| `subscribeEncryptedTopic(name, roomKey)` | Subscribe & auto-decrypt encrypted topic room |
| `publishTopic(name, text, nickname?)` | Broadcast text to topic |
| `publishEncryptedTopic(name, text, roomKey, nickname?)` | Broadcast ciphertext to topic |
| `connectPeer(multiaddr)` | Dial peer directly |
| `disconnectPeer(peerId)` | Disconnect peer |
| `addBootstrapPeer(multiaddr)` | Add custom bootstrap node |

### Observable Flows

| Flow | Type | Description |
|---|---|---|
| `nodeStatus` | `StateFlow<Libp2pNodeStatus>` | `STOPPED`, `STARTING`, `RUNNING`, `ERROR` |
| `peerId` | `StateFlow<String?>` | Local cryptographic peer ID |
| `connectedPeers` | `StateFlow<List<String>>` | Connected peer IDs |
| `incomingMessages` | `SharedFlow<Libp2pMessage>` | Incoming DMs, media, and voice notes |
| `deliveryReceipts` | `SharedFlow<Libp2pDeliveryReceipt>` | Live delivery and read ACKs |
| `typingEvents` | `SharedFlow<Libp2pTypingEvent>` | Real-time typing indicators |
| `peerPresence` | `StateFlow<Map<String, Libp2pPeerPresence>>` | Connection health score (0-100) & latency |
| `pendingOutboxCount` | `StateFlow<Int>` | Count of queued offline messages |
| `bandwidthStats` | `StateFlow<Libp2pBandwidthStats>` | Real-time transfer rates & telemetry |

---

## 🧪 Unit Testing

```bash
./gradlew :libp2p:test
```

Tests cover:
- ✅ Persistent Outbox Queue & Auto-Flushing
- ✅ Voice Note Waveform Preservation & Chunk Assembly
- ✅ Encrypted GossipSub Group Rooms (AES-256-GCM)
- ✅ Connection Health Quality Classification (0-100)
- ✅ SHA-256 File Integrity Checksums
- ✅ LRU Message Deduplication
- ✅ Exponential Backoff with Jitter
- ✅ Wire Message JSON Serialization

---

## 📄 License

```
Copyright 2025 khushi28503

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
```
