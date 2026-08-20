# android-libp2p

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![API](https://img.shields.io/badge/API-24%2B-brightgreen.svg)](https://android-arsenal.com/api?level=24)
[![Platform](https://img.shields.io/badge/Platform-Android-green.svg)](https://developer.android.com)

A high-performance, production-ready **peer-to-peer networking library** for Android applications.  
Built on [go-libp2p](https://github.com/libp2p/go-libp2p) and compiled for Android via [Gomobile](https://pkg.go.dev/golang.org/x/mobile/cmd/gomobile).

**Zero servers. Zero accounts. Just peers.**

---

## ✨ Features

| Feature | Description |
|---|---|
| 🔗 **Direct Messaging** | Unicast text messages over QUIC/TCP streams between peers |
| 📡 **GossipSub Pub/Sub** | Scalable multicast group channels via epidemic gossip protocol |
| 🔍 **DHT Discovery** | Find peers globally via Kademlia DHT and IPFS bootstrap nodes |
| 🌐 **NAT Traversal** | ICE/STUN hole-punching with automatic Circuit Relay v2 fallback |
| 📦 **File Streaming** | Automatic 200 KB chunked media transfer with progress callbacks |
| 📊 **Bandwidth Telemetry** | Real-time upload/download rate monitoring via Kotlin StateFlows |
| 🔒 **Ed25519 Identity** | Persistent cryptographic peer identity with automatic key management |
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

**Option B: Copy into Existing Project**

1. Copy `libp2p/src/main/java/io/libp2p/android/Libp2pAndroid.kt` into your source tree
2. Copy `libp2p/libs/golib.aar` into your `app/libs/` directory  
3. Add dependencies:
```kotlin
dependencies {
    implementation(files("libs/golib.aar"))
    implementation("com.google.code.gson:gson:2.11.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.9.0")
}
```

### 2. Start a P2P Node

```kotlin
val client = Libp2pClient.getInstance(context)

lifecycleScope.launch {
    val peerId = client.start().getOrThrow()
    Log.i("P2P", "My Peer ID: $peerId")
}
```

### 3. Send a Direct Message

```kotlin
client.sendDirectMessage(
    targetPeerId = "12D3KooW...",
    text = "Hello from Android!",
    senderNickname = "Alice"
)
```

### 4. Subscribe to a Group Channel

```kotlin
client.subscribeTopic("my-room")
    .onEach { msg ->
        Log.i("P2P", "${msg.senderNickname}: ${msg.content}")
    }
    .launchIn(lifecycleScope)
```

### 5. Stream a File

```kotlin
val photoBytes = File("/path/to/photo.jpg").readBytes()

client.sendDirectMedia(
    targetPeerId = "12D3KooW...",
    fileName = "photo.jpg",
    contentType = "image/jpeg",
    fileBytes = photoBytes,
    onProgress = { sent, total ->
        Log.i("P2P", "Progress: ${sent * 100 / total}%")
    }
)
```

---

## 📖 API Reference

### `Libp2pClient`

| Method | Description |
|---|---|
| `getInstance(context, config)` | Get singleton client instance |
| `start(privateKey?)` | Start the P2P node, returns `Result<PeerId>` |
| `stop()` | Stop the node and release sockets |
| `sendDirectMessage(peerId, text, nickname?)` | Send a text DM |
| `sendDirectMedia(peerId, fileName, contentType, bytes, onProgress?)` | Stream a file with chunking |
| `subscribeTopic(name)` | Subscribe to a GossipSub topic, returns `Flow<Message>` |
| `unsubscribeTopic(name)` | Leave a topic |
| `publishTopic(name, text, nickname?)` | Broadcast text to topic |
| `publishTopicMedia(name, fileName, contentType, bytes)` | Broadcast media to topic |
| `connectPeer(multiaddr)` | Dial a peer directly |
| `disconnectPeer(peerId)` | Disconnect a peer |
| `addBootstrapPeer(multiaddr)` | Add a custom bootstrap node |

### Observable Flows

| Flow | Type | Description |
|---|---|---|
| `nodeStatus` | `StateFlow<Libp2pNodeStatus>` | `STOPPED`, `STARTING`, `RUNNING`, `ERROR` |
| `peerId` | `StateFlow<String?>` | Local peer ID |
| `connectedPeers` | `StateFlow<List<String>>` | Connected peer IDs |
| `listenAddresses` | `StateFlow<List<String>>` | Local multiaddresses |
| `incomingMessages` | `SharedFlow<Libp2pMessage>` | All incoming DMs and media |
| `peerEvents` | `SharedFlow<Libp2pPeerEvent>` | Peer connect/disconnect events |
| `bandwidthStats` | `StateFlow<Libp2pBandwidthStats>` | Real-time bandwidth metrics |

---

## ⚙️ Configuration

```kotlin
val config = Libp2pConfig(
    listenPort = 0,                   // 0 = auto-assign
    enableRelay = true,               // Circuit Relay v2
    enableDht = true,                 // Kademlia DHT
    mediaChunkSizeBytes = 200 * 1024, // 200 KB chunks
    bandwidthLimitBytesPerSec = 0L,   // 0 = unlimited
    bootstrapPeers = listOf(          // IPFS bootstrap nodes
        "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
        // ...
    ),
    iceServers = listOf(              // STUN/TURN servers
        "stun:stun.l.google.com:19302"
    )
)

val client = Libp2pClient.getInstance(context, config)
```

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────┐
│              Your Android App               │
│                                             │
│  Libp2pClient (Kotlin API)                  │
│  ├── Direct Messaging (sendDirectMessage)   │
│  ├── GossipSub Pub/Sub (subscribeTopic)     │
│  ├── File Streaming (sendDirectMedia)       │
│  └── Reactive Flows (StateFlow/SharedFlow)  │
├─────────────────────────────────────────────┤
│  Libp2pChunkAssembler (200KB chunking)      │
├─────────────────────────────────────────────┤
│  golib.aar (Go Mobile Bridge)               │
│  ├── go-libp2p (QUIC + TCP transports)      │
│  ├── Kademlia DHT (peer discovery)          │
│  ├── GossipSub (epidemic multicast)         │
│  ├── Circuit Relay v2 (NAT fallback)        │
│  └── ICE/STUN/TURN (hole punching)          │
└─────────────────────────────────────────────┘
```

---

## 🧪 Testing

Run unit tests (no emulator required):

```bash
./gradlew :libp2p:test
```

Tests cover:
- ✅ Configuration defaults and overrides
- ✅ Chunk assembler (in-order, out-of-order, duplicate, multi-transfer)
- ✅ Wire message JSON serialization/deserialization
- ✅ Data model integrity (enums, flags, defaults)

---

## 🛡️ ProGuard / R8

Rules are automatically included via `consumer-rules.pro`. If you need manual rules:

```proguard
-keep class golib.** { *; }
-keep interface golib.** { *; }
-keep class io.libp2p.android.** { *; }
-keepclassmembers class io.libp2p.android.** { *; }
```

---

## 📄 License

```
Copyright 2025 khushi28503

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
```

See [LICENSE](LICENSE) for the full text.

---

## 🤝 Contributing

Contributions are welcome! Please open an issue or pull request.

1. Fork the repo
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request
