package io.libp2p.android

import android.content.Context
import android.net.ConnectivityManager
import android.net.Network
import android.util.Base64
import android.util.Log
import com.google.gson.Gson
import golib.Golib
import golib.MobileConnectionHandler
import golib.MobileMessageHandler
import golib.MobilePeerAddressHandler
import golib.MobileTopicMessageHandler
import golib.P2PNode
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.ceil

// ============================================================================
// PUBLIC DATA MODELS & ENUMS
// ============================================================================

/** Node lifecycle status */
enum class Libp2pNodeStatus {
    STOPPED,
    STARTING,
    RUNNING,
    ERROR
}

/** Peer connection event */
data class Libp2pPeerEvent(
    val peerId: String,
    val isConnected: Boolean,
    val multiaddress: String? = null,
    val timestamp: Long = System.currentTimeMillis()
)

/** Peer information snapshot */
data class Libp2pPeer(
    val peerId: String,
    val isConnected: Boolean = false,
    val multiaddresses: List<String> = emptyList(),
    val lastSeen: Long = System.currentTimeMillis()
)

/** Incoming message received via direct stream or topic */
data class Libp2pMessage(
    val senderPeerId: String,
    val content: String,
    val messageId: String = UUID.randomUUID().toString(),
    val senderNickname: String? = null,
    val timestamp: Long = System.currentTimeMillis(),
    val isTopicMessage: Boolean = false,
    val topicName: String? = null,
    // Media fields (null for standard text messages)
    val fileBytes: ByteArray? = null,
    val fileName: String? = null,
    val contentType: String? = null,
    val fileSize: Long? = null
) {
    val isMediaMessage: Boolean get() = fileBytes != null
}

/** Live bandwidth metrics */
data class Libp2pBandwidthStats(
    val sessionInBytes: Long = 0L,
    val sessionOutBytes: Long = 0L,
    val sessionTotalBytes: Long = 0L,
    val dailyInBytes: Long = 0L,
    val dailyOutBytes: Long = 0L,
    val dailyTotalBytes: Long = 0L,
    val currentRateInBytesPerSec: Double = 0.0,
    val currentRateOutBytesPerSec: Double = 0.0,
    val projectedHourlyTotalBytes: Long = 0L,
    val rawJson: String = "{}"
)

/** Configuration options for Libp2pClient */
data class Libp2pConfig(
    val listenPort: Int = 0, // 0 = automatic dynamic port
    val enableRelay: Boolean = true,
    val enableDht: Boolean = true,
    val mediaChunkSizeBytes: Int = 200 * 1024, // 200 KB per chunk
    val bootstrapPeers: List<String> = DEFAULT_BOOTSTRAP_PEERS,
    val iceServers: List<String> = DEFAULT_ICE_SERVERS,
    val bandwidthLimitBytesPerSec: Long = 0L // 0 = unlimited
) {
    companion object {
        val DEFAULT_BOOTSTRAP_PEERS = listOf(
            "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
            "/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
            "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoVegavRt5ns5W28EWn7nkVUW6B2xNxC4Q888pFn",
            "/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt"
        )
        val DEFAULT_ICE_SERVERS = listOf(
            "stun:stun.l.google.com:19302",
            "stun:stun1.l.google.com:19302",
            "stun:stun2.l.google.com:19302"
        )
    }
}

// ============================================================================
// MAIN CLIENT ENTRY POINT (Libp2pClient)
// ============================================================================

/**
 * High-performance, production-ready libp2p client for Android applications.
 *
 * Provides:
 * - Direct peer-to-peer messaging (text and binary files with automatic chunking)
 * - Topic-based multicast pub/sub (GossipSub)
 * - Kademlia DHT peer discovery & bootstrap management
 * - ICE/STUN/TURN NAT hole-punching and Circuit Relay v2
 * - Reactive Kotlin StateFlow and SharedFlow observability
 */
class Libp2pClient private constructor(
    private val context: Context,
    val config: Libp2pConfig
) {
    companion object {
        private const val TAG = "Libp2pClient"
        private const val PREFS_NAME = "libp2p_client_prefs"
        private const val KEY_PRIVATE_KEY = "libp2p_private_key"
        private const val KEY_PEER_ID = "libp2p_peer_id"
        private const val STATUS_REFRESH_INTERVAL_MS = 5_000L

        @Volatile
        private var defaultInstance: Libp2pClient? = null

        /** Get or initialize the default singleton instance */
        fun getInstance(context: Context, config: Libp2pConfig = Libp2pConfig()): Libp2pClient {
            return defaultInstance ?: synchronized(this) {
                defaultInstance ?: Libp2pClient(context.applicationContext, config).also {
                    defaultInstance = it
                }
            }
        }
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val gson: Gson by lazy { Gson() }
    private val nodeLock = Mutex()
    private var node: P2PNode? = null
    private var statusRefreshJob: Job? = null

    // Chunk reassembler for incoming multi-part files
    private val chunkAssembler = Libp2pChunkAssembler()

    // Shared preferences for key persistence
    private val prefs by lazy {
        context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
    }

    // ------------------------------------------------------------------------
    // Reactive Observables (Flows)
    // ------------------------------------------------------------------------

    private val _nodeStatus = MutableStateFlow(Libp2pNodeStatus.STOPPED)
    val nodeStatus: StateFlow<Libp2pNodeStatus> = _nodeStatus.asStateFlow()

    private val _peerId = MutableStateFlow<String?>(null)
    val peerId: StateFlow<String?> = _peerId.asStateFlow()

    private val _connectedPeers = MutableStateFlow<List<String>>(emptyList())
    val connectedPeers: StateFlow<List<String>> = _connectedPeers.asStateFlow()

    private val _listenAddresses = MutableStateFlow<List<String>>(emptyList())
    val listenAddresses: StateFlow<List<String>> = _listenAddresses.asStateFlow()

    private val _bandwidthStats = MutableStateFlow(Libp2pBandwidthStats())
    val bandwidthStats: StateFlow<Libp2pBandwidthStats> = _bandwidthStats.asStateFlow()

    private val _incomingMessages = MutableSharedFlow<Libp2pMessage>(extraBufferCapacity = 64)
    val incomingMessages: SharedFlow<Libp2pMessage> = _incomingMessages.asSharedFlow()

    private val _peerEvents = MutableSharedFlow<Libp2pPeerEvent>(extraBufferCapacity = 64)
    val peerEvents: SharedFlow<Libp2pPeerEvent> = _peerEvents.asSharedFlow()

    // Active topic subscriptions
    private val subscribedTopics = ConcurrentHashMap<String, MutableSharedFlow<Libp2pMessage>>()

    // Known peer multiaddresses
    private val peerAddressMap = ConcurrentHashMap<String, MutableSet<String>>()

    init {
        // Restore cached Peer ID if available
        prefs.getString(KEY_PEER_ID, null)?.let {
            _peerId.value = it
        }
        setupNetworkCallback()
    }

    // ========================================================================
    // LIFECYCLE: START / STOP
    // ========================================================================

    /**
     * Start the libp2p node.
     * @param privateKeyBase64 Optional existing Ed25519 private key (Base64). If null, generated/restored.
     * @return Result with the local Peer ID string on success.
     */
    suspend fun start(privateKeyBase64: String? = null): Result<String> = nodeLock.withLock {
        withContext(Dispatchers.IO) {
            if (_nodeStatus.value == Libp2pNodeStatus.RUNNING && node != null) {
                return@withContext Result.success(_peerId.value.orEmpty())
            }

            try {
                _nodeStatus.value = Libp2pNodeStatus.STARTING
                Log.i(TAG, "Starting Libp2pClient on port ${config.listenPort}...")

                val keyToUse = privateKeyBase64 ?: prefs.getString(KEY_PRIVATE_KEY, "") ?: ""
                val dataDir = File(context.filesDir, "libp2p_data").apply { mkdirs() }.absolutePath

                val createdNode = Golib.newP2PNode(
                    config.listenPort.toLong(),
                    config.enableRelay,
                    keyToUse,
                    dataDir
                ) ?: return@withContext Result.failure(IllegalStateException("Golib.newP2PNode returned null"))

                // Wire Go bridge callbacks
                setupBridgeHandlers(createdNode)

                // Configure ICE STUN/TURN servers
                val iceJson = JSONArray(config.iceServers).toString()
                try { createdNode.setIceServers(iceJson) } catch (e: Exception) { Log.w(TAG, "setIceServers failed", e) }

                // Configure bandwidth limit if set
                if (config.bandwidthLimitBytesPerSec > 0) {
                    try { createdNode.setBandwidthLimit(config.bandwidthLimitBytesPerSec) } catch (e: Exception) { Log.w(TAG, "setBandwidthLimit failed", e) }
                }

                // Add configured bootstrap peers
                for (bootstrapAddr in config.bootstrapPeers) {
                    try { createdNode.addBootstrapPeer(bootstrapAddr) } catch (e: Exception) { Log.w(TAG, "addBootstrapPeer failed for $bootstrapAddr", e) }
                }

                // Start node
                createdNode.start()

                val localPeerId = createdNode.peerID
                val exportedKey = try { createdNode.exportPrivateKey() } catch (e: Exception) { "" }

                if (exportedKey.isNotEmpty()) {
                    prefs.edit()
                        .putString(KEY_PRIVATE_KEY, exportedKey)
                        .putString(KEY_PEER_ID, localPeerId)
                        .apply()
                }

                node = createdNode
                _peerId.value = localPeerId
                _nodeStatus.value = Libp2pNodeStatus.RUNNING

                refreshNodeState()
                startStatusPoller()

                Log.i(TAG, "Libp2pClient started successfully. PeerID: $localPeerId")
                Result.success(localPeerId)
            } catch (e: Throwable) {
                _nodeStatus.value = Libp2pNodeStatus.ERROR
                Log.e(TAG, "Failed to start Libp2pClient", e)
                Result.failure(e)
            }
        }
    }

    /**
     * Stop the libp2p node and release all network sockets.
     */
    suspend fun stop(): Result<Unit> = nodeLock.withLock {
        withContext(Dispatchers.IO) {
            try {
                Log.i(TAG, "Stopping Libp2pClient...")
                statusRefreshJob?.cancel()
                statusRefreshJob = null

                node?.stop()
                node = null

                _nodeStatus.value = Libp2pNodeStatus.STOPPED
                _connectedPeers.value = emptyList()
                _listenAddresses.value = emptyList()

                Log.i(TAG, "Libp2pClient stopped.")
                Result.success(Unit)
            } catch (e: Throwable) {
                Log.e(TAG, "Error stopping Libp2pClient", e)
                Result.failure(e)
            }
        }
    }

    // ========================================================================
    // DIRECT MESSAGING & MEDIA STREAMING
    // ========================================================================

    /**
     * Send a direct text message to a specific peer.
     * @param targetPeerId The remote peer's 52-character libp2p Peer ID.
     * @param text Plaintext message content.
     * @param senderNickname Optional display name of sender.
     * @return Result with unique message ID.
     */
    suspend fun sendDirectMessage(
        targetPeerId: String,
        text: String,
        senderNickname: String? = null
    ): Result<String> = withContext(Dispatchers.IO) {
        val activeNode = node ?: return@withContext Result.failure(IllegalStateException("Node is not running"))
        val messageId = UUID.randomUUID().toString()

        val wire = Libp2pWireMessage(
            type = "dm",
            content = text,
            messageID = messageId,
            senderNickname = senderNickname,
            timestamp = System.currentTimeMillis()
        )

        try {
            val jsonString = gson.toJson(wire)
            activeNode.sendMessage(targetPeerId, jsonString)
            Log.d(TAG, "Direct message sent to $targetPeerId (ID: $messageId)")
            Result.success(messageId)
        } catch (e: Throwable) {
            Log.e(TAG, "Failed to send direct message to $targetPeerId", e)
            Result.failure(e)
        }
    }

    /**
     * Send a binary file / media (photo, audio voice note, document) to a peer.
     * The file is automatically divided into 200 KB chunks and transmitted in parallel.
     *
     * @param targetPeerId The remote peer's libp2p Peer ID.
     * @param fileName Original file name with extension.
     * @param contentType MIME type (e.g. "image/jpeg", "audio/mp4", "application/pdf").
     * @param fileBytes Raw binary file data.
     * @param onProgress Optional progress callback returning (sentChunks, totalChunks).
     * @return Result with the stable transfer chunkId UUID.
     */
    suspend fun sendDirectMedia(
        targetPeerId: String,
        fileName: String,
        contentType: String,
        fileBytes: ByteArray,
        senderNickname: String? = null,
        onProgress: ((sentChunks: Int, totalChunks: Int) -> Unit)? = null
    ): Result<String> = withContext(Dispatchers.IO) {
        val activeNode = node ?: return@withContext Result.failure(IllegalStateException("Node is not running"))
        val chunkId = UUID.randomUUID().toString()
        val chunkSize = config.mediaChunkSizeBytes

        val totalChunks = ceil(fileBytes.size.toDouble() / chunkSize.toDouble()).toInt().coerceAtLeast(1)
        Log.i(TAG, "Streaming media ($fileName, ${fileBytes.size} bytes) in $totalChunks chunks to $targetPeerId")

        try {
            for (index in 0 until totalChunks) {
                val start = index * chunkSize
                val end = (start + chunkSize).coerceAtMost(fileBytes.size)
                val slice = fileBytes.copyOfRange(start, end)
                val base64Data = Base64.encodeToString(slice, Base64.NO_WRAP)

                val wireChunk = Libp2pWireMessage(
                    type = "dm",
                    content = "",
                    messageID = UUID.randomUUID().toString(),
                    senderNickname = senderNickname,
                    timestamp = System.currentTimeMillis(),
                    contentType = contentType,
                    fileName = fileName,
                    fileSize = fileBytes.size.toLong(),
                    chunkId = chunkId,
                    chunkIndex = index,
                    totalChunks = totalChunks,
                    fileData = base64Data
                )

                val jsonChunk = gson.toJson(wireChunk)
                activeNode.sendMessage(targetPeerId, jsonChunk)
                onProgress?.invoke(index + 1, totalChunks)
            }

            Log.i(TAG, "Media transfer $chunkId ($fileName) completed successfully.")
            Result.success(chunkId)
        } catch (e: Throwable) {
            Log.e(TAG, "Failed to stream media $chunkId to $targetPeerId", e)
            Result.failure(e)
        }
    }

    // ========================================================================
    // TOPIC PUB/SUB (GOSSIPSUB MULTICAST)
    // ========================================================================

    /**
     * Subscribe to a GossipSub topic channel and receive a Flow of messages.
     * @param topicName Topic identifier (e.g. "general", "geohash-dr5reg", "room-alpha").
     */
    fun subscribeTopic(topicName: String): Flow<Libp2pMessage> {
        val topicFlow = subscribedTopics.getOrPut(topicName) {
            MutableSharedFlow(extraBufferCapacity = 64)
        }

        scope.launch {
            nodeLock.withLock {
                try {
                    node?.subscribeToTopic(topicName)
                    Log.i(TAG, "Subscribed to GossipSub topic: $topicName")
                } catch (e: Exception) {
                    Log.e(TAG, "Failed to subscribe to topic: $topicName", e)
                }
            }
        }

        return topicFlow.asSharedFlow()
    }

    /**
     * Unsubscribe from a GossipSub topic channel.
     */
    suspend fun unsubscribeTopic(topicName: String): Result<Unit> = withContext(Dispatchers.IO) {
        nodeLock.withLock {
            try {
                node?.unsubscribeFromTopic(topicName)
                subscribedTopics.remove(topicName)
                Log.i(TAG, "Unsubscribed from topic: $topicName")
                Result.success(Unit)
            } catch (e: Throwable) {
                Log.e(TAG, "Failed to unsubscribe from topic: $topicName", e)
                Result.failure(e)
            }
        }
    }

    /**
     * Publish a text message to a GossipSub topic.
     */
    suspend fun publishTopic(
        topicName: String,
        text: String,
        senderNickname: String? = null
    ): Result<Unit> = withContext(Dispatchers.IO) {
        val activeNode = node ?: return@withContext Result.failure(IllegalStateException("Node is not running"))

        val wire = Libp2pWireMessage(
            type = "topic",
            content = text,
            messageID = UUID.randomUUID().toString(),
            senderNickname = senderNickname,
            timestamp = System.currentTimeMillis()
        )

        try {
            val jsonString = gson.toJson(wire)
            activeNode.publishToTopic(topicName, jsonString)
            Log.d(TAG, "Published message to topic: $topicName")
            Result.success(Unit)
        } catch (e: Throwable) {
            Log.e(TAG, "Failed to publish to topic: $topicName", e)
            Result.failure(e)
        }
    }

    /**
     * Publish a media file to a GossipSub topic.
     */
    suspend fun publishTopicMedia(
        topicName: String,
        fileName: String,
        contentType: String,
        fileBytes: ByteArray,
        senderNickname: String? = null
    ): Result<Unit> = withContext(Dispatchers.IO) {
        val activeNode = node ?: return@withContext Result.failure(IllegalStateException("Node is not running"))
        val chunkId = UUID.randomUUID().toString()
        val chunkSize = config.mediaChunkSizeBytes
        val totalChunks = ceil(fileBytes.size.toDouble() / chunkSize.toDouble()).toInt().coerceAtLeast(1)

        try {
            for (index in 0 until totalChunks) {
                val start = index * chunkSize
                val end = (start + chunkSize).coerceAtMost(fileBytes.size)
                val slice = fileBytes.copyOfRange(start, end)
                val base64Data = Base64.encodeToString(slice, Base64.NO_WRAP)

                val wireChunk = Libp2pWireMessage(
                    type = "topic",
                    content = "",
                    messageID = UUID.randomUUID().toString(),
                    senderNickname = senderNickname,
                    timestamp = System.currentTimeMillis(),
                    contentType = contentType,
                    fileName = fileName,
                    fileSize = fileBytes.size.toLong(),
                    chunkId = chunkId,
                    chunkIndex = index,
                    totalChunks = totalChunks,
                    fileData = base64Data
                )

                val jsonChunk = gson.toJson(wireChunk)
                activeNode.publishToTopic(topicName, jsonChunk)
            }
            Log.i(TAG, "Published media $chunkId ($fileName) to topic: $topicName")
            Result.success(Unit)
        } catch (e: Throwable) {
            Log.e(TAG, "Failed to publish media to topic: $topicName", e)
            Result.failure(e)
        }
    }

    // ========================================================================
    // PEER MANAGEMENT & NETWORKING
    // ========================================================================

    /** Connect directly to a peer multiaddress */
    suspend fun connectPeer(multiaddr: String): Result<Unit> = withContext(Dispatchers.IO) {
        val activeNode = node ?: return@withContext Result.failure(IllegalStateException("Node not running"))
        try {
            activeNode.connectToPeer(multiaddr)
            Result.success(Unit)
        } catch (e: Throwable) {
            Result.failure(e)
        }
    }

    /** Disconnect a connected peer */
    suspend fun disconnectPeer(peerId: String): Result<Unit> = withContext(Dispatchers.IO) {
        val activeNode = node ?: return@withContext Result.failure(IllegalStateException("Node not running"))
        try {
            activeNode.disconnectPeer(peerId)
            Result.success(Unit)
        } catch (e: Throwable) {
            Result.failure(e)
        }
    }

    /** Add a custom bootstrap peer */
    suspend fun addBootstrapPeer(multiaddr: String): Result<Unit> = withContext(Dispatchers.IO) {
        val activeNode = node ?: return@withContext Result.failure(IllegalStateException("Node not running"))
        try {
            activeNode.addBootstrapPeer(multiaddr)
            Result.success(Unit)
        } catch (e: Throwable) {
            Result.failure(e)
        }
    }

    /** Get known multiaddresses for a peer */
    fun getPeerMultiaddresses(peerId: String): List<String> {
        return peerAddressMap[peerId]?.toList() ?: emptyList()
    }

    // ========================================================================
    // INTERNAL BRIDGE HANDLERS & MESSAGE PROCESSING
    // ========================================================================

    private fun setupBridgeHandlers(node: P2PNode) {
        // Direct stream message handler
        node.setMessageHandler(object : MobileMessageHandler {
            override fun handleMessage(senderPeerID: String, content: String) {
                scope.launch {
                    processIncomingPayload(senderPeerID, content, isTopic = false, topicName = null)
                }
            }
        })

        // GossipSub topic message handler
        node.setTopicMessageHandler(object : MobileTopicMessageHandler {
            override fun handleTopicMessage(topicName: String, senderPeerID: String, content: String) {
                scope.launch {
                    processIncomingPayload(senderPeerID, content, isTopic = true, topicName = topicName)
                }
            }
        })

        // Connection state callback
        node.setConnectionHandler(object : MobileConnectionHandler {
            override fun handlePeerConnected(peerID: String, remoteAddr: String) {
                scope.launch {
                    val addrs = peerAddressMap.getOrPut(peerID) { ConcurrentHashMap.newKeySet() }
                    if (remoteAddr.isNotBlank()) addrs.add(remoteAddr)
                    _peerEvents.emit(Libp2pPeerEvent(peerId = peerID, isConnected = true, multiaddress = remoteAddr))
                    refreshNodeState()
                }
            }

            override fun handlePeerDisconnected(peerID: String) {
                scope.launch {
                    _peerEvents.emit(Libp2pPeerEvent(peerId = peerID, isConnected = false))
                    refreshNodeState()
                }
            }
        })

        // Peer address stream callback
        node.setPeerAddressHandler(object : MobilePeerAddressHandler {
            override fun handlePeerAddress(peerID: String, multiaddr: String) {
                if (peerID.isNotBlank() && multiaddr.isNotBlank()) {
                    val addrs = peerAddressMap.getOrPut(peerID) { ConcurrentHashMap.newKeySet() }
                    addrs.add(multiaddr)
                }
            }
        })
    }

    private suspend fun processIncomingPayload(
        senderPeerId: String,
        rawPayload: String,
        isTopic: Boolean,
        topicName: String?
    ) {
        val wireMessage: Libp2pWireMessage = try {
            gson.fromJson(rawPayload, Libp2pWireMessage::class.java)
        } catch (e: Exception) {
            // Fallback for plain raw text
            Libp2pWireMessage(
                type = if (isTopic) "topic" else "dm",
                content = rawPayload,
                messageID = UUID.randomUUID().toString(),
                senderNickname = null,
                timestamp = System.currentTimeMillis()
            )
        }

        // Check if this is a media chunk
        if (wireMessage.chunkId != null && wireMessage.chunkIndex != null &&
            wireMessage.totalChunks != null && wireMessage.fileData != null) {

            val rawChunkBytes = try {
                Base64.decode(wireMessage.fileData, Base64.DEFAULT)
            } catch (e: Exception) {
                Log.w(TAG, "Base64 decode failed for chunk ${wireMessage.chunkIndex}", e)
                return
            }

            val assembled = chunkAssembler.addChunk(
                chunkId = wireMessage.chunkId,
                chunkIndex = wireMessage.chunkIndex,
                totalChunks = wireMessage.totalChunks,
                contentType = wireMessage.contentType ?: "application/octet-stream",
                fileName = wireMessage.fileName ?: "file.bin",
                chunkData = rawChunkBytes
            )

            if (assembled != null) {
                val fullMessage = Libp2pMessage(
                    senderPeerId = senderPeerId,
                    content = "",
                    messageId = wireMessage.chunkId,
                    senderNickname = wireMessage.senderNickname,
                    timestamp = wireMessage.timestamp,
                    isTopicMessage = isTopic,
                    topicName = topicName,
                    fileBytes = assembled.bytes,
                    fileName = assembled.fileName,
                    contentType = assembled.contentType,
                    fileSize = assembled.bytes.size.toLong()
                )
                dispatchMessage(fullMessage)
            }
        } else {
            // Standard text message
            val textMessage = Libp2pMessage(
                senderPeerId = senderPeerId,
                content = wireMessage.content,
                messageId = wireMessage.messageID,
                senderNickname = wireMessage.senderNickname,
                timestamp = wireMessage.timestamp,
                isTopicMessage = isTopic,
                topicName = topicName
            )
            dispatchMessage(textMessage)
        }
    }

    private suspend fun dispatchMessage(message: Libp2pMessage) {
        _incomingMessages.emit(message)
        if (message.isTopicMessage && message.topicName != null) {
            subscribedTopics[message.topicName]?.emit(message)
        }
    }

    private fun refreshNodeState() {
        val activeNode = node ?: return
        try {
            // Refresh listen addresses
            val addrsJson = activeNode.listenAddresses
            if (addrsJson.isNotBlank() && addrsJson != "[]") {
                val arr = JSONArray(addrsJson)
                val list = (0 until arr.length()).map { arr.getString(it) }
                _listenAddresses.value = list
            }

            // Refresh connected peers
            val peersJson = activeNode.connectedPeers
            if (peersJson.isNotBlank() && peersJson != "[]") {
                val arr = JSONArray(peersJson)
                val list = (0 until arr.length()).map { arr.getString(it) }
                _connectedPeers.value = list
            } else {
                _connectedPeers.value = emptyList()
            }
        } catch (e: Exception) {
            Log.w(TAG, "State refresh error", e)
        }
    }

    private fun startStatusPoller() {
        statusRefreshJob?.cancel()
        statusRefreshJob = scope.launch {
            while (isActive) {
                delay(STATUS_REFRESH_INTERVAL_MS)
                refreshNodeState()
                pollBandwidthStats()
            }
        }
    }

    private fun pollBandwidthStats() {
        val activeNode = node ?: return
        try {
            val statsJson = activeNode.bandwidthStats
            if (statsJson.isNotBlank() && statsJson.startsWith("{")) {
                val obj = JSONObject(statsJson)
                val stats = Libp2pBandwidthStats(
                    sessionInBytes = obj.optLong("sessionInBytes", 0L),
                    sessionOutBytes = obj.optLong("sessionOutBytes", 0L),
                    sessionTotalBytes = obj.optLong("sessionTotalBytes", 0L),
                    dailyInBytes = obj.optLong("dailyInBytes", 0L),
                    dailyOutBytes = obj.optLong("dailyOutBytes", 0L),
                    dailyTotalBytes = obj.optLong("dailyTotalBytes", 0L),
                    currentRateInBytesPerSec = obj.optDouble("currentRateInBytesPerSec", 0.0),
                    currentRateOutBytesPerSec = obj.optDouble("currentRateOutBytesPerSec", 0.0),
                    projectedHourlyTotalBytes = obj.optLong("projectedHourlyTotalBytes", 0L),
                    rawJson = statsJson
                )
                _bandwidthStats.value = stats
            }
        } catch (e: Exception) {
            // Ignore polling parse errors
        }
    }

    private fun setupNetworkCallback() {
        val cm = context.getSystemService(Context.CONNECTIVITY_SERVICE) as? ConnectivityManager ?: return
        cm.registerDefaultNetworkCallback(object : ConnectivityManager.NetworkCallback() {
            override fun onAvailable(network: Network) {
                scope.launch {
                    if (_nodeStatus.value == Libp2pNodeStatus.RUNNING) {
                        Log.i(TAG, "Network became available; refreshing libp2p state...")
                        refreshNodeState()
                    }
                }
            }
            override fun onLost(network: Network) {
                scope.launch {
                    Log.w(TAG, "Network lost; updating libp2p state...")
                    refreshNodeState()
                }
            }
        })
    }
}

// ============================================================================
// INTERNAL WIRE MESSAGING & CHUNK ASSEMBLER
// ============================================================================

/** Internal JSON Wire Framing Format */
data class Libp2pWireMessage(
    val type: String,               // "dm", "topic"
    val content: String,            // Text content
    val messageID: String,          // Unique message UUID
    val senderNickname: String?,    // Display alias
    val timestamp: Long,            // Unix epoch ms
    val contentType: String? = null,
    val fileName: String? = null,
    val fileSize: Long? = null,
    val chunkId: String? = null,    // Multi-chunk transfer UUID
    val chunkIndex: Int? = null,    // 0-based chunk index
    val totalChunks: Int? = null,   // Total chunk count
    val fileData: String? = null    // Base64 chunk bytes
)

/** Reassembles 200 KB multi-chunk binary media transfers with auto-expiration */
class Libp2pChunkAssembler(
    private val timeoutMs: Long = 30_000L,
    private val cleanupIntervalMs: Long = 10_000L
) {
    companion object {
        private const val TAG = "Libp2pChunkAssembler"
    }

    data class AssembledMedia(val bytes: ByteArray, val contentType: String, val fileName: String)

    private data class ChunkState(
        val chunks: MutableMap<Int, ByteArray> = mutableMapOf(),
        val contentType: String,
        val fileName: String,
        val totalChunks: Int,
        val timestamp: Long = System.currentTimeMillis()
    )

    private val pending = ConcurrentHashMap<String, ChunkState>()
    private val job = SupervisorJob()
    private val scope = CoroutineScope(Dispatchers.IO + job)

    init {
        scope.launch {
            while (isActive) {
                delay(cleanupIntervalMs)
                cleanup()
            }
        }
    }

    fun addChunk(
        chunkId: String,
        chunkIndex: Int,
        totalChunks: Int,
        contentType: String,
        fileName: String,
        chunkData: ByteArray
    ): AssembledMedia? {
        val state = pending.getOrPut(chunkId) {
            ChunkState(contentType = contentType, fileName = fileName, totalChunks = totalChunks)
        }

        synchronized(state.chunks) {
            state.chunks[chunkIndex] = chunkData
            if (state.chunks.size == totalChunks) {
                val ordered = (0 until totalChunks).mapNotNull { i -> state.chunks[i] }
                if (ordered.size < totalChunks) return null
                val merged = ordered.reduce { acc, b -> acc + b }
                pending.remove(chunkId)
                Log.d(TAG, "Successfully reassembled $chunkId ($fileName, ${merged.size} bytes)")
                return AssembledMedia(merged, state.contentType, state.fileName)
            }
        }
        return null
    }

    private fun cleanup() {
        val cutoff = System.currentTimeMillis() - timeoutMs
        val expired = pending.entries.filter { it.value.timestamp < cutoff }.map { it.key }
        expired.forEach {
            pending.remove(it)
            Log.d(TAG, "Expired incomplete chunk transfer: $it")
        }
    }

    fun shutdown() {
        job.cancel()
    }
}
