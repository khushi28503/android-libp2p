package com.roman.zemzeme.p2p

import android.content.Context
import android.content.SharedPreferences
import android.net.ConnectivityManager
import android.net.Network
import android.util.Base64
import android.util.Log
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
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
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.ceil

// ============================================================================
// 1. P2P DATA MODELS, CONSTANTS & ENUMS
// ============================================================================

/** Node lifecycle status */
enum class P2PNodeStatus {
    STOPPED,
    STARTING,
    RUNNING,
    ERROR
}

/** P2P message types */
enum class P2PMessageType {
    DIRECT_MESSAGE,
    TOPIC_MESSAGE,
    CHANNEL_MESSAGE
}

/** Peer connection event */
data class P2PPeerEvent(
    val peerId: String,
    val isConnected: Boolean,
    val multiaddress: String? = null,
    val timestamp: Long = System.currentTimeMillis()
)

/** Peer snapshot information */
data class P2PPeer(
    val peerId: String,
    val isConnected: Boolean = false,
    val multiaddresses: List<String> = emptyList(),
    val lastSeen: Long = System.currentTimeMillis()
)

/** Incoming P2P message (text or chunked media) */
data class P2PIncomingMessage(
    val senderPeerID: String,
    val content: String,
    val type: P2PMessageType,
    val messageId: String = UUID.randomUUID().toString(),
    val topicName: String? = null,
    val timestamp: Long = System.currentTimeMillis(),
    val senderNickname: String? = null,
    val fileBytes: ByteArray? = null,
    val fileName: String? = null,
    val contentType: String? = null,
    val fileSize: Long? = null
) {
    val isMediaMessage: Boolean get() = fileBytes != null
}

/** Internal P2P JSON wire message framing */
data class P2PWireMessage(
    val type: String,               // "dm", "topic", "channel"
    val content: String,            // Plaintext content
    val messageID: String,          // Unique message UUID
    val senderNickname: String?,    // Sender display alias
    val timestamp: Long,            // Unix epoch ms
    val contentType: String? = null,
    val fileName: String? = null,
    val fileSize: Long? = null,
    val chunkId: String? = null,    // Chunk transfer UUID
    val chunkIndex: Int? = null,    // 0-based chunk index
    val totalChunks: Int? = null,   // Total chunk count
    val fileData: String? = null    // Base64 chunk bytes
)

/** Live P2P bandwidth telemetry */
data class P2PBandwidthStats(
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

/** GossipSub topic message */
data class P2PTopicMessage(
    val topicName: String,
    val senderPeerID: String,
    val content: String,
    val timestamp: Long = System.currentTimeMillis(),
    val isDirect: Boolean = false,
    val messageID: String = UUID.randomUUID().toString()
)

/** P2P system constants */
object P2PConstants {
    const val PROTOCOL_DIRECT = "/zemzeme/direct/1.0.0"
    const val PROTOCOL_GOSSIPSUB = "/meshsub/1.1.0"
    const val PROTOCOL_DHT = "/ipfs/kad/1.0.0"
    const val PROTOCOL_RELAY_V2 = "/libp2p/circuit/relay/0.2.0/hop"
    const val PROTOCOL_RELAY_V2_STOP = "/libp2p/circuit/relay/0.2.0/stop"

    const val DEFAULT_CHUNK_SIZE = 200 * 1024 // 200 KB
    const val CHUNK_TIMEOUT_MS = 30_000L
    const val STATUS_POLL_INTERVAL_MS = 5_000L

    val DEFAULT_BOOTSTRAP_NODES = listOf(
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

// ============================================================================
// 2. CHUNK ASSEMBLER (200 KB BINARY SLICES)
// ============================================================================

/**
 * Reassembles multi-chunk P2P binary media transfers with auto-expiration.
 */
class P2PChunkAssembler(
    private val timeoutMs: Long = P2PConstants.CHUNK_TIMEOUT_MS,
    private val cleanupIntervalMs: Long = 10_000L
) {
    companion object {
        private const val TAG = "P2PChunkAssembler"
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
                Log.d(TAG, "Reassembled $chunkId ($fileName, ${merged.size} bytes)")
                return AssembledMedia(merged, state.contentType, state.fileName)
            }
        }
        return null
    }

    private fun cleanup() {
        val cutoff = System.currentTimeMillis() - timeoutMs
        val expired = pending.entries.filter { it.value.timestamp < cutoff }.map { it.key }
        expired.forEach { pending.remove(it) }
    }

    fun shutdown() {
        job.cancel()
    }
}

// ============================================================================
// 3. P2P CONFIGURATION MANAGER
// ============================================================================

/**
 * P2P Configuration Manager for managing node settings, DHT, and bootstrap peers.
 */
class P2PConfig(private val context: Context) {
    companion object {
        private const val PREFS_NAME = "p2p_config_prefs"
        private const val KEY_BOOTSTRAP_NODES = "custom_bootstrap_nodes"
        private const val KEY_ENABLE_RELAY = "enable_relay"
        private const val KEY_LISTEN_PORT = "listen_port"
        private const val KEY_BANDWIDTH_LIMIT = "bandwidth_limit"
    }

    private val prefs: SharedPreferences = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
    private val gson = Gson()

    fun getBootstrapNodes(): List<String> {
        val customJson = prefs.getString(KEY_BOOTSTRAP_NODES, null)
        if (!customJson.isNullOrBlank()) {
            try {
                val type = object : TypeToken<List<String>>() {}.type
                val list: List<String> = gson.fromJson(customJson, type)
                if (list.isNotEmpty()) return list
            } catch (e: Exception) {
                // fallback
            }
        }
        return P2PConstants.DEFAULT_BOOTSTRAP_NODES
    }

    fun setBootstrapNodes(nodes: List<String>) {
        prefs.edit().putString(KEY_BOOTSTRAP_NODES, gson.toJson(nodes)).apply()
    }

    fun isRelayEnabled(): Boolean = prefs.getBoolean(KEY_ENABLE_RELAY, true)
    fun setRelayEnabled(enabled: Boolean) = prefs.edit().putBoolean(KEY_ENABLE_RELAY, enabled).apply()

    fun getListenPort(): Int = prefs.getInt(KEY_LISTEN_PORT, 0)
    fun setListenPort(port: Int) = prefs.edit().putInt(KEY_LISTEN_PORT, port).apply()

    fun getBandwidthLimit(): Long = prefs.getLong(KEY_BANDWIDTH_LIMIT, 0L)
    fun setBandwidthLimit(limit: Long) = prefs.edit().putLong(KEY_BANDWIDTH_LIMIT, limit).apply()
}

// ============================================================================
// 4. P2P ALIAS & FAVORITES REGISTRIES
// ============================================================================

/**
 * Maps P2P conversation keys ("p2p:12D3KooW...") to display nicknames.
 */
object P2PAliasRegistry {
    private const val PREFS_NAME = "p2p_alias_registry"
    private val rawPeerIdMap = ConcurrentHashMap<String, String>()
    private val displayNameMap = ConcurrentHashMap<String, String>()
    private var prefs: SharedPreferences? = null

    fun initialize(context: Context) {
        prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
        prefs?.all?.forEach { (k, v) ->
            if (v is String) {
                when {
                    k.startsWith("raw_") -> rawPeerIdMap[k.removePrefix("raw_")] = v
                    k.startsWith("display_") -> displayNameMap[k.removePrefix("display_")] = v
                }
            }
        }
    }

    fun put(convKey: String, rawPeerId: String) {
        rawPeerIdMap[convKey] = rawPeerId
        prefs?.edit()?.putString("raw_$convKey", rawPeerId)?.apply()
    }

    fun get(convKey: String): String? = rawPeerIdMap[convKey]
    fun setDisplayName(convKey: String, name: String) {
        displayNameMap[convKey] = name
        prefs?.edit()?.putString("display_$convKey", name)?.apply()
    }
    fun getDisplayName(convKey: String): String? = displayNameMap[convKey]
    fun clear() {
        rawPeerIdMap.clear()
        displayNameMap.clear()
        prefs?.edit()?.clear()?.apply()
    }
}

/**
 * Stores starred / favorite P2P peer IDs.
 */
object P2PFavoritesRegistry {
    private const val PREFS_NAME = "p2p_favorites_registry"
    private val favorites = ConcurrentHashMap.newKeySet<String>()
    private var prefs: SharedPreferences? = null

    fun initialize(context: Context) {
        prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
        prefs?.getStringSet("favorites", emptySet())?.let { favorites.addAll(it) }
    }

    fun isFavorite(peerId: String): Boolean = favorites.contains(peerId)

    fun toggleFavorite(peerId: String): Boolean {
        val isNowFav = if (favorites.contains(peerId)) {
            favorites.remove(peerId)
            false
        } else {
            favorites.add(peerId)
            true
        }
        prefs?.edit()?.putStringSet("favorites", favorites.toSet())?.apply()
        return isNowFav
    }
}

// ============================================================================
// 5. CORE P2P LIBRARY REPOSITORY (GO LIBP2P BRIDGE)
// ============================================================================

/**
 * Core P2P engine managing the Go libp2p node lifecycle, DHT discovery,
 * STUN/TURN ICE hole-punching, bandwidth telemetry, and callbacks.
 */
class P2PLibraryRepository(private val context: Context) {
    companion object {
        private const val TAG = "P2PLibraryRepository"
        private const val PREFS_NAME = "p2p_repo_prefs"
        private const val KEY_PRIVATE_KEY = "p2p_private_key"
        private const val KEY_PEER_ID = "p2p_peer_id"
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val gson = Gson()
    private val nodeLock = Mutex()
    private var node: P2PNode? = null
    private var pollerJob: Job? = null

    private val p2pConfig = P2PConfig(context)
    private val prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)

    // Observable states
    private val _nodeStatus = MutableStateFlow(P2PNodeStatus.STOPPED)
    val nodeStatus: StateFlow<P2PNodeStatus> = _nodeStatus.asStateFlow()

    private val _peerID = MutableStateFlow<String?>(prefs.getString(KEY_PEER_ID, null))
    val peerID: StateFlow<String?> = _peerID.asStateFlow()

    private val _connectedPeers = MutableStateFlow<List<String>>(emptyList())
    val connectedPeers: StateFlow<List<String>> = _connectedPeers.asStateFlow()

    private val _listenAddresses = MutableStateFlow<List<String>>(emptyList())
    val listenAddresses: StateFlow<List<String>> = _listenAddresses.asStateFlow()

    private val _bandwidthStats = MutableStateFlow(P2PBandwidthStats())
    val bandwidthStats: StateFlow<P2PBandwidthStats> = _bandwidthStats.asStateFlow()

    private val _incomingMessages = MutableSharedFlow<P2PIncomingMessage>(extraBufferCapacity = 64)
    val incomingMessages: SharedFlow<P2PIncomingMessage> = _incomingMessages.asSharedFlow()

    private val _peerEvents = MutableSharedFlow<P2PPeerEvent>(extraBufferCapacity = 64)
    val peerEvents: SharedFlow<P2PPeerEvent> = _peerEvents.asSharedFlow()

    private val peerAddressMap = ConcurrentHashMap<String, MutableSet<String>>()

    init {
        setupNetworkListener()
    }

    suspend fun startNode(privateKeyBase64: String? = null): Result<Unit> = nodeLock.withLock {
        withContext(Dispatchers.IO) {
            if (_nodeStatus.value == P2PNodeStatus.RUNNING && node != null) {
                return@withContext Result.success(Unit)
            }

            try {
                _nodeStatus.value = P2PNodeStatus.STARTING
                val key = privateKeyBase64 ?: prefs.getString(KEY_PRIVATE_KEY, "") ?: ""
                val dataDir = File(context.filesDir, "p2p_data").apply { mkdirs() }.absolutePath

                val createdNode = Golib.newP2PNode(
                    p2pConfig.getListenPort().toLong(),
                    p2pConfig.isRelayEnabled(),
                    key,
                    dataDir
                ) ?: return@withContext Result.failure(IllegalStateException("Golib.newP2PNode returned null"))

                wireBridgeHandlers(createdNode)

                // Set ICE STUN/TURN servers
                val iceJson = JSONArray(P2PConstants.DEFAULT_ICE_SERVERS).toString()
                try { createdNode.setIceServers(iceJson) } catch (e: Exception) { Log.w(TAG, "setIceServers failed", e) }

                // Set bandwidth limit
                val limit = p2pConfig.getBandwidthLimit()
                if (limit > 0) {
                    try { createdNode.setBandwidthLimit(limit) } catch (e: Exception) { Log.w(TAG, "setBandwidthLimit failed", e) }
                }

                // Add bootstrap nodes
                for (bootstrap in p2pConfig.getBootstrapNodes()) {
                    try { createdNode.addBootstrapPeer(bootstrap) } catch (e: Exception) { Log.w(TAG, "addBootstrapPeer failed", e) }
                }

                createdNode.start()

                val localId = createdNode.peerID
                val exportedKey = try { createdNode.exportPrivateKey() } catch (e: Exception) { "" }

                if (exportedKey.isNotEmpty()) {
                    prefs.edit().putString(KEY_PRIVATE_KEY, exportedKey).putString(KEY_PEER_ID, localId).apply()
                }

                node = createdNode
                _peerID.value = localId
                _nodeStatus.value = P2PNodeStatus.RUNNING

                refreshState()
                startPoller()

                Log.i(TAG, "P2P Node started successfully. PeerID: $localId")
                Result.success(Unit)
            } catch (e: Throwable) {
                _nodeStatus.value = P2PNodeStatus.ERROR
                Log.e(TAG, "Failed to start P2P Node", e)
                Result.failure(e)
            }
        }
    }

    suspend fun stopNode(): Result<Unit> = nodeLock.withLock {
        withContext(Dispatchers.IO) {
            try {
                pollerJob?.cancel()
                pollerJob = null

                node?.stop()
                node = null

                _nodeStatus.value = P2PNodeStatus.STOPPED
                _connectedPeers.value = emptyList()
                _listenAddresses.value = emptyList()

                Result.success(Unit)
            } catch (e: Throwable) {
                Result.failure(e)
            }
        }
    }

    suspend fun sendMessage(targetPeerId: String, payloadJson: String): Result<Unit> = withContext(Dispatchers.IO) {
        val activeNode = node ?: return@withContext Result.failure(IllegalStateException("Node not running"))
        try {
            activeNode.sendMessage(targetPeerId, payloadJson)
            Result.success(Unit)
        } catch (e: Throwable) {
            Result.failure(e)
        }
    }

    suspend fun publishToTopic(topicName: String, payloadJson: String): Result<Unit> = withContext(Dispatchers.IO) {
        val activeNode = node ?: return@withContext Result.failure(IllegalStateException("Node not running"))
        try {
            activeNode.publishToTopic(topicName, payloadJson)
            Result.success(Unit)
        } catch (e: Throwable) {
            Result.failure(e)
        }
    }

    suspend fun subscribeToTopic(topicName: String): Result<Unit> = withContext(Dispatchers.IO) {
        val activeNode = node ?: return@withContext Result.failure(IllegalStateException("Node not running"))
        try {
            activeNode.subscribeToTopic(topicName)
            Result.success(Unit)
        } catch (e: Throwable) {
            Result.failure(e)
        }
    }

    suspend fun unsubscribeFromTopic(topicName: String): Result<Unit> = withContext(Dispatchers.IO) {
        val activeNode = node ?: return@withContext Result.failure(IllegalStateException("Node not running"))
        try {
            activeNode.unsubscribeFromTopic(topicName)
            Result.success(Unit)
        } catch (e: Throwable) {
            Result.failure(e)
        }
    }

    suspend fun connectToPeer(multiaddr: String): Result<Unit> = withContext(Dispatchers.IO) {
        val activeNode = node ?: return@withContext Result.failure(IllegalStateException("Node not running"))
        try {
            activeNode.connectToPeer(multiaddr)
            Result.success(Unit)
        } catch (e: Throwable) {
            Result.failure(e)
        }
    }

    suspend fun disconnectPeer(peerId: String): Result<Unit> = withContext(Dispatchers.IO) {
        val activeNode = node ?: return@withContext Result.failure(IllegalStateException("Node not running"))
        try {
            activeNode.disconnectPeer(peerId)
            Result.success(Unit)
        } catch (e: Throwable) {
            Result.failure(e)
        }
    }

    private fun wireBridgeHandlers(node: P2PNode) {
        node.setMessageHandler(object : MobileMessageHandler {
            override fun handleMessage(senderPeerID: String, content: String) {
                scope.launch {
                    _incomingMessages.emit(
                        P2PIncomingMessage(
                            senderPeerID = senderPeerID,
                            content = content,
                            type = P2PMessageType.DIRECT_MESSAGE
                        )
                    )
                }
            }
        })

        node.setTopicMessageHandler(object : MobileTopicMessageHandler {
            override fun handleTopicMessage(topicName: String, senderPeerID: String, content: String) {
                scope.launch {
                    _incomingMessages.emit(
                        P2PIncomingMessage(
                            senderPeerID = senderPeerID,
                            content = content,
                            type = P2PMessageType.TOPIC_MESSAGE,
                            topicName = topicName
                        )
                    )
                }
            }
        })

        node.setConnectionHandler(object : MobileConnectionHandler {
            override fun handlePeerConnected(peerID: String, remoteAddr: String) {
                scope.launch {
                    val addrs = peerAddressMap.getOrPut(peerID) { ConcurrentHashMap.newKeySet() }
                    if (remoteAddr.isNotBlank()) addrs.add(remoteAddr)
                    _peerEvents.emit(P2PPeerEvent(peerId = peerID, isConnected = true, multiaddress = remoteAddr))
                    refreshState()
                }
            }

            override fun handlePeerDisconnected(peerID: String) {
                scope.launch {
                    _peerEvents.emit(P2PPeerEvent(peerId = peerID, isConnected = false))
                    refreshState()
                }
            }
        })

        node.setPeerAddressHandler(object : MobilePeerAddressHandler {
            override fun handlePeerAddress(peerID: String, multiaddr: String) {
                if (peerID.isNotBlank() && multiaddr.isNotBlank()) {
                    val addrs = peerAddressMap.getOrPut(peerID) { ConcurrentHashMap.newKeySet() }
                    addrs.add(multiaddr)
                }
            }
        })
    }

    private fun refreshState() {
        val activeNode = node ?: return
        try {
            val addrs = activeNode.listenAddresses
            if (addrs.isNotBlank() && addrs != "[]") {
                val arr = JSONArray(addrs)
                _listenAddresses.value = (0 until arr.length()).map { arr.getString(it) }
            }

            val peers = activeNode.connectedPeers
            if (peers.isNotBlank() && peers != "[]") {
                val arr = JSONArray(peers)
                _connectedPeers.value = (0 until arr.length()).map { arr.getString(it) }
            } else {
                _connectedPeers.value = emptyList()
            }
        } catch (e: Exception) {
            Log.w(TAG, "refreshState error", e)
        }
    }

    private fun startPoller() {
        pollerJob?.cancel()
        pollerJob = scope.launch {
            while (isActive) {
                delay(P2PConstants.STATUS_POLL_INTERVAL_MS)
                refreshState()
                pollBandwidth()
            }
        }
    }

    private fun pollBandwidth() {
        val activeNode = node ?: return
        try {
            val json = activeNode.bandwidthStats
            if (json.isNotBlank() && json.startsWith("{")) {
                val obj = JSONObject(json)
                _bandwidthStats.value = P2PBandwidthStats(
                    sessionInBytes = obj.optLong("sessionInBytes", 0L),
                    sessionOutBytes = obj.optLong("sessionOutBytes", 0L),
                    sessionTotalBytes = obj.optLong("sessionTotalBytes", 0L),
                    dailyInBytes = obj.optLong("dailyInBytes", 0L),
                    dailyOutBytes = obj.optLong("dailyOutBytes", 0L),
                    dailyTotalBytes = obj.optLong("dailyTotalBytes", 0L),
                    currentRateInBytesPerSec = obj.optDouble("currentRateInBytesPerSec", 0.0),
                    currentRateOutBytesPerSec = obj.optDouble("currentRateOutBytesPerSec", 0.0),
                    projectedHourlyTotalBytes = obj.optLong("projectedHourlyTotalBytes", 0L),
                    rawJson = json
                )
            }
        } catch (e: Exception) {
            // ignore
        }
    }

    private fun setupNetworkListener() {
        val cm = context.getSystemService(Context.CONNECTIVITY_SERVICE) as? ConnectivityManager ?: return
        cm.registerDefaultNetworkCallback(object : ConnectivityManager.NetworkCallback() {
            override fun onAvailable(network: Network) {
                scope.launch { if (_nodeStatus.value == P2PNodeStatus.RUNNING) refreshState() }
            }
            override fun onLost(network: Network) {
                scope.launch { refreshState() }
            }
        })
    }
}

// ============================================================================
// 6. TOPICS REPOSITORY (GOSSIPSUB PUB/SUB)
// ============================================================================

/**
 * Manages GossipSub topic subscriptions and multicast message streams.
 */
class P2PTopicsRepository(
    private val context: Context,
    private val p2pRepository: P2PLibraryRepository
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val gson = Gson()

    private val _topicMessages = MutableSharedFlow<P2PTopicMessage>(extraBufferCapacity = 64)
    val topicMessages: SharedFlow<P2PTopicMessage> = _topicMessages.asSharedFlow()

    private val subscribedTopics = ConcurrentHashMap.newKeySet<String>()

    init {
        p2pRepository.incomingMessages
            .filter { it.type == P2PMessageType.TOPIC_MESSAGE && it.topicName != null }
            .onEach { incoming ->
                _topicMessages.emit(
                    P2PTopicMessage(
                        topicName = incoming.topicName!!,
                        senderPeerID = incoming.senderPeerID,
                        content = incoming.content,
                        timestamp = incoming.timestamp
                    )
                )
            }
            .launchIn(scope)
    }

    suspend fun subscribe(topicName: String): Result<Unit> {
        val res = p2pRepository.subscribeToTopic(topicName)
        if (res.isSuccess) subscribedTopics.add(topicName)
        return res
    }

    suspend fun unsubscribe(topicName: String): Result<Unit> {
        val res = p2pRepository.unsubscribeFromTopic(topicName)
        if (res.isSuccess) subscribedTopics.remove(topicName)
        return res
    }

    suspend fun publish(topicName: String, text: String, senderNickname: String? = null): Result<Unit> {
        val wire = P2PWireMessage(
            type = "topic",
            content = text,
            messageID = UUID.randomUUID().toString(),
            senderNickname = senderNickname,
            timestamp = System.currentTimeMillis()
        )
        return p2pRepository.publishToTopic(topicName, gson.toJson(wire))
    }

    fun isSubscribed(topicName: String): Boolean = subscribedTopics.contains(topicName)
    fun getSubscribedTopics(): Set<String> = subscribedTopics.toSet()
}

// ============================================================================
// 7. P2P TRANSPORT & CHUNKED FILE COORDINATOR
// ============================================================================

/**
 * High-level P2P Message and File Transfer Coordinator.
 * Handles automatic 200 KB chunking, reassembly, and message routing.
 */
class P2PTransport private constructor(private val context: Context) {
    companion object {
        private const val TAG = "P2PTransport"

        @Volatile
        private var instance: P2PTransport? = null

        fun getInstance(context: Context): P2PTransport {
            return instance ?: synchronized(this) {
                instance ?: P2PTransport(context.applicationContext).also { instance = it }
            }
        }
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val gson = Gson()
    private val chunkAssembler = P2PChunkAssembler()

    val repository: P2PLibraryRepository by lazy { P2PLibraryRepository(context) }
    val topics: P2PTopicsRepository by lazy { P2PTopicsRepository(context, repository) }

    private val _messages = MutableSharedFlow<P2PIncomingMessage>(extraBufferCapacity = 64)
    val messages: SharedFlow<P2PIncomingMessage> = _messages.asSharedFlow()

    init {
        P2PAliasRegistry.initialize(context)
        P2PFavoritesRegistry.initialize(context)

        repository.incomingMessages
            .onEach { rawMsg -> handleRawIncoming(rawMsg) }
            .launchIn(scope)
    }

    suspend fun start(privateKeyBase64: String? = null): Result<Unit> = repository.startNode(privateKeyBase64)
    suspend fun stop(): Result<Unit> = repository.stopNode()
    fun isRunning(): Boolean = repository.nodeStatus.value == P2PNodeStatus.RUNNING
    fun getMyPeerID(): String? = repository.peerID.value

    /** Send direct text message */
    suspend fun sendDirectMessage(
        targetPeerId: String,
        text: String,
        senderNickname: String? = null,
        messageId: String = UUID.randomUUID().toString()
    ): Result<String> = withContext(Dispatchers.IO) {
        val wire = P2PWireMessage(
            type = "dm",
            content = text,
            messageID = messageId,
            senderNickname = senderNickname,
            timestamp = System.currentTimeMillis()
        )
        val res = repository.sendMessage(targetPeerId, gson.toJson(wire))
        if (res.isSuccess) Result.success(messageId) else Result.failure(res.exceptionOrNull() ?: Exception("Send failed"))
    }

    /** Stream binary file in 200 KB chunks */
    suspend fun sendDirectMedia(
        targetPeerId: String,
        fileName: String,
        contentType: String,
        fileBytes: ByteArray,
        senderNickname: String? = null,
        onProgress: ((sent: Int, total: Int) -> Unit)? = null
    ): Result<String> = withContext(Dispatchers.IO) {
        val chunkId = UUID.randomUUID().toString()
        val chunkSize = P2PConstants.DEFAULT_CHUNK_SIZE
        val totalChunks = ceil(fileBytes.size.toDouble() / chunkSize.toDouble()).toInt().coerceAtLeast(1)

        try {
            for (i in 0 until totalChunks) {
                val start = i * chunkSize
                val end = (start + chunkSize).coerceAtMost(fileBytes.size)
                val slice = fileBytes.copyOfRange(start, end)
                val base64 = Base64.encodeToString(slice, Base64.NO_WRAP)

                val wire = P2PWireMessage(
                    type = "dm",
                    content = "",
                    messageID = UUID.randomUUID().toString(),
                    senderNickname = senderNickname,
                    timestamp = System.currentTimeMillis(),
                    contentType = contentType,
                    fileName = fileName,
                    fileSize = fileBytes.size.toLong(),
                    chunkId = chunkId,
                    chunkIndex = i,
                    totalChunks = totalChunks,
                    fileData = base64
                )

                val sendRes = repository.sendMessage(targetPeerId, gson.toJson(wire))
                if (sendRes.isFailure) return@withContext Result.failure(sendRes.exceptionOrNull() ?: Exception("Chunk send failed"))
                onProgress?.invoke(i + 1, totalChunks)
            }
            Result.success(chunkId)
        } catch (e: Throwable) {
            Result.failure(e)
        }
    }

    private suspend fun handleRawIncoming(raw: P2PIncomingMessage) {
        val wire: P2PWireMessage = try {
            gson.fromJson(raw.content, P2PWireMessage::class.java)
        } catch (e: Exception) {
            // Raw text fallback
            P2PWireMessage(
                type = if (raw.type == P2PMessageType.TOPIC_MESSAGE) "topic" else "dm",
                content = raw.content,
                messageID = raw.messageId,
                senderNickname = null,
                timestamp = raw.timestamp
            )
        }

        // Check for chunked media
        if (wire.chunkId != null && wire.chunkIndex != null && wire.totalChunks != null && wire.fileData != null) {
            val chunkBytes = try {
                Base64.decode(wire.fileData, Base64.DEFAULT)
            } catch (e: Exception) {
                return
            }

            val assembled = chunkAssembler.addChunk(
                chunkId = wire.chunkId,
                chunkIndex = wire.chunkIndex,
                totalChunks = wire.totalChunks,
                contentType = wire.contentType ?: "application/octet-stream",
                fileName = wire.fileName ?: "file.bin",
                chunkData = chunkBytes
            )

            if (assembled != null) {
                _messages.emit(
                    P2PIncomingMessage(
                        senderPeerID = raw.senderPeerID,
                        content = "",
                        type = raw.type,
                        messageId = wire.chunkId,
                        topicName = raw.topicName,
                        timestamp = wire.timestamp,
                        senderNickname = wire.senderNickname,
                        fileBytes = assembled.bytes,
                        fileName = assembled.fileName,
                        contentType = assembled.contentType,
                        fileSize = assembled.bytes.size.toLong()
                    )
                )
            }
        } else {
            _messages.emit(
                P2PIncomingMessage(
                    senderPeerID = raw.senderPeerID,
                    content = wire.content,
                    type = raw.type,
                    messageId = wire.messageID,
                    topicName = raw.topicName,
                    timestamp = wire.timestamp,
                    senderNickname = wire.senderNickname
                )
            )
        }
    }
}
