package io.libp2p.android

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
import java.security.MessageDigest
import java.security.SecureRandom
import java.util.Collections
import java.util.LinkedHashMap
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec
import kotlin.math.ceil
import kotlin.math.min
import kotlin.math.pow
import kotlin.random.Random

// ============================================================================
// 1. PUBLIC DATA MODELS & ENUMS
// ============================================================================

/** Node lifecycle status */
enum class Libp2pNodeStatus {
    STOPPED,
    STARTING,
    RUNNING,
    ERROR
}

/** Delivery / Read Receipt Status */
enum class Libp2pReceiptStatus {
    DELIVERED,
    READ
}

/** Delivery receipt notification */
data class Libp2pDeliveryReceipt(
    val originalMessageId: String,
    val senderPeerId: String,
    val status: Libp2pReceiptStatus,
    val timestamp: Long = System.currentTimeMillis()
)

/** Real-time typing event */
data class Libp2pTypingEvent(
    val peerId: String,
    val isTyping: Boolean,
    val topicName: String? = null,
    val timestamp: Long = System.currentTimeMillis()
)

/** Live peer presence and latency round-trip telemetry */
data class Libp2pPeerPresence(
    val peerId: String,
    val isOnline: Boolean = true,
    val rttLatencyMs: Long? = null,
    val multiaddresses: List<String> = emptyList(),
    val lastSeen: Long = System.currentTimeMillis()
)

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
    val lastSeen: Long = System.currentTimeMillis(),
    val latencyMs: Long? = null
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
    // Media fields
    val fileBytes: ByteArray? = null,
    val fileName: String? = null,
    val contentType: String? = null,
    val fileSize: Long? = null,
    val isEncrypted: Boolean = false,
    val checksumSha256: String? = null
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
    val bandwidthLimitBytesPerSec: Long = 0L, // 0 = unlimited
    val autoReconnect: Boolean = true
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
// 2. PRODUCTION MESSAGE DEDUPLICATION ENGINE (LRU + TTL)
// ============================================================================

/** Thread-safe LRU message deduplicator with sliding-window TTL */
class Libp2pDeduplicator(
    private val maxEntries: Int = 5_000,
    private val ttlMs: Long = 10 * 60 * 1000L // 10 minutes
) {
    private val cache: MutableMap<String, Long> = Collections.synchronizedMap(
        object : LinkedHashMap<String, Long>(maxEntries, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, Long>?): Boolean {
                return size > maxEntries
            }
        }
    )

    fun isNew(messageId: String): Boolean {
        if (messageId.isBlank()) return true
        val now = System.currentTimeMillis()
        synchronized(cache) {
            val previousTime = cache[messageId]
            if (previousTime != null && (now - previousTime) < ttlMs) {
                return false
            }
            cache[messageId] = now
            return true
        }
    }

    fun clear() {
        cache.clear()
    }
}

// ============================================================================
// 3. EXPONENTIAL BACKOFF DIALER
// ============================================================================

/** Exponential backoff retry manager with randomized jitter */
class Libp2pBackoffDialer(
    private val initialDelayMs: Long = 1_000L,
    private val maxDelayMs: Long = 30_000L,
    private val multiplier: Double = 1.5,
    private val jitterRatio: Double = 0.2
) {
    private val attemptCounters = ConcurrentHashMap<String, Int>()

    fun getNextDelayMs(peerId: String): Long {
        val attempt = attemptCounters.getOrDefault(peerId, 0)
        attemptCounters[peerId] = attempt + 1

        val calculated = initialDelayMs * multiplier.pow(attempt.toDouble())
        val capped = min(calculated.toLong(), maxDelayMs)

        val jitter = (capped * jitterRatio * Random.nextDouble(-1.0, 1.0)).toLong()
        return (capped + jitter).coerceAtLeast(initialDelayMs)
    }

    fun reset(peerId: String) {
        attemptCounters.remove(peerId)
    }
}

// ============================================================================
// 4. END-TO-END ENCRYPTION (E2EE) SECURITY UTILITY
// ============================================================================

/** Built-in AES-256-GCM cryptographic utility for optional End-to-End Encryption */
object Libp2pCrypto {
    private const val ALGORITHM = "AES/GCM/NoPadding"
    private const val TAG_LENGTH_BIT = 128
    private const val IV_LENGTH_BYTE = 12

    fun sha256(data: ByteArray): String {
        val digest = MessageDigest.getInstance("SHA-256").digest(data)
        return digest.joinToString("") { "%02x".format(it) }
    }

    fun generateKey(): String {
        val keyGen = KeyGenerator.getInstance("AES")
        keyGen.init(256, SecureRandom())
        val secretKey = keyGen.generateKey()
        return Base64.encodeToString(secretKey.encoded, Base64.NO_WRAP)
    }

    fun encrypt(plainText: String, keyBase64: String): String {
        return encryptBytes(plainText.toByteArray(Charsets.UTF_8), keyBase64)
    }

    fun encryptBytes(data: ByteArray, keyBase64: String): String {
        val keyBytes = Base64.decode(keyBase64, Base64.NO_WRAP)
        val secretKey: SecretKey = SecretKeySpec(keyBytes, "AES")

        val iv = ByteArray(IV_LENGTH_BYTE)
        SecureRandom().nextBytes(iv)

        val cipher = Cipher.getInstance(ALGORITHM)
        val spec = GCMParameterSpec(TAG_LENGTH_BIT, iv)
        cipher.init(Cipher.ENCRYPT_MODE, secretKey, spec)

        val cipherText = cipher.doFinal(data)
        val combined = ByteArray(iv.size + cipherText.size)
        System.arraycopy(iv, 0, combined, 0, iv.size)
        System.arraycopy(cipherText, 0, combined, iv.size, cipherText.size)

        return Base64.encodeToString(combined, Base64.NO_WRAP)
    }

    fun decrypt(cipherTextBase64: String, keyBase64: String): String {
        val decryptedBytes = decryptBytes(cipherTextBase64, keyBase64)
        return String(decryptedBytes, Charsets.UTF_8)
    }

    fun decryptBytes(cipherTextBase64: String, keyBase64: String): ByteArray {
        val keyBytes = Base64.decode(keyBase64, Base64.NO_WRAP)
        val secretKey: SecretKey = SecretKeySpec(keyBytes, "AES")

        val combined = Base64.decode(cipherTextBase64, Base64.NO_WRAP)
        if (combined.size < IV_LENGTH_BYTE) throw IllegalArgumentException("Invalid ciphertext payload")

        val iv = ByteArray(IV_LENGTH_BYTE)
        val cipherText = ByteArray(combined.size - IV_LENGTH_BYTE)
        System.arraycopy(combined, 0, iv, 0, IV_LENGTH_BYTE)
        System.arraycopy(combined, IV_LENGTH_BYTE, cipherText, 0, cipherText.size)

        val cipher = Cipher.getInstance(ALGORITHM)
        val spec = GCMParameterSpec(TAG_LENGTH_BIT, iv)
        cipher.init(Cipher.DECRYPT_MODE, secretKey, spec)

        return cipher.doFinal(cipherText)
    }
}

// ============================================================================
// 5. MAIN CLIENT ENTRY POINT (Libp2pClient)
// ============================================================================

/**
 * Enterprise-grade, production-ready libp2p client for Android applications.
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
        private const val PING_TIMEOUT_MS = 10_000L

        const val POLL_INTERVAL_NORMAL_MS = 15_000L
        const val POLL_INTERVAL_IDLE_MS = 45_000L

        @Volatile
        private var defaultInstance: Libp2pClient? = null

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

    private val deduplicator = Libp2pDeduplicator()
    private val backoffDialer = Libp2pBackoffDialer()
    private val chunkAssembler = Libp2pChunkAssembler()
    private val activeTransfers = ConcurrentHashMap.newKeySet<String>()

    private val prefs by lazy {
        context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
    }

    // Reactive Observables
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

    private val _deliveryReceipts = MutableSharedFlow<Libp2pDeliveryReceipt>(extraBufferCapacity = 64)
    val deliveryReceipts: SharedFlow<Libp2pDeliveryReceipt> = _deliveryReceipts.asSharedFlow()

    private val _typingEvents = MutableSharedFlow<Libp2pTypingEvent>(extraBufferCapacity = 64)
    val typingEvents: SharedFlow<Libp2pTypingEvent> = _typingEvents.asSharedFlow()

    private val _peerPresence = MutableStateFlow<Map<String, Libp2pPeerPresence>>(emptyMap())
    val peerPresence: StateFlow<Map<String, Libp2pPeerPresence>> = _peerPresence.asStateFlow()

    private val subscribedTopics = ConcurrentHashMap<String, MutableSharedFlow<Libp2pMessage>>()
    private val peerAddressMap = ConcurrentHashMap<String, MutableSet<String>>()
    private val pendingPings = ConcurrentHashMap<String, CompletableDeferred<Long>>()

    init {
        prefs.getString(KEY_PEER_ID, null)?.let {
            _peerId.value = it
        }
        setupNetworkCallback()
    }

    // ========================================================================
    // LIFECYCLE: START / STOP
    // ========================================================================

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

                setupBridgeHandlers(createdNode)

                val iceJson = JSONArray(config.iceServers).toString()
                try { createdNode.setIceServers(iceJson) } catch (e: Exception) { Log.w(TAG, "setIceServers failed", e) }

                if (config.bandwidthLimitBytesPerSec > 0) {
                    try { createdNode.setBandwidthLimit(config.bandwidthLimitBytesPerSec) } catch (e: Exception) { Log.w(TAG, "setBandwidthLimit failed", e) }
                }

                for (bootstrapAddr in config.bootstrapPeers) {
                    try { createdNode.addBootstrapPeer(bootstrapAddr) } catch (e: Exception) { Log.w(TAG, "addBootstrapPeer failed for $bootstrapAddr", e) }
                }

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
                startAdaptivePoller()

                Log.i(TAG, "Libp2pClient started successfully. PeerID: $localPeerId")
                Result.success(localPeerId)
            } catch (e: Throwable) {
                _nodeStatus.value = Libp2pNodeStatus.ERROR
                Log.e(TAG, "Failed to start Libp2pClient", e)
                Result.failure(e)
            }
        }
    }

    suspend fun stop(): Result<Unit> = nodeLock.withLock {
        withContext(Dispatchers.IO) {
            try {
                statusRefreshJob?.cancel()
                statusRefreshJob = null

                node?.stop()
                node = null

                _nodeStatus.value = Libp2pNodeStatus.STOPPED
                _connectedPeers.value = emptyList()
                _listenAddresses.value = emptyList()
                _peerPresence.value = emptyMap()
                deduplicator.clear()

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

    suspend fun sendDirectMessage(
        targetPeerId: String,
        text: String,
        senderNickname: String? = null,
        messageId: String = UUID.randomUUID().toString()
    ): Result<String> = withContext(Dispatchers.IO) {
        val activeNode = node ?: return@withContext Result.failure(IllegalStateException("Node is not running"))

        val wire = Libp2pWireMessage(
            type = "dm",
            content = text,
            messageID = messageId,
            senderNickname = senderNickname,
            timestamp = System.currentTimeMillis()
        )

        try {
            activeNode.sendMessage(targetPeerId, gson.toJson(wire))
            Result.success(messageId)
        } catch (e: Throwable) {
            Result.failure(e)
        }
    }

    suspend fun sendEncryptedDirectMessage(
        targetPeerId: String,
        text: String,
        aesKeyBase64: String,
        senderNickname: String? = null,
        messageId: String = UUID.randomUUID().toString()
    ): Result<String> = withContext(Dispatchers.IO) {
        val activeNode = node ?: return@withContext Result.failure(IllegalStateException("Node is not running"))

        val cipherText = Libp2pCrypto.encrypt(text, aesKeyBase64)
        val wire = Libp2pWireMessage(
            type = "dm",
            content = cipherText,
            messageID = messageId,
            senderNickname = senderNickname,
            timestamp = System.currentTimeMillis(),
            isEncrypted = true
        )

        try {
            activeNode.sendMessage(targetPeerId, gson.toJson(wire))
            Result.success(messageId)
        } catch (e: Throwable) {
            Result.failure(e)
        }
    }

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
        val checksumSha256 = Libp2pCrypto.sha256(fileBytes)

        activeTransfers.add(chunkId)

        try {
            for (index in 0 until totalChunks) {
                if (!activeTransfers.contains(chunkId)) {
                    return@withContext Result.failure(Exception("Transfer $chunkId cancelled"))
                }

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
                    fileData = base64Data,
                    checksumSha256 = checksumSha256
                )

                activeNode.sendMessage(targetPeerId, gson.toJson(wireChunk))
                onProgress?.invoke(index + 1, totalChunks)
            }

            activeTransfers.remove(chunkId)
            Result.success(chunkId)
        } catch (e: Throwable) {
            activeTransfers.remove(chunkId)
            Result.failure(e)
        }
    }

    fun cancelMediaTransfer(chunkId: String) {
        activeTransfers.remove(chunkId)
        chunkAssembler.cancelTransfer(chunkId)
    }

    suspend fun sendTyping(targetPeerId: String, isTyping: Boolean, topicName: String? = null): Result<Unit> = withContext(Dispatchers.IO) {
        val activeNode = node ?: return@withContext Result.failure(IllegalStateException("Node not running"))
        val wire = Libp2pWireMessage(
            type = "typing",
            content = "",
            messageID = UUID.randomUUID().toString(),
            isTyping = isTyping
        )
        try {
            if (topicName != null) {
                activeNode.publishToTopic(topicName, gson.toJson(wire))
            } else {
                activeNode.sendMessage(targetPeerId, gson.toJson(wire))
            }
            Result.success(Unit)
        } catch (e: Throwable) {
            Result.failure(e)
        }
    }

    suspend fun sendReadReceipt(targetPeerId: String, originalMessageId: String): Result<Unit> = withContext(Dispatchers.IO) {
        val activeNode = node ?: return@withContext Result.failure(IllegalStateException("Node not running"))
        val wire = Libp2pWireMessage(
            type = "ack",
            content = "",
            messageID = UUID.randomUUID().toString(),
            ackMessageId = originalMessageId,
            ackStatus = "read"
        )
        try {
            activeNode.sendMessage(targetPeerId, gson.toJson(wire))
            Result.success(Unit)
        } catch (e: Throwable) {
            Result.failure(e)
        }
    }

    suspend fun pingPeer(targetPeerId: String): Result<Long> = withContext(Dispatchers.IO) {
        val activeNode = node ?: return@withContext Result.failure(IllegalStateException("Node not running"))
        val pingId = UUID.randomUUID().toString()
        val deferred = CompletableDeferred<Long>()
        pendingPings[pingId] = deferred

        val wire = Libp2pWireMessage(
            type = "ping",
            content = "",
            messageID = pingId,
            pingTimestamp = System.currentTimeMillis()
        )

        try {
            activeNode.sendMessage(targetPeerId, gson.toJson(wire))
            withTimeout(PING_TIMEOUT_MS) {
                val rtt = deferred.await()
                updatePeerLatency(targetPeerId, rtt)
                Result.success(rtt)
            }
        } catch (e: Exception) {
            pendingPings.remove(pingId)
            Result.failure(e)
        }
    }

    private fun updatePeerLatency(peerId: String, rtt: Long) {
        val current = _peerPresence.value.toMutableMap()
        val existing = current[peerId] ?: Libp2pPeerPresence(peerId = peerId)
        current[peerId] = existing.copy(rttLatencyMs = rtt, lastSeen = System.currentTimeMillis())
        _peerPresence.value = current
    }

    // ========================================================================
    // TOPIC PUB/SUB (GOSSIPSUB MULTICAST)
    // ========================================================================

    fun subscribeTopic(topicName: String): Flow<Libp2pMessage> {
        val topicFlow = subscribedTopics.getOrPut(topicName) {
            MutableSharedFlow(extraBufferCapacity = 64)
        }

        scope.launch {
            nodeLock.withLock {
                try {
                    node?.subscribeToTopic(topicName)
                } catch (e: Exception) {
                    Log.e(TAG, "Failed to subscribe to topic: $topicName", e)
                }
            }
        }

        return topicFlow.asSharedFlow()
    }

    suspend fun unsubscribeTopic(topicName: String): Result<Unit> = withContext(Dispatchers.IO) {
        nodeLock.withLock {
            try {
                node?.unsubscribeFromTopic(topicName)
                subscribedTopics.remove(topicName)
                Result.success(Unit)
            } catch (e: Throwable) {
                Result.failure(e)
            }
        }
    }

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
            activeNode.publishToTopic(topicName, gson.toJson(wire))
            Result.success(Unit)
        } catch (e: Throwable) {
            Result.failure(e)
        }
    }

    // ========================================================================
    // PEER MANAGEMENT & NETWORKING
    // ========================================================================

    suspend fun connectPeer(multiaddr: String): Result<Unit> = withContext(Dispatchers.IO) {
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

    suspend fun addBootstrapPeer(multiaddr: String): Result<Unit> = withContext(Dispatchers.IO) {
        val activeNode = node ?: return@withContext Result.failure(IllegalStateException("Node not running"))
        try {
            activeNode.addBootstrapPeer(multiaddr)
            Result.success(Unit)
        } catch (e: Throwable) {
            Result.failure(e)
        }
    }

    fun getPeerMultiaddresses(peerId: String): List<String> {
        return peerAddressMap[peerId]?.toList() ?: emptyList()
    }

    // ========================================================================
    // INTERNAL BRIDGE HANDLERS & MESSAGE PROCESSING
    // ========================================================================

    private fun setupBridgeHandlers(node: P2PNode) {
        node.setMessageHandler(object : MobileMessageHandler {
            override fun handleMessage(senderPeerID: String, content: String) {
                scope.launch {
                    processIncomingPayload(senderPeerID, content, isTopic = false, topicName = null)
                }
            }
        })

        node.setTopicMessageHandler(object : MobileTopicMessageHandler {
            override fun handleTopicMessage(topicName: String, senderPeerID: String, content: String) {
                scope.launch {
                    processIncomingPayload(senderPeerID, content, isTopic = true, topicName = topicName)
                }
            }
        })

        node.setConnectionHandler(object : MobileConnectionHandler {
            override fun handlePeerConnected(peerID: String, remoteAddr: String) {
                scope.launch {
                    val addrs = peerAddressMap.getOrPut(peerID) { ConcurrentHashMap.newKeySet() }
                    if (remoteAddr.isNotBlank()) addrs.add(remoteAddr)
                    backoffDialer.reset(peerID)
                    _peerEvents.emit(Libp2pPeerEvent(peerId = peerID, isConnected = true, multiaddress = remoteAddr))

                    val current = _peerPresence.value.toMutableMap()
                    current[peerID] = Libp2pPeerPresence(
                        peerId = peerID,
                        isOnline = true,
                        multiaddresses = addrs.toList(),
                        lastSeen = System.currentTimeMillis()
                    )
                    _peerPresence.value = current

                    refreshNodeState()
                }
            }

            override fun handlePeerDisconnected(peerID: String) {
                scope.launch {
                    _peerEvents.emit(Libp2pPeerEvent(peerId = peerID, isConnected = false))

                    val current = _peerPresence.value.toMutableMap()
                    current[peerID]?.let {
                        current[peerID] = it.copy(isOnline = false, lastSeen = System.currentTimeMillis())
                        _peerPresence.value = current
                    }

                    refreshNodeState()
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

    private suspend fun processIncomingPayload(
        senderPeerId: String,
        rawPayload: String,
        isTopic: Boolean,
        topicName: String?
    ) {
        val wire: Libp2pWireMessage = try {
            gson.fromJson(rawPayload, Libp2pWireMessage::class.java)
        } catch (e: Exception) {
            Libp2pWireMessage(
                type = if (isTopic) "topic" else "dm",
                content = rawPayload,
                messageID = UUID.randomUUID().toString(),
                senderNickname = null
            )
        }

        // Deduplication check
        if (!deduplicator.isNew(wire.messageID)) {
            return
        }

        when (wire.type) {
            "ack" -> {
                val ackId = wire.ackMessageId ?: return
                val status = if (wire.ackStatus == "read") Libp2pReceiptStatus.READ else Libp2pReceiptStatus.DELIVERED
                _deliveryReceipts.emit(Libp2pDeliveryReceipt(originalMessageId = ackId, senderPeerId = senderPeerId, status = status))
            }
            "typing" -> {
                val isTyping = wire.isTyping ?: false
                _typingEvents.emit(Libp2pTypingEvent(peerId = senderPeerId, isTyping = isTyping, topicName = topicName))
            }
            "ping" -> {
                val pong = Libp2pWireMessage(
                    type = "pong",
                    content = "",
                    messageID = wire.messageID,
                    pingTimestamp = wire.pingTimestamp
                )
                try { node?.sendMessage(senderPeerId, gson.toJson(pong)) } catch (e: Exception) { /* ignore */ }
            }
            "pong" -> {
                val pingId = wire.messageID
                val deferred = pendingPings.remove(pingId)
                if (deferred != null && wire.pingTimestamp != null) {
                    val rtt = System.currentTimeMillis() - wire.pingTimestamp
                    deferred.complete(rtt)
                }
            }
            else -> {
                // Auto-reply with delivery ACK for direct messages
                if (!isTopic) {
                    val ack = Libp2pWireMessage(
                        type = "ack",
                        content = "",
                        messageID = UUID.randomUUID().toString(),
                        ackMessageId = wire.messageID,
                        ackStatus = "delivered"
                    )
                    try { node?.sendMessage(senderPeerId, gson.toJson(ack)) } catch (e: Exception) { /* ignore */ }
                }

                // Check if this is a media chunk
                if (wire.chunkId != null && wire.chunkIndex != null &&
                    wire.totalChunks != null && wire.fileData != null) {

                    val rawChunkBytes = try {
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
                        chunkData = rawChunkBytes,
                        expectedChecksumSha256 = wire.checksumSha256
                    )

                    if (assembled != null) {
                        val fullMessage = Libp2pMessage(
                            senderPeerId = senderPeerId,
                            content = "",
                            messageId = wire.chunkId,
                            senderNickname = wire.senderNickname,
                            timestamp = wire.timestamp,
                            isTopicMessage = isTopic,
                            topicName = topicName,
                            fileBytes = assembled.bytes,
                            fileName = assembled.fileName,
                            contentType = assembled.contentType,
                            fileSize = assembled.bytes.size.toLong(),
                            checksumSha256 = wire.checksumSha256
                        )
                        dispatchMessage(fullMessage)
                    }
                } else {
                    val textMessage = Libp2pMessage(
                        senderPeerId = senderPeerId,
                        content = wire.content,
                        messageId = wire.messageID,
                        senderNickname = wire.senderNickname,
                        timestamp = wire.timestamp,
                        isTopicMessage = isTopic,
                        topicName = topicName,
                        isEncrypted = wire.isEncrypted ?: false
                    )
                    dispatchMessage(textMessage)
                }
            }
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
            val addrsJson = activeNode.listenAddresses
            if (addrsJson.isNotBlank() && addrsJson != "[]") {
                val arr = JSONArray(addrsJson)
                _listenAddresses.value = (0 until arr.length()).map { arr.getString(it) }
            }

            val peersJson = activeNode.connectedPeers
            if (peersJson.isNotBlank() && peersJson != "[]") {
                val arr = JSONArray(peersJson)
                _connectedPeers.value = (0 until arr.length()).map { arr.getString(it) }
            } else {
                _connectedPeers.value = emptyList()
            }
        } catch (e: Exception) {
            Log.w(TAG, "State refresh error", e)
        }
    }

    private fun startAdaptivePoller() {
        statusRefreshJob?.cancel()
        statusRefreshJob = scope.launch {
            while (isActive) {
                val pollDelay = if (_connectedPeers.value.isNotEmpty()) {
                    POLL_INTERVAL_NORMAL_MS
                } else {
                    POLL_INTERVAL_IDLE_MS
                }
                delay(pollDelay)
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
                _bandwidthStats.value = Libp2pBandwidthStats(
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
            }
        } catch (e: Exception) {
            // ignore
        }
    }

    private fun setupNetworkCallback() {
        val cm = context.getSystemService(Context.CONNECTIVITY_SERVICE) as? ConnectivityManager ?: return
        cm.registerDefaultNetworkCallback(object : ConnectivityManager.NetworkCallback() {
            override fun onAvailable(network: Network) {
                scope.launch {
                    if (_nodeStatus.value == Libp2pNodeStatus.RUNNING) {
                        refreshNodeState()
                    }
                }
            }
            override fun onLost(network: Network) {
                scope.launch {
                    refreshNodeState()
                }
            }
        })
    }
}

// ============================================================================
// 6. INTERNAL WIRE MESSAGING & CHUNK ASSEMBLER
// ============================================================================

/** Internal JSON Wire Framing Format */
data class Libp2pWireMessage(
    val type: String,                       // "dm", "topic", "ack", "typing", "ping", "pong"
    val content: String,                    // Text content
    val messageID: String,                  // Unique message UUID
    val senderNickname: String? = null,     // Display alias
    val timestamp: Long = System.currentTimeMillis(),
    val contentType: String? = null,
    val fileName: String? = null,
    val fileSize: Long? = null,
    val chunkId: String? = null,            // Multi-chunk transfer UUID
    val chunkIndex: Int? = null,            // 0-based chunk index
    val totalChunks: Int? = null,           // Total chunk count
    val fileData: String? = null,           // Base64 chunk bytes
    val checksumSha256: String? = null,     // SHA-256 digest
    val ackMessageId: String? = null,       // Delivery receipt reference
    val ackStatus: String? = null,          // "delivered", "read"
    val isTyping: Boolean? = null,          // Typing indicator state
    val pingTimestamp: Long? = null,        // Latency epoch ms
    val isEncrypted: Boolean? = null        // E2EE payload flag
)

/** Reassembles 200 KB multi-chunk binary media transfers with SHA-256 integrity verification */
class Libp2pChunkAssembler(
    private val timeoutMs: Long = 30_000L,
    private val cleanupIntervalMs: Long = 10_000L
) {
    companion object {
        private const val TAG = "Libp2pChunkAssembler"
    }

    data class AssembledMedia(
        val bytes: ByteArray,
        val contentType: String,
        val fileName: String,
        val checksumValid: Boolean = true
    )

    private data class ChunkState(
        val chunks: MutableMap<Int, ByteArray> = mutableMapOf(),
        val contentType: String,
        val fileName: String,
        val totalChunks: Int,
        val expectedChecksum: String? = null,
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
        chunkData: ByteArray,
        expectedChecksumSha256: String? = null
    ): AssembledMedia? {
        val state = pending.getOrPut(chunkId) {
            ChunkState(
                contentType = contentType,
                fileName = fileName,
                totalChunks = totalChunks,
                expectedChecksum = expectedChecksumSha256
            )
        }

        synchronized(state.chunks) {
            state.chunks[chunkIndex] = chunkData
            if (state.chunks.size == totalChunks) {
                var totalBytesCount = 0
                for (i in 0 until totalChunks) {
                    val piece = state.chunks[i] ?: return null
                    totalBytesCount += piece.size
                }

                val merged = ByteArray(totalBytesCount)
                var writeOffset = 0
                for (i in 0 until totalChunks) {
                    val piece = state.chunks[i]!!
                    System.arraycopy(piece, 0, merged, writeOffset, piece.size)
                    writeOffset += piece.size
                }

                pending.remove(chunkId)

                val checksumValid = if (!state.expectedChecksum.isNullOrBlank()) {
                    val actualSha = Libp2pCrypto.sha256(merged)
                    actualSha.equals(state.expectedChecksum, ignoreCase = true)
                } else true

                Log.d(TAG, "Successfully reassembled $chunkId ($fileName, ${merged.size} bytes, SHA-256: $checksumValid)")
                return AssembledMedia(merged, state.contentType, state.fileName, checksumValid)
            }
        }
        return null
    }

    fun cancelTransfer(chunkId: String): Boolean = pending.remove(chunkId) != null
    fun isTransferActive(chunkId: String): Boolean = pending.containsKey(chunkId)

    private fun cleanup() {
        val cutoff = System.currentTimeMillis() - timeoutMs
        val expired = pending.entries.filter { it.value.timestamp < cutoff }.map { it.key }
        expired.forEach { pending.remove(it) }
    }

    fun shutdown() {
        job.cancel()
    }
}
