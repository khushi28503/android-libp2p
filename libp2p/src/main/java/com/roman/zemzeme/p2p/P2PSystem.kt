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
// 1. P2P DATA MODELS, CONSTANTS, ENUMS & PROTOCOLS
// ============================================================================

/** Node lifecycle status */
enum class P2PNodeStatus {
    STOPPED,
    STARTING,
    RUNNING,
    ERROR
}

/** P2P message classification */
enum class P2PMessageType {
    DIRECT_MESSAGE,
    TOPIC_MESSAGE,
    CHANNEL_MESSAGE,
    DELIVERY_RECEIPT,
    TYPING_INDICATOR,
    PING_PONG
}

/** Delivery / Read Receipt Status */
enum class P2PReceiptStatus {
    DELIVERED,
    READ
}

/** Delivery receipt notification */
data class P2PDeliveryReceipt(
    val originalMessageId: String,
    val senderPeerId: String,
    val status: P2PReceiptStatus,
    val timestamp: Long = System.currentTimeMillis()
)

/** Real-time typing event */
data class P2PTypingEvent(
    val peerId: String,
    val isTyping: Boolean,
    val topicName: String? = null,
    val timestamp: Long = System.currentTimeMillis()
)

/** Live peer presence and latency round-trip telemetry */
data class P2PPeerPresence(
    val peerId: String,
    val isOnline: Boolean = true,
    val rttLatencyMs: Long? = null,
    val multiaddresses: List<String> = emptyList(),
    val lastSeen: Long = System.currentTimeMillis()
)

/** Peer connection lifecycle event */
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
    val lastSeen: Long = System.currentTimeMillis(),
    val latencyMs: Long? = null
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
    val fileSize: Long? = null,
    val isEncrypted: Boolean = false,
    val checksumSha256: String? = null
) {
    val isMediaMessage: Boolean get() = fileBytes != null
}

/** Internal P2P JSON wire message framing */
data class P2PWireMessage(
    val type: String,                       // "dm", "topic", "ack", "typing", "ping", "pong"
    val content: String,                    // Plaintext content or payload
    val messageID: String,                  // Unique message UUID
    val senderNickname: String? = null,     // Sender display alias
    val timestamp: Long = System.currentTimeMillis(),
    // Media streaming fields
    val contentType: String? = null,
    val fileName: String? = null,
    val fileSize: Long? = null,
    val chunkId: String? = null,            // Chunk transfer UUID
    val chunkIndex: Int? = null,            // 0-based chunk index
    val totalChunks: Int? = null,           // Total chunk count
    val fileData: String? = null,           // Base64 chunk bytes
    val checksumSha256: String? = null,     // SHA-256 digest of complete file
    // Feature extension fields (ACK, Typing, Ping, Encryption)
    val ackMessageId: String? = null,       // Referenced message ID for delivery receipt
    val ackStatus: String? = null,          // "delivered", "read"
    val isTyping: Boolean? = null,          // Typing indicator state
    val pingTimestamp: Long? = null,        // Latency measurement epoch ms
    val isEncrypted: Boolean? = null        // E2EE payload flag
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
    val messageID: String = UUID.randomUUID().toString(),
    val senderNickname: String? = null
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
    const val PING_TIMEOUT_MS = 10_000L

    // Battery-adaptive polling rates (ms)
    const val POLL_INTERVAL_ACTIVE_MEDIA_MS = 3_000L
    const val POLL_INTERVAL_NORMAL_MS = 15_000L
    const val POLL_INTERVAL_IDLE_MS = 45_000L

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
// 2. PRODUCTION MESSAGE DEDUPLICATION ENGINE (LRU + TTL)
// ============================================================================

/**
 * Thread-safe LRU message deduplicator with sliding-window TTL.
 * Prevents GossipSub echo loops and duplicate network retransmissions from triggering UI duplicate events.
 */
class P2PDeduplicator(
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

    /**
     * Check if a message is seen for the first time.
     * @return true if message is NEW (not seen before), false if duplicate.
     */
    fun isNew(messageId: String): Boolean {
        if (messageId.isBlank()) return true
        val now = System.currentTimeMillis()
        synchronized(cache) {
            val previousTime = cache[messageId]
            if (previousTime != null && (now - previousTime) < ttlMs) {
                return false // Duplicate
            }
            cache[messageId] = now
            return true // New message
        }
    }

    /** Clear deduplication history */
    fun clear() {
        cache.clear()
    }
}

// ============================================================================
// 3. END-TO-END ENCRYPTION (E2EE) SECURITY UTILITY
// ============================================================================

/**
 * Built-in AES-256-GCM cryptographic utility for optional End-to-End Encryption.
 */
object P2PCrypto {
    private const val ALGORITHM = "AES/GCM/NoPadding"
    private const val TAG_LENGTH_BIT = 128
    private const val IV_LENGTH_BYTE = 12

    /** Compute SHA-256 hex digest of byte array */
    fun sha256(data: ByteArray): String {
        val digest = MessageDigest.getInstance("SHA-256").digest(data)
        return digest.joinToString("") { "%02x".format(it) }
    }

    /** Generate a secure random 256-bit AES key (Base64 encoded) */
    fun generateKey(): String {
        val keyGen = KeyGenerator.getInstance("AES")
        keyGen.init(256, SecureRandom())
        val secretKey = keyGen.generateKey()
        return Base64.encodeToString(secretKey.encoded, Base64.NO_WRAP)
    }

    /** Encrypt plaintext string using AES-256-GCM */
    fun encrypt(plainText: String, keyBase64: String): String {
        return encryptBytes(plainText.toByteArray(Charsets.UTF_8), keyBase64)
    }

    /** Encrypt raw bytes using AES-256-GCM. Returns Base64 payload (IV + Ciphertext + Tag). */
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

    /** Decrypt Base64 payload string using AES-256-GCM */
    fun decrypt(cipherTextBase64: String, keyBase64: String): String {
        val decryptedBytes = decryptBytes(cipherTextBase64, keyBase64)
        return String(decryptedBytes, Charsets.UTF_8)
    }

    /** Decrypt Base64 payload back to raw bytes */
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
// 4. CHUNK ASSEMBLER (200 KB BINARY SLICES WITH SHA-256 INTEGRITY)
// ============================================================================

/**
 * Reassembles multi-chunk P2P binary media transfers with SHA-256 integrity verification.
 */
class P2PChunkAssembler(
    private val timeoutMs: Long = P2PConstants.CHUNK_TIMEOUT_MS,
    private val cleanupIntervalMs: Long = 10_000L
) {
    companion object {
        private const val TAG = "P2PChunkAssembler"
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
                // Pre-size output buffer to avoid intermediate array copies
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

                // Verify SHA-256 integrity if checksum provided
                val checksumValid = if (!state.expectedChecksum.isNullOrBlank()) {
                    val actualSha = P2PCrypto.sha256(merged)
                    actualSha.equals(state.expectedChecksum, ignoreCase = true)
                } else true

                if (!checksumValid) {
                    Log.w(TAG, "SHA-256 checksum mismatch on transfer $chunkId ($fileName)")
                } else {
                    Log.d(TAG, "Reassembled $chunkId ($fileName, ${merged.size} bytes, SHA-256 verified)")
                }

                return AssembledMedia(merged, state.contentType, state.fileName, checksumValid)
            }
        }
        return null
    }

    fun cancelTransfer(chunkId: String): Boolean = pending.remove(chunkId) != null
    fun isTransferActive(chunkId: String): Boolean = pending.containsKey(chunkId)
    fun getActiveTransferCount(): Int = pending.size

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
// 5. EXPONENTIAL BACKOFF & JITTER DIALER
// ============================================================================

/**
 * Intelligent exponential backoff retry manager with randomized jitter.
 */
class P2PBackoffDialer(
    private val initialDelayMs: Long = 1_000L,
    private val maxDelayMs: Long = 30_000L,
    private val multiplier: Double = 1.5,
    private val jitterRatio: Double = 0.2
) {
    private val attemptCounters = ConcurrentHashMap<String, Int>()

    /** Calculate next delay in ms for a peer */
    fun getNextDelayMs(peerId: String): Long {
        val attempt = attemptCounters.getOrDefault(peerId, 0)
        attemptCounters[peerId] = attempt + 1

        val calculated = initialDelayMs * multiplier.pow(attempt.toDouble())
        val capped = min(calculated.toLong(), maxDelayMs)

        // Apply randomized jitter
        val jitter = (capped * jitterRatio * Random.nextDouble(-1.0, 1.0)).toLong()
        return (capped + jitter).coerceAtLeast(initialDelayMs)
    }

    /** Reset attempt count upon successful connection */
    fun reset(peerId: String) {
        attemptCounters.remove(peerId)
    }
}

// ============================================================================
// 6. P2P CONFIGURATION MANAGER
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
        private const val KEY_AUTO_RECONNECT = "auto_reconnect"
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

    fun isAutoReconnectEnabled(): Boolean = prefs.getBoolean(KEY_AUTO_RECONNECT, true)
    fun setAutoReconnectEnabled(enabled: Boolean) = prefs.edit().putBoolean(KEY_AUTO_RECONNECT, enabled).apply()
}

// ============================================================================
// 7. P2P ALIAS & FAVORITES REGISTRIES
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

    fun getFavorites(): Set<String> = favorites.toSet()
}

// ============================================================================
// 8. CORE P2P LIBRARY REPOSITORY (BATTERY-AWARE & RESILIENT)
// ============================================================================

/**
 * Core P2P engine managing Go libp2p bridge, battery-adaptive polling,
 * deduplication, and automatic reconnection.
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

    private val deduplicator = P2PDeduplicator()
    private val backoffDialer = P2PBackoffDialer()

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

    // Enhanced Feature Flows
    private val _deliveryReceipts = MutableSharedFlow<P2PDeliveryReceipt>(extraBufferCapacity = 64)
    val deliveryReceipts: SharedFlow<P2PDeliveryReceipt> = _deliveryReceipts.asSharedFlow()

    private val _typingEvents = MutableSharedFlow<P2PTypingEvent>(extraBufferCapacity = 64)
    val typingEvents: SharedFlow<P2PTypingEvent> = _typingEvents.asSharedFlow()

    private val _peerPresence = MutableStateFlow<Map<String, P2PPeerPresence>>(emptyMap())
    val peerPresence: StateFlow<Map<String, P2PPeerPresence>> = _peerPresence.asStateFlow()

    private val peerAddressMap = ConcurrentHashMap<String, MutableSet<String>>()
    private val pendingPings = ConcurrentHashMap<String, CompletableDeferred<Long>>()

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

                val iceJson = JSONArray(P2PConstants.DEFAULT_ICE_SERVERS).toString()
                try { createdNode.setIceServers(iceJson) } catch (e: Exception) { Log.w(TAG, "setIceServers failed", e) }

                val limit = p2pConfig.getBandwidthLimit()
                if (limit > 0) {
                    try { createdNode.setBandwidthLimit(limit) } catch (e: Exception) { Log.w(TAG, "setBandwidthLimit failed", e) }
                }

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
                startAdaptivePoller()

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
                _peerPresence.value = emptyMap()
                deduplicator.clear()

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

    suspend fun sendDeliveryReceipt(
        targetPeerId: String,
        originalMessageId: String,
        status: P2PReceiptStatus = P2PReceiptStatus.DELIVERED
    ): Result<Unit> = withContext(Dispatchers.IO) {
        val wire = P2PWireMessage(
            type = "ack",
            content = "",
            messageID = UUID.randomUUID().toString(),
            ackMessageId = originalMessageId,
            ackStatus = if (status == P2PReceiptStatus.DELIVERED) "delivered" else "read"
        )
        sendMessage(targetPeerId, gson.toJson(wire))
    }

    suspend fun sendTypingIndicator(
        targetPeerId: String,
        isTyping: Boolean,
        topicName: String? = null
    ): Result<Unit> = withContext(Dispatchers.IO) {
        val wire = P2PWireMessage(
            type = "typing",
            content = "",
            messageID = UUID.randomUUID().toString(),
            isTyping = isTyping
        )
        if (topicName != null) {
            publishToTopic(topicName, gson.toJson(wire))
        } else {
            sendMessage(targetPeerId, gson.toJson(wire))
        }
    }

    suspend fun pingPeer(targetPeerId: String): Result<Long> = withContext(Dispatchers.IO) {
        val pingId = UUID.randomUUID().toString()
        val deferred = CompletableDeferred<Long>()
        pendingPings[pingId] = deferred

        val wire = P2PWireMessage(
            type = "ping",
            content = "",
            messageID = pingId,
            pingTimestamp = System.currentTimeMillis()
        )

        val sendRes = sendMessage(targetPeerId, gson.toJson(wire))
        if (sendRes.isFailure) {
            pendingPings.remove(pingId)
            return@withContext Result.failure(sendRes.exceptionOrNull() ?: Exception("Ping send failed"))
        }

        try {
            withTimeout(P2PConstants.PING_TIMEOUT_MS) {
                val rtt = deferred.await()
                updatePeerLatency(targetPeerId, rtt)
                Result.success(rtt)
            }
        } catch (e: TimeoutCancellationException) {
            pendingPings.remove(pingId)
            Result.failure(Exception("Ping timed out to $targetPeerId"))
        }
    }

    private fun updatePeerLatency(peerId: String, rtt: Long) {
        val current = _peerPresence.value.toMutableMap()
        val existing = current[peerId] ?: P2PPeerPresence(peerId = peerId)
        current[peerId] = existing.copy(rttLatencyMs = rtt, lastSeen = System.currentTimeMillis())
        _peerPresence.value = current
    }

    private fun wireBridgeHandlers(node: P2PNode) {
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
                    _peerEvents.emit(P2PPeerEvent(peerId = peerID, isConnected = true, multiaddress = remoteAddr))

                    val current = _peerPresence.value.toMutableMap()
                    current[peerID] = P2PPeerPresence(
                        peerId = peerID,
                        isOnline = true,
                        multiaddresses = addrs.toList(),
                        lastSeen = System.currentTimeMillis()
                    )
                    _peerPresence.value = current

                    refreshState()
                }
            }

            override fun handlePeerDisconnected(peerID: String) {
                scope.launch {
                    _peerEvents.emit(P2PPeerEvent(peerId = peerID, isConnected = false))

                    val current = _peerPresence.value.toMutableMap()
                    current[peerID]?.let {
                        current[peerID] = it.copy(isOnline = false, lastSeen = System.currentTimeMillis())
                        _peerPresence.value = current
                    }

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

    private suspend fun processIncomingPayload(
        senderPeerId: String,
        rawPayload: String,
        isTopic: Boolean,
        topicName: String?
    ) {
        val wire: P2PWireMessage = try {
            gson.fromJson(rawPayload, P2PWireMessage::class.java)
        } catch (e: Exception) {
            P2PWireMessage(
                type = if (isTopic) "topic" else "dm",
                content = rawPayload,
                messageID = UUID.randomUUID().toString(),
                senderNickname = null
            )
        }

        // Deduplication check to protect from echo loops
        if (!deduplicator.isNew(wire.messageID)) {
            return
        }

        when (wire.type) {
            "ack" -> {
                val ackId = wire.ackMessageId ?: return
                val status = if (wire.ackStatus == "read") P2PReceiptStatus.READ else P2PReceiptStatus.DELIVERED
                _deliveryReceipts.emit(P2PDeliveryReceipt(originalMessageId = ackId, senderPeerId = senderPeerId, status = status))
            }
            "typing" -> {
                val isTyping = wire.isTyping ?: false
                _typingEvents.emit(P2PTypingEvent(peerId = senderPeerId, isTyping = isTyping, topicName = topicName))
            }
            "ping" -> {
                val pong = P2PWireMessage(
                    type = "pong",
                    content = "",
                    messageID = wire.messageID,
                    pingTimestamp = wire.pingTimestamp
                )
                sendMessage(senderPeerId, gson.toJson(pong))
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
                if (!isTopic) {
                    scope.launch {
                        sendDeliveryReceipt(senderPeerId, wire.messageID, P2PReceiptStatus.DELIVERED)
                    }
                }

                _incomingMessages.emit(
                    P2PIncomingMessage(
                        senderPeerID = senderPeerId,
                        content = rawPayload,
                        type = if (isTopic) P2PMessageType.TOPIC_MESSAGE else P2PMessageType.DIRECT_MESSAGE,
                        messageId = wire.messageID,
                        topicName = topicName,
                        timestamp = wire.timestamp,
                        senderNickname = wire.senderNickname,
                        isEncrypted = wire.isEncrypted ?: false,
                        checksumSha256 = wire.checksumSha256
                    )
                )
            }
        }
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

    private fun startAdaptivePoller() {
        pollerJob?.cancel()
        pollerJob = scope.launch {
            while (isActive) {
                // Adaptive poll rate: shorter when connected peers exist, longer when idle
                val pollDelay = if (_connectedPeers.value.isNotEmpty()) {
                    P2PConstants.POLL_INTERVAL_NORMAL_MS
                } else {
                    P2PConstants.POLL_INTERVAL_IDLE_MS
                }
                delay(pollDelay)
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
                scope.launch {
                    if (_nodeStatus.value == P2PNodeStatus.RUNNING) {
                        refreshState()
                        if (p2pConfig.isAutoReconnectEnabled()) {
                            reconnectFavoritesWithBackoff()
                        }
                    }
                }
            }
            override fun onLost(network: Network) {
                scope.launch { refreshState() }
            }
        })
    }

    private suspend fun reconnectFavoritesWithBackoff() {
        val favs = P2PFavoritesRegistry.getFavorites()
        for (fav in favs) {
            val addrs = peerAddressMap[fav]
            addrs?.firstOrNull()?.let { addr ->
                scope.launch {
                    val delayMs = backoffDialer.getNextDelayMs(fav)
                    delay(delayMs)
                    try { connectToPeer(addr) } catch (e: Exception) { /* retry next cycle */ }
                }
            }
        }
    }
}

// ============================================================================
// 9. TOPICS REPOSITORY (GOSSIPSUB PUB/SUB WITH TYPING)
// ============================================================================

/**
 * Manages GossipSub topic subscriptions, multicast message streams, and group typing indicators.
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
                        timestamp = incoming.timestamp,
                        senderNickname = incoming.senderNickname
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

    suspend fun sendTyping(topicName: String, isTyping: Boolean): Result<Unit> {
        return p2pRepository.sendTypingIndicator(targetPeerId = "", isTyping = isTyping, topicName = topicName)
    }

    fun isSubscribed(topicName: String): Boolean = subscribedTopics.contains(topicName)
    fun getSubscribedTopics(): Set<String> = subscribedTopics.toSet()
}

// ============================================================================
// 10. P2P TRANSPORT & CHUNKED FILE COORDINATOR
// ============================================================================

/**
 * High-level P2P Message, Media, ACK, and Encryption Coordinator.
 * Handles automatic 200 KB chunking, SHA-256 integrity, delivery ACKs, and latency pings.
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
    private val activeTransfers = ConcurrentHashMap.newKeySet<String>()

    val repository: P2PLibraryRepository by lazy { P2PLibraryRepository(context) }
    val topics: P2PTopicsRepository by lazy { P2PTopicsRepository(context, repository) }

    private val _messages = MutableSharedFlow<P2PIncomingMessage>(extraBufferCapacity = 64)
    val messages: SharedFlow<P2PIncomingMessage> = _messages.asSharedFlow()

    // Pass-through flows from repository
    val deliveryReceipts: SharedFlow<P2PDeliveryReceipt> get() = repository.deliveryReceipts
    val typingEvents: SharedFlow<P2PTypingEvent> get() = repository.typingEvents
    val peerPresence: StateFlow<Map<String, P2PPeerPresence>> get() = repository.peerPresence

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

    /** Send encrypted direct text message (E2EE) */
    suspend fun sendEncryptedDirectMessage(
        targetPeerId: String,
        text: String,
        aesKeyBase64: String,
        senderNickname: String? = null,
        messageId: String = UUID.randomUUID().toString()
    ): Result<String> = withContext(Dispatchers.IO) {
        val encryptedContent = P2PCrypto.encrypt(text, aesKeyBase64)
        val wire = P2PWireMessage(
            type = "dm",
            content = encryptedContent,
            messageID = messageId,
            senderNickname = senderNickname,
            timestamp = System.currentTimeMillis(),
            isEncrypted = true
        )
        val res = repository.sendMessage(targetPeerId, gson.toJson(wire))
        if (res.isSuccess) Result.success(messageId) else Result.failure(res.exceptionOrNull() ?: Exception("Send failed"))
    }

    /** Stream binary file in 200 KB chunks with SHA-256 integrity checksum and cancellation support */
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
        val checksumSha256 = P2PCrypto.sha256(fileBytes)

        activeTransfers.add(chunkId)

        try {
            for (i in 0 until totalChunks) {
                if (!activeTransfers.contains(chunkId)) {
                    return@withContext Result.failure(Exception("Transfer $chunkId cancelled"))
                }

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
                    fileData = base64,
                    checksumSha256 = checksumSha256
                )

                val sendRes = repository.sendMessage(targetPeerId, gson.toJson(wire))
                if (sendRes.isFailure) {
                    activeTransfers.remove(chunkId)
                    return@withContext Result.failure(sendRes.exceptionOrNull() ?: Exception("Chunk send failed"))
                }
                onProgress?.invoke(i + 1, totalChunks)
            }
            activeTransfers.remove(chunkId)
            Result.success(chunkId)
        } catch (e: Throwable) {
            activeTransfers.remove(chunkId)
            Result.failure(e)
        }
    }

    /** Cancel an active outgoing or incoming media transfer */
    fun cancelMediaTransfer(chunkId: String) {
        activeTransfers.remove(chunkId)
        chunkAssembler.cancelTransfer(chunkId)
    }

    suspend fun sendTyping(targetPeerId: String, isTyping: Boolean) = repository.sendTypingIndicator(targetPeerId, isTyping)
    suspend fun sendReadReceipt(targetPeerId: String, originalMessageId: String) =
        repository.sendDeliveryReceipt(targetPeerId, originalMessageId, P2PReceiptStatus.READ)
    suspend fun pingPeer(targetPeerId: String): Result<Long> = repository.pingPeer(targetPeerId)

    private suspend fun handleRawIncoming(raw: P2PIncomingMessage) {
        val wire: P2PWireMessage = try {
            gson.fromJson(raw.content, P2PWireMessage::class.java)
        } catch (e: Exception) {
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
                chunkData = chunkBytes,
                expectedChecksumSha256 = wire.checksumSha256
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
                        fileSize = assembled.bytes.size.toLong(),
                        checksumSha256 = wire.checksumSha256
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
                    senderNickname = wire.senderNickname,
                    isEncrypted = wire.isEncrypted ?: false
                )
            )
        }
    }
}
