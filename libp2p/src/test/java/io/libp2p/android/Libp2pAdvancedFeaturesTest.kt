package io.libp2p.android

import com.google.gson.Gson
import org.junit.Assert.*
import org.junit.Test

/**
 * Unit tests for Advanced P2P Features:
 * - End-to-End Encryption (AES-256-GCM)
 * - Delivery Receipts (ACK)
 * - Typing Indicators
 * - Latency Ping / Pong
 * - Transfer Cancellation
 */
class Libp2pAdvancedFeaturesTest {

    private val gson = Gson()

    // ── 1. End-to-End Encryption (AES-256-GCM) ──────────────────────────

    @Test
    fun `crypto generates valid 256-bit AES key`() {
        val key = Libp2pCrypto.generateKey()
        assertNotNull(key)
        assertTrue(key.isNotBlank())
        // 32 bytes in Base64 is 44 characters with padding
        assertEquals(44, key.length)
    }

    @Test
    fun `crypto encrypts and decrypts text successfully`() {
        val key = Libp2pCrypto.generateKey()
        val originalText = "Top secret decentralized P2P message! 🔒"

        val cipherText = Libp2pCrypto.encrypt(originalText, key)
        assertNotEquals(originalText, cipherText)

        val decryptedText = Libp2pCrypto.decrypt(cipherText, key)
        assertEquals(originalText, decryptedText)
    }

    @Test
    fun `crypto encrypts and decrypts binary payloads`() {
        val key = Libp2pCrypto.generateKey()
        val originalBytes = byteArrayOf(0x01, 0x02, 0x03, 0xFF.toByte(), 0xAA.toByte())

        val cipherBase64 = Libp2pCrypto.encryptBytes(originalBytes, key)
        val decryptedBytes = Libp2pCrypto.decryptBytes(cipherBase64, key)

        assertArrayEquals(originalBytes, decryptedBytes)
    }

    @Test(expected = Exception::class)
    fun `crypto decryption with wrong key fails`() {
        val keyA = Libp2pCrypto.generateKey()
        val keyB = Libp2pCrypto.generateKey()

        val cipherText = Libp2pCrypto.encrypt("Secret", keyA)
        Libp2pCrypto.decrypt(cipherText, keyB) // Should throw GCM auth tag mismatch
    }

    // ── 2. Transfer Cancellation ────────────────────────────────────────

    @Test
    fun `chunk assembler cancelTransfer removes in-flight state`() {
        val assembler = Libp2pChunkAssembler()
        val chunk0 = "PART_1".toByteArray()

        // Send 1 of 3 chunks
        val res = assembler.addChunk("cancel-test-01", 0, 3, "text/plain", "doc.txt", chunk0)
        assertNull(res)
        assertTrue(assembler.isTransferActive("cancel-test-01"))

        // Cancel transfer
        val cancelled = assembler.cancelTransfer("cancel-test-01")
        assertTrue(cancelled)
        assertFalse(assembler.isTransferActive("cancel-test-01"))
    }

    // ── 3. Wire Message Protocols (ACK, Typing, Ping) ───────────────────

    @Test
    fun `delivery ACK wire message serializes and deserializes`() {
        val ack = Libp2pWireMessage(
            type = "ack",
            content = "",
            messageID = "ack-uuid-123",
            ackMessageId = "orig-msg-999",
            ackStatus = "delivered"
        )

        val json = gson.toJson(ack)
        val restored = gson.fromJson(json, Libp2pWireMessage::class.java)

        assertEquals("ack", restored.type)
        assertEquals("orig-msg-999", restored.ackMessageId)
        assertEquals("delivered", restored.ackStatus)
    }

    @Test
    fun `typing indicator wire message serializes and deserializes`() {
        val typing = Libp2pWireMessage(
            type = "typing",
            content = "",
            messageID = "type-uuid-456",
            isTyping = true
        )

        val json = gson.toJson(typing)
        val restored = gson.fromJson(json, Libp2pWireMessage::class.java)

        assertEquals("typing", restored.type)
        assertEquals(true, restored.isTyping)
    }

    @Test
    fun `ping pong latency wire message serializes and deserializes`() {
        val ping = Libp2pWireMessage(
            type = "ping",
            content = "",
            messageID = "ping-uuid-789",
            pingTimestamp = 1700000000000L
        )

        val json = gson.toJson(ping)
        val restored = gson.fromJson(json, Libp2pWireMessage::class.java)

        assertEquals("ping", restored.type)
        assertEquals(1700000000000L, restored.pingTimestamp)
    }

    // ── 4. Models & Enums ────────────────────────────────────────────────

    @Test
    fun `receipt status enum has DELIVERED and READ`() {
        assertEquals("DELIVERED", Libp2pReceiptStatus.DELIVERED.name)
        assertEquals("READ", Libp2pReceiptStatus.READ.name)
    }

    @Test
    fun `peer presence model stores latency and status`() {
        val presence = Libp2pPeerPresence(
            peerId = "12D3KooWPeerXYZ",
            isOnline = true,
            rttLatencyMs = 42L,
            multiaddresses = listOf("/ip4/127.0.0.1/tcp/4001")
        )

        assertEquals("12D3KooWPeerXYZ", presence.peerId)
        assertTrue(presence.isOnline)
        assertEquals(42L, presence.rttLatencyMs)
        assertEquals(1, presence.multiaddresses.size)
    }
}
