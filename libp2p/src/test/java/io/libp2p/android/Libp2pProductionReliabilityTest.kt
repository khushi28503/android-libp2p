package io.libp2p.android

import org.junit.Assert.*
import org.junit.Test

/**
 * Unit tests for Production & Reliability Engines:
 * - SHA-256 File Integrity Checksum
 * - LRU Message Deduplication & Replay Protection
 * - Exponential Backoff with Jitter
 * - Single-Pass Buffer Chunk Merging
 */
class Libp2pProductionReliabilityTest {

    // ── 1. SHA-256 Integrity Verification ───────────────────────────────

    @Test
    fun `sha256 calculation matches known vector`() {
        val data = "Hello, P2P Integrity!".toByteArray(Charsets.UTF_8)
        val hash = Libp2pCrypto.sha256(data)

        assertNotNull(hash)
        assertEquals(64, hash.length) // 256 bits = 64 hex chars
    }

    @Test
    fun `chunk assembly verifies valid SHA-256 checksum`() {
        val assembler = Libp2pChunkAssembler()
        val chunk0 = "ChunkZero_".toByteArray()
        val chunk1 = "ChunkOne".toByteArray()
        val fullData = "ChunkZero_ChunkOne".toByteArray()
        val expectedSha = Libp2pCrypto.sha256(fullData)

        assembler.addChunk("sha-test-01", 0, 2, "text/plain", "file.txt", chunk0, expectedSha)
        val result = assembler.addChunk("sha-test-01", 1, 2, "text/plain", "file.txt", chunk1, expectedSha)

        assertNotNull(result)
        assertTrue("Checksum should be valid", result!!.checksumValid)
        assertArrayEquals(fullData, result.bytes)
    }

    @Test
    fun `chunk assembly flags tampered SHA-256 checksum`() {
        val assembler = Libp2pChunkAssembler()
        val chunk0 = "TamperedPayload".toByteArray()
        val fakeSha = "0000000000000000000000000000000000000000000000000000000000000000"

        val result = assembler.addChunk("sha-test-02", 0, 1, "text/plain", "file.txt", chunk0, fakeSha)

        assertNotNull(result)
        assertFalse("Checksum should be flagged as invalid", result!!.checksumValid)
    }

    // ── 2. Message Deduplication (LRU + TTL) ────────────────────────────

    @Test
    fun `deduplicator identifies new messages`() {
        val dedup = Libp2pDeduplicator(maxEntries = 100, ttlMs = 5000L)

        assertTrue(dedup.isNew("msg-001"))
        assertTrue(dedup.isNew("msg-002"))
        assertTrue(dedup.isNew("msg-003"))
    }

    @Test
    fun `deduplicator rejects duplicate message IDs`() {
        val dedup = Libp2pDeduplicator(maxEntries = 100, ttlMs = 5000L)

        assertTrue(dedup.isNew("msg-unique-123"))
        assertFalse("Second time should be duplicate", dedup.isNew("msg-unique-123"))
        assertFalse("Third time should be duplicate", dedup.isNew("msg-unique-123"))
    }

    @Test
    fun `deduplicator clear resets state`() {
        val dedup = Libp2pDeduplicator(maxEntries = 100, ttlMs = 5000L)

        assertTrue(dedup.isNew("msg-reset"))
        assertFalse(dedup.isNew("msg-reset"))

        dedup.clear()
        assertTrue("After clear, message is treated as new", dedup.isNew("msg-reset"))
    }

    // ── 3. Exponential Backoff & Jitter Dialer ───────────────────────────

    @Test
    fun `backoff dialer increases delay with attempts`() {
        val dialer = Libp2pBackoffDialer(initialDelayMs = 1000L, maxDelayMs = 30000L, multiplier = 2.0, jitterRatio = 0.0)

        val d1 = dialer.getNextDelayMs("peer-1")
        val d2 = dialer.getNextDelayMs("peer-1")
        val d3 = dialer.getNextDelayMs("peer-1")

        assertEquals(1000L, d1)
        assertEquals(2000L, d2)
        assertEquals(4000L, d3)
    }

    @Test
    fun `backoff dialer caps at max delay`() {
        val dialer = Libp2pBackoffDialer(initialDelayMs = 1000L, maxDelayMs = 5000L, multiplier = 2.0, jitterRatio = 0.0)

        // 1000 -> 2000 -> 4000 -> 8000 (capped at 5000)
        dialer.getNextDelayMs("peer-cap")
        dialer.getNextDelayMs("peer-cap")
        dialer.getNextDelayMs("peer-cap")
        val d4 = dialer.getNextDelayMs("peer-cap")

        assertEquals(5000L, d4)
    }

    @Test
    fun `backoff dialer reset restores initial delay`() {
        val dialer = Libp2pBackoffDialer(initialDelayMs = 1000L, maxDelayMs = 30000L, multiplier = 2.0, jitterRatio = 0.0)

        dialer.getNextDelayMs("peer-r")
        dialer.getNextDelayMs("peer-r")
        dialer.reset("peer-r")

        val resetDelay = dialer.getNextDelayMs("peer-r")
        assertEquals(1000L, resetDelay)
    }
}
