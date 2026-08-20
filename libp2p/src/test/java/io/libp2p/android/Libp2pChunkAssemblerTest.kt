package io.libp2p.android

import org.junit.Assert.*
import org.junit.Before
import org.junit.Test

/**
 * Tests for the Libp2pChunkAssembler — the core media chunking/reassembly engine.
 * All tests run on pure JVM without Android dependencies.
 */
class Libp2pChunkAssemblerTest {

    private lateinit var assembler: Libp2pChunkAssembler

    @Before
    fun setUp() {
        // Long timeout so tests don't expire during execution
        assembler = Libp2pChunkAssembler(timeoutMs = 60_000L, cleanupIntervalMs = 60_000L)
    }

    @Test
    fun `single chunk file assembles immediately`() {
        val data = "Hello, P2P World!".toByteArray()

        val result = assembler.addChunk(
            chunkId = "transfer-001",
            chunkIndex = 0,
            totalChunks = 1,
            contentType = "text/plain",
            fileName = "hello.txt",
            chunkData = data
        )

        assertNotNull(result)
        assertArrayEquals(data, result!!.bytes)
        assertEquals("text/plain", result.contentType)
        assertEquals("hello.txt", result.fileName)
    }

    @Test
    fun `multi-chunk file assembles in order`() {
        val chunk0 = "AAAA".toByteArray()
        val chunk1 = "BBBB".toByteArray()
        val chunk2 = "CCCC".toByteArray()

        val r0 = assembler.addChunk("t-002", 0, 3, "application/octet-stream", "data.bin", chunk0)
        assertNull("Should not assemble after first chunk", r0)

        val r1 = assembler.addChunk("t-002", 1, 3, "application/octet-stream", "data.bin", chunk1)
        assertNull("Should not assemble after second chunk", r1)

        val r2 = assembler.addChunk("t-002", 2, 3, "application/octet-stream", "data.bin", chunk2)
        assertNotNull("Should assemble after final chunk", r2)

        val expected = "AAAABBBBCCCC".toByteArray()
        assertArrayEquals(expected, r2!!.bytes)
    }

    @Test
    fun `multi-chunk file assembles out of order`() {
        val chunk0 = "1111".toByteArray()
        val chunk1 = "2222".toByteArray()
        val chunk2 = "3333".toByteArray()

        // Send chunks out of order: 2, 0, 1
        val r2 = assembler.addChunk("t-003", 2, 3, "image/jpeg", "photo.jpg", chunk2)
        assertNull(r2)

        val r0 = assembler.addChunk("t-003", 0, 3, "image/jpeg", "photo.jpg", chunk0)
        assertNull(r0)

        val r1 = assembler.addChunk("t-003", 1, 3, "image/jpeg", "photo.jpg", chunk1)
        assertNotNull("All chunks received, should assemble", r1)

        val expected = "111122223333".toByteArray()
        assertArrayEquals(expected, r1!!.bytes)
        assertEquals("image/jpeg", r1.contentType)
        assertEquals("photo.jpg", r1.fileName)
    }

    @Test
    fun `duplicate chunk does not cause double assembly`() {
        val chunk0 = "AA".toByteArray()
        val chunk1 = "BB".toByteArray()

        assembler.addChunk("t-004", 0, 2, "text/plain", "dup.txt", chunk0)
        // Duplicate chunk 0
        assembler.addChunk("t-004", 0, 2, "text/plain", "dup.txt", chunk0)

        val result = assembler.addChunk("t-004", 1, 2, "text/plain", "dup.txt", chunk1)
        assertNotNull(result)
        assertArrayEquals("AABB".toByteArray(), result!!.bytes)
    }

    @Test
    fun `independent transfers do not interfere`() {
        val dataA0 = "A0".toByteArray()
        val dataA1 = "A1".toByteArray()
        val dataB0 = "B0".toByteArray()
        val dataB1 = "B1".toByteArray()

        // Interleave two transfers
        assertNull(assembler.addChunk("transfer-A", 0, 2, "text/plain", "a.txt", dataA0))
        assertNull(assembler.addChunk("transfer-B", 0, 2, "text/plain", "b.txt", dataB0))
        
        val resultA = assembler.addChunk("transfer-A", 1, 2, "text/plain", "a.txt", dataA1)
        assertNotNull(resultA)
        assertArrayEquals("A0A1".toByteArray(), resultA!!.bytes)
        assertEquals("a.txt", resultA.fileName)

        val resultB = assembler.addChunk("transfer-B", 1, 2, "text/plain", "b.txt", dataB1)
        assertNotNull(resultB)
        assertArrayEquals("B0B1".toByteArray(), resultB!!.bytes)
        assertEquals("b.txt", resultB.fileName)
    }

    @Test
    fun `large binary chunk data preserved exactly`() {
        // Simulate a 200 KB chunk
        val largeData = ByteArray(200 * 1024) { (it % 256).toByte() }

        val result = assembler.addChunk("t-large", 0, 1, "application/octet-stream", "large.bin", largeData)
        assertNotNull(result)
        assertEquals(200 * 1024, result!!.bytes.size)
        assertArrayEquals(largeData, result.bytes)
    }

    @Test
    fun `incomplete transfer returns null`() {
        // Only send 2 of 5 chunks — should never assemble
        assembler.addChunk("t-incomplete", 0, 5, "video/mp4", "video.mp4", "chunk0".toByteArray())
        val result = assembler.addChunk("t-incomplete", 3, 5, "video/mp4", "video.mp4", "chunk3".toByteArray())
        assertNull("Should not assemble with only 2/5 chunks", result)
    }

    @Test
    fun `assembled media preserves content type and filename`() {
        val result = assembler.addChunk(
            "t-meta", 0, 1, "audio/opus", "voice_note.ogg", "audiodata".toByteArray()
        )
        assertNotNull(result)
        assertEquals("audio/opus", result!!.contentType)
        assertEquals("voice_note.ogg", result.fileName)
    }

    @Test
    fun `shutdown cancels cleanup coroutine`() {
        // Should not throw
        assembler.shutdown()
    }

    @Test
    fun `reassembly clears pending state after completion`() {
        val data = "test".toByteArray()

        val result = assembler.addChunk("t-clear", 0, 1, "text/plain", "t.txt", data)
        assertNotNull(result)

        // After assembly, adding a chunk with the same ID starts a new transfer
        val result2 = assembler.addChunk("t-clear", 0, 2, "text/plain", "t.txt", "new".toByteArray())
        assertNull("Should start fresh transfer, not auto-complete", result2)
    }
}
