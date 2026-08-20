package io.libp2p.android

import com.google.gson.Gson
import org.junit.Assert.*
import org.junit.Test

/**
 * Tests for the Libp2pWireMessage JSON wire format — serialization and deserialization.
 */
class Libp2pWireMessageTest {

    private val gson = Gson()

    @Test
    fun `text DM serializes to valid JSON`() {
        val wire = Libp2pWireMessage(
            type = "dm",
            content = "Hello from P2P!",
            messageID = "msg-001",
            senderNickname = "Alice",
            timestamp = 1700000000000L
        )

        val json = gson.toJson(wire)
        assertTrue(json.contains("\"type\":\"dm\""))
        assertTrue(json.contains("\"content\":\"Hello from P2P!\""))
        assertTrue(json.contains("\"messageID\":\"msg-001\""))
        assertTrue(json.contains("\"senderNickname\":\"Alice\""))
    }

    @Test
    fun `text DM deserializes from JSON`() {
        val json = """
            {
                "type": "dm",
                "content": "Test message",
                "messageID": "msg-002",
                "senderNickname": "Bob",
                "timestamp": 1700000000000
            }
        """.trimIndent()

        val wire = gson.fromJson(json, Libp2pWireMessage::class.java)
        assertEquals("dm", wire.type)
        assertEquals("Test message", wire.content)
        assertEquals("msg-002", wire.messageID)
        assertEquals("Bob", wire.senderNickname)
        assertEquals(1700000000000L, wire.timestamp)
        // Media fields should be null
        assertNull(wire.chunkId)
        assertNull(wire.chunkIndex)
        assertNull(wire.totalChunks)
        assertNull(wire.fileData)
    }

    @Test
    fun `media chunk serializes with all fields`() {
        val wire = Libp2pWireMessage(
            type = "dm",
            content = "",
            messageID = "msg-003",
            senderNickname = "Charlie",
            timestamp = 1700000000000L,
            contentType = "image/jpeg",
            fileName = "photo.jpg",
            fileSize = 500_000L,
            chunkId = "chunk-uuid-001",
            chunkIndex = 2,
            totalChunks = 5,
            fileData = "base64encodeddata=="
        )

        val json = gson.toJson(wire)
        assertTrue(json.contains("\"chunkId\":\"chunk-uuid-001\""))
        assertTrue(json.contains("\"chunkIndex\":2"))
        assertTrue(json.contains("\"totalChunks\":5"))
        assertTrue(json.contains("\"fileData\":\"base64encodeddata==\""))
        assertTrue(json.contains("\"fileName\":\"photo.jpg\""))
        assertTrue(json.contains("\"fileSize\":500000"))
    }

    @Test
    fun `media chunk round-trip preserves all fields`() {
        val original = Libp2pWireMessage(
            type = "topic",
            content = "",
            messageID = "msg-004",
            senderNickname = null,
            timestamp = 1700000000000L,
            contentType = "audio/opus",
            fileName = "voice.ogg",
            fileSize = 12345L,
            chunkId = "xfer-999",
            chunkIndex = 0,
            totalChunks = 1,
            fileData = "SGVsbG8="
        )

        val json = gson.toJson(original)
        val restored = gson.fromJson(json, Libp2pWireMessage::class.java)

        assertEquals(original.type, restored.type)
        assertEquals(original.messageID, restored.messageID)
        assertEquals(original.contentType, restored.contentType)
        assertEquals(original.fileName, restored.fileName)
        assertEquals(original.fileSize, restored.fileSize)
        assertEquals(original.chunkId, restored.chunkId)
        assertEquals(original.chunkIndex, restored.chunkIndex)
        assertEquals(original.totalChunks, restored.totalChunks)
        assertEquals(original.fileData, restored.fileData)
    }

    @Test
    fun `topic message type serializes correctly`() {
        val wire = Libp2pWireMessage(
            type = "topic",
            content = "Broadcast!",
            messageID = "msg-005",
            senderNickname = "GroupUser",
            timestamp = 1700000000000L
        )

        val json = gson.toJson(wire)
        assertTrue(json.contains("\"type\":\"topic\""))

        val restored = gson.fromJson(json, Libp2pWireMessage::class.java)
        assertEquals("topic", restored.type)
        assertEquals("Broadcast!", restored.content)
    }

    @Test
    fun `null optional fields are excluded or null in JSON`() {
        val wire = Libp2pWireMessage(
            type = "dm",
            content = "Simple text",
            messageID = "msg-006",
            senderNickname = null,
            timestamp = 1700000000000L
        )

        val json = gson.toJson(wire)
        val restored = gson.fromJson(json, Libp2pWireMessage::class.java)

        assertNull(restored.senderNickname)
        assertNull(restored.contentType)
        assertNull(restored.fileName)
        assertNull(restored.fileSize)
        assertNull(restored.chunkId)
        assertNull(restored.chunkIndex)
        assertNull(restored.totalChunks)
        assertNull(restored.fileData)
    }
}
