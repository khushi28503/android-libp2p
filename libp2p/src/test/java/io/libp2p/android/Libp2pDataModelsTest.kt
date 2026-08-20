package io.libp2p.android

import org.junit.Assert.*
import org.junit.Test

/**
 * Tests for public data model classes and enums.
 */
class Libp2pDataModelsTest {

    // ---- Libp2pNodeStatus enum ----

    @Test
    fun `node status has exactly 4 states`() {
        val values = Libp2pNodeStatus.values()
        assertEquals(4, values.size)
    }

    @Test
    fun `node status enum values match expected names`() {
        assertEquals("STOPPED", Libp2pNodeStatus.STOPPED.name)
        assertEquals("STARTING", Libp2pNodeStatus.STARTING.name)
        assertEquals("RUNNING", Libp2pNodeStatus.RUNNING.name)
        assertEquals("ERROR", Libp2pNodeStatus.ERROR.name)
    }

    // ---- Libp2pPeerEvent ----

    @Test
    fun `peer event stores connection state`() {
        val event = Libp2pPeerEvent(
            peerId = "12D3KooWTestPeer",
            isConnected = true,
            multiaddress = "/ip4/192.168.1.1/tcp/4001"
        )
        assertEquals("12D3KooWTestPeer", event.peerId)
        assertTrue(event.isConnected)
        assertEquals("/ip4/192.168.1.1/tcp/4001", event.multiaddress)
        assertTrue(event.timestamp > 0)
    }

    @Test
    fun `peer event defaults multiaddress to null`() {
        val event = Libp2pPeerEvent(peerId = "peer1", isConnected = false)
        assertNull(event.multiaddress)
    }

    // ---- Libp2pPeer ----

    @Test
    fun `peer has sensible defaults`() {
        val peer = Libp2pPeer(peerId = "12D3KooWPeer123")
        assertEquals("12D3KooWPeer123", peer.peerId)
        assertFalse(peer.isConnected)
        assertTrue(peer.multiaddresses.isEmpty())
        assertTrue(peer.lastSeen > 0)
    }

    // ---- Libp2pMessage ----

    @Test
    fun `text message is not a media message`() {
        val msg = Libp2pMessage(
            senderPeerId = "sender1",
            content = "Hello"
        )
        assertFalse(msg.isMediaMessage)
        assertNull(msg.fileBytes)
        assertNull(msg.fileName)
        assertNull(msg.contentType)
    }

    @Test
    fun `media message is detected via fileBytes`() {
        val msg = Libp2pMessage(
            senderPeerId = "sender2",
            content = "",
            fileBytes = byteArrayOf(1, 2, 3),
            fileName = "test.bin",
            contentType = "application/octet-stream",
            fileSize = 3L
        )
        assertTrue(msg.isMediaMessage)
        assertEquals("test.bin", msg.fileName)
        assertEquals(3, msg.fileBytes!!.size)
    }

    @Test
    fun `message defaults to non-topic message`() {
        val msg = Libp2pMessage(senderPeerId = "peer", content = "test")
        assertFalse(msg.isTopicMessage)
        assertNull(msg.topicName)
    }

    @Test
    fun `topic message preserves topic name`() {
        val msg = Libp2pMessage(
            senderPeerId = "peer",
            content = "broadcast",
            isTopicMessage = true,
            topicName = "general-chat"
        )
        assertTrue(msg.isTopicMessage)
        assertEquals("general-chat", msg.topicName)
    }

    @Test
    fun `message has auto-generated UUID and timestamp`() {
        val msg = Libp2pMessage(senderPeerId = "p", content = "x")
        assertNotNull(msg.messageId)
        assertTrue(msg.messageId.isNotBlank())
        assertTrue(msg.timestamp > 0)
    }

    // ---- Libp2pBandwidthStats ----

    @Test
    fun `bandwidth stats default to zero`() {
        val stats = Libp2pBandwidthStats()
        assertEquals(0L, stats.sessionInBytes)
        assertEquals(0L, stats.sessionOutBytes)
        assertEquals(0L, stats.sessionTotalBytes)
        assertEquals(0L, stats.dailyInBytes)
        assertEquals(0L, stats.dailyOutBytes)
        assertEquals(0L, stats.dailyTotalBytes)
        assertEquals(0.0, stats.currentRateInBytesPerSec, 0.001)
        assertEquals(0.0, stats.currentRateOutBytesPerSec, 0.001)
        assertEquals(0L, stats.projectedHourlyTotalBytes)
        assertEquals("{}", stats.rawJson)
    }
}
