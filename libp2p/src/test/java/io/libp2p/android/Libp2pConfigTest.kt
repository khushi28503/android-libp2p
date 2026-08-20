package io.libp2p.android

import org.junit.Assert.*
import org.junit.Test

/**
 * Tests for Libp2pConfig defaults, custom values, and companion constants.
 */
class Libp2pConfigTest {

    @Test
    fun `default config has port 0 for automatic assignment`() {
        val config = Libp2pConfig()
        assertEquals(0, config.listenPort)
    }

    @Test
    fun `default config has relay enabled`() {
        val config = Libp2pConfig()
        assertTrue(config.enableRelay)
    }

    @Test
    fun `default config has DHT enabled`() {
        val config = Libp2pConfig()
        assertTrue(config.enableDht)
    }

    @Test
    fun `default chunk size is 200 KB`() {
        val config = Libp2pConfig()
        assertEquals(200 * 1024, config.mediaChunkSizeBytes)
    }

    @Test
    fun `default bandwidth limit is unlimited (0)`() {
        val config = Libp2pConfig()
        assertEquals(0L, config.bandwidthLimitBytesPerSec)
    }

    @Test
    fun `default bootstrap peers contain 4 IPFS nodes`() {
        val config = Libp2pConfig()
        assertEquals(4, config.bootstrapPeers.size)
        assertTrue(config.bootstrapPeers.all { it.contains("bootstrap.libp2p.io") })
    }

    @Test
    fun `default ICE servers contain Google STUN servers`() {
        val config = Libp2pConfig()
        assertEquals(3, config.iceServers.size)
        assertTrue(config.iceServers.all { it.startsWith("stun:") })
    }

    @Test
    fun `custom config overrides all defaults`() {
        val customBootstrap = listOf("/ip4/1.2.3.4/tcp/4001/p2p/QmCustomPeer")
        val customIce = listOf("turn:myturn.server:3478")

        val config = Libp2pConfig(
            listenPort = 9999,
            enableRelay = false,
            enableDht = false,
            mediaChunkSizeBytes = 100 * 1024,
            bootstrapPeers = customBootstrap,
            iceServers = customIce,
            bandwidthLimitBytesPerSec = 500_000L
        )

        assertEquals(9999, config.listenPort)
        assertFalse(config.enableRelay)
        assertFalse(config.enableDht)
        assertEquals(100 * 1024, config.mediaChunkSizeBytes)
        assertEquals(customBootstrap, config.bootstrapPeers)
        assertEquals(customIce, config.iceServers)
        assertEquals(500_000L, config.bandwidthLimitBytesPerSec)
    }

    @Test
    fun `config data class equality works`() {
        val a = Libp2pConfig(listenPort = 42)
        val b = Libp2pConfig(listenPort = 42)
        assertEquals(a, b)
    }

    @Test
    fun `config data class copy works`() {
        val original = Libp2pConfig()
        val modified = original.copy(listenPort = 8080, enableRelay = false)
        assertEquals(8080, modified.listenPort)
        assertFalse(modified.enableRelay)
        // Unchanged fields preserved
        assertTrue(modified.enableDht)
        assertEquals(200 * 1024, modified.mediaChunkSizeBytes)
    }
}
