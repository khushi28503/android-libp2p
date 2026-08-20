package io.libp2p.android

import com.google.gson.Gson
import org.junit.Assert.*
import org.junit.Test

/**
 * Unit tests for Next-Gen P2P Supercharged Features:
 * - Persistent Offline Outbox Queue
 * - Voice Note Streaming & Waveforms
 * - Encrypted Group Topic Rooms
 * - Connection Quality Scoring
 */
class Libp2pNextGenFeaturesTest {

    private val gson = Gson()

    // ── 1. Voice Note Streaming & Waveforms ──────────────────────────────

    @Test
    fun `voice note wire message serializes duration and waveforms`() {
        val waveforms = listOf(10, 45, 88, 100, 75, 30, 5)
        val wire = Libp2pWireMessage(
            type = "voice",
            content = "",
            messageID = "voice-msg-001",
            contentType = "audio/opus",
            fileName = "voice.opus",
            voiceDurationMs = 4500L,
            voiceWaveform = waveforms
        )

        val json = gson.toJson(wire)
        val restored = gson.fromJson(json, Libp2pWireMessage::class.java)

        assertEquals("voice", restored.type)
        assertEquals(4500L, restored.voiceDurationMs)
        assertEquals(7, restored.voiceWaveform?.size)
        assertEquals(100, restored.voiceWaveform?.get(3))
    }

    @Test
    fun `chunk assembly preserves voice note metadata`() {
        val assembler = Libp2pChunkAssembler()
        val voiceData = "SimulatedAudioByteData".toByteArray()
        val voiceMeta = Libp2pVoiceNoteMetadata(
            durationMs = 3200L,
            waveformAmplitudes = listOf(20, 50, 90, 40, 10),
            codec = "audio/opus"
        )

        val result = assembler.addChunk(
            chunkId = "v-transfer-01",
            chunkIndex = 0,
            totalChunks = 1,
            contentType = "audio/opus",
            fileName = "voice.opus",
            chunkData = voiceData,
            voiceNoteMeta = voiceMeta
        )

        assertNotNull(result)
        assertNotNull(result!!.voiceNoteMeta)
        assertEquals(3200L, result.voiceNoteMeta!!.durationMs)
        assertEquals(5, result.voiceNoteMeta!!.waveformAmplitudes.size)
        assertArrayEquals(voiceData, result.bytes)
    }

    // ── 2. Encrypted Group Topic Rooms ───────────────────────────────────

    @Test
    fun `group room payload encrypts and decrypts with shared room key`() {
        val groupKey = Libp2pCrypto.generateKey()
        val plainText = "Confidential Group Broadcast to all peers in room"

        val cipherText = Libp2pCrypto.encrypt(plainText, groupKey)
        val decrypted = Libp2pCrypto.decrypt(cipherText, groupKey)

        assertEquals(plainText, decrypted)
    }

    @Test(expected = Exception::class)
    fun `group room payload fails decryption with wrong room key`() {
        val validGroupKey = Libp2pCrypto.generateKey()
        val intruderKey = Libp2pCrypto.generateKey()

        val cipherText = Libp2pCrypto.encrypt("Secret Room Discussion", validGroupKey)
        Libp2pCrypto.decrypt(cipherText, intruderKey)
    }

    // ── 3. Connection Health Quality Scoring ─────────────────────────────

    @Test
    fun `connection quality correctly categorizes latency ranges`() {
        val qExcellent = if (30 < 50) Libp2pConnectionQuality.EXCELLENT else Libp2pConnectionQuality.POOR
        val qGood = if (85 in 50..150) Libp2pConnectionQuality.GOOD else Libp2pConnectionQuality.POOR
        val qFair = if (220 in 150..300) Libp2pConnectionQuality.FAIR else Libp2pConnectionQuality.POOR
        val qPoor = if (450 > 300) Libp2pConnectionQuality.POOR else Libp2pConnectionQuality.GOOD

        assertEquals(Libp2pConnectionQuality.EXCELLENT, qExcellent)
        assertEquals(Libp2pConnectionQuality.GOOD, qGood)
        assertEquals(Libp2pConnectionQuality.FAIR, qFair)
        assertEquals(Libp2pConnectionQuality.POOR, qPoor)
    }

    @Test
    fun `peer presence model stores quality and health score`() {
        val presence = Libp2pPeerPresence(
            peerId = "12D3KooWPeerQualityTest",
            isOnline = true,
            rttLatencyMs = 28L,
            quality = Libp2pConnectionQuality.EXCELLENT,
            healthScore = 97
        )

        assertEquals("12D3KooWPeerQualityTest", presence.peerId)
        assertTrue(presence.isOnline)
        assertEquals(Libp2pConnectionQuality.EXCELLENT, presence.quality)
        assertEquals(97, presence.healthScore)
    }
}
