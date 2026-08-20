package io.libp2p.android.sample

import android.os.Bundle
import android.util.Log
import android.widget.Toast
import androidx.appcompat.app.AppCompatActivity
import io.libp2p.android.Libp2pClient
import io.libp2p.android.Libp2pConfig
import io.libp2p.android.Libp2pNodeStatus
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach

/**
 * Minimal sample demonstrating how to integrate the android-libp2p library.
 *
 * This activity:
 * 1. Starts a libp2p node
 * 2. Subscribes to a GossipSub topic
 * 3. Listens for incoming direct messages
 * 4. Monitors peer connection events
 */
class SampleActivity : AppCompatActivity() {

    companion object {
        private const val TAG = "Libp2pSample"
    }

    private lateinit var client: Libp2pClient
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main)

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        // 1. Initialize the client with default config
        client = Libp2pClient.getInstance(this, Libp2pConfig())

        // 2. Observe node status changes
        client.nodeStatus.onEach { status ->
            Log.i(TAG, "Node status: $status")
            if (status == Libp2pNodeStatus.RUNNING) {
                Toast.makeText(this, "P2P Node Running! ID: ${client.peerId.value}", Toast.LENGTH_LONG).show()
            }
        }.launchIn(scope)

        // 3. Listen for incoming direct messages
        client.incomingMessages.onEach { message ->
            if (message.isMediaMessage) {
                Log.i(TAG, "Received media: ${message.fileName} (${message.fileBytes?.size} bytes)")
            } else {
                Log.i(TAG, "Received DM from ${message.senderPeerId}: ${message.content}")
            }
        }.launchIn(scope)

        // 4. Monitor peer connect/disconnect events
        client.peerEvents.onEach { event ->
            val action = if (event.isConnected) "connected" else "disconnected"
            Log.i(TAG, "Peer ${event.peerId} $action")
        }.launchIn(scope)

        // 5. Subscribe to a GossipSub topic
        client.subscribeTopic("android-libp2p-demo").onEach { msg ->
            Log.i(TAG, "[Topic] ${msg.senderNickname ?: msg.senderPeerId}: ${msg.content}")
        }.launchIn(scope)

        // 6. Start the node
        scope.launch {
            val result = client.start()
            result.onSuccess { peerId ->
                Log.i(TAG, "Node started successfully! PeerID: $peerId")

                // Example: Send a topic broadcast
                client.publishTopic(
                    topicName = "android-libp2p-demo",
                    text = "Hello from android-libp2p sample!",
                    senderNickname = "SampleApp"
                )
            }.onFailure { error ->
                Log.e(TAG, "Failed to start node", error)
            }
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        scope.launch {
            client.stop()
        }
        scope.cancel()
    }
}
