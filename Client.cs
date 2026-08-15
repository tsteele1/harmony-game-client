using System.Net.WebSockets;
using System.Buffers;

namespace Harmony {

/*
 * A single "Game / Host" Client that can connect / interface with Harmony Servers.
 *
 * Used to abstract away the complexities of sending and receiving WebSocket messages.
*/
public class Client: IClientMessaging {
    public struct CloseStatus {
        public WebSocketCloseStatus statusCode;
        public string statusDescription;

        public CloseStatus(WebSocketCloseStatus status, string statusDescription) {
            this.statusCode = status;
            this.statusDescription = statusDescription;
        }
    }

    public WebSocketState State { get { return socket.State; } }

    /* Internal Variables */
    // A way for the server to know how many clients you want to be able to communicate with.
    // NOTE: This does not mean that the Client class itself will enforce this. It's for the
    // server.
    int maxClientCount;

    // The Room Code you will connect to on the Server side.
    public string Id { get; set; }

    // What you want to do when you receive a Message from the server.
    IClientMessageHandler messageHandler;

    // Completely shuts down the Client from running.
    // Expected as a one time use to finish up the program.
    CancellationToken cancelToken;

    public bool Connected { get { return (socket.State == WebSocketState.Connecting || socket.State == WebSocketState.Open); } }

    private bool stopReconnecting = false;

    private SemaphoreSlim disconnectSemaphore = new SemaphoreSlim(1, 1);
    private bool disconnected = false;

    private string[] receivers = [];
    public string[] Receivers { get { return receivers; } set { receivers = value; } }

    /* Web Variables */
    Messenger messenger = new Messenger();

    // For compatibility with IClientMessaging
    public Messenger Messager { get { return messenger; } }

    ClientWebSocket socket = new ClientWebSocket();

    /* Reconnection Logic*/
    private TimeSpan baseReconnectDelay = TimeSpan.FromSeconds(1);

    private TimeSpan maxReconnectDelay = TimeSpan.FromSeconds(30);

    // Retries for reconnecting.
    private int retries = 0;

    private int maxRetries = 10;

    private int messageBufferSize;

    // See variable definitions for clarification on what each individual member does.
    // NOTE: Throws ArgumentException if maxClientCount or messageBufferSize are unusable integers for our purposes.
    // (you want to connect to at least one client, and you want to receive at least one byte of data per message).
    public Client(int maxClientCount, IClientMessageHandler messageHandler, CancellationToken cancelToken, int messageBufferSize = 4096) {
        if (maxClientCount <= 0) {
            throw new ArgumentException("Expected value >= 1", "maxClientCount");
        }
        else if (messageBufferSize <= 0) {
            throw new ArgumentException("Expected value >= 1", "messageBufferSize");
        }

        this.Id = String.Empty;
        this.maxClientCount = maxClientCount;
        this.messageBufferSize = messageBufferSize;
        this.messageHandler = messageHandler;
        this.cancelToken = cancelToken;
    }

    /*
     * Connect to a given Room or retry if a connection is not established (up to maxRetries times).
     *
     * Parameters:
     *      addr (string): The address (an API endpoint with no trailing slash) to connect to for room creation.
     *
     * Returns:
     *      A status providing information about the WebSocketClosure
     *      (default is NormalClosure, ProtocolError for a received Error, and EndpointUnavailable upon shutdown).
     *
     * NOTE:
     *      Implements jittered reconnections (increasing delay between reconnect attempts with some random time offset).
     *      Stops immediately if it receives an error from the Server or is manually cancelled client-side via provided
     *      CancellationToken. Attempts to reconnect to the provided Room Id via adding it as a query parameter at the end of addr 
     *      if an Id has been retrieved (you will need to implement the logic for getting the Id).
    */
    public async Task<CloseStatus> ConnectToRoomWithRetryAsync(string addr) {
        CloseStatus closeStatus = new CloseStatus(WebSocketCloseStatus.NormalClosure, "Normal Closure");
        TimeSpan reconnectDelay = TimeSpan.FromSeconds(baseReconnectDelay.TotalSeconds);
        Random randomDelay = Random.Shared;

        while (!cancelToken.IsCancellationRequested && retries < maxRetries) {
            if (stopReconnecting) break;

            socket = new ClientWebSocket();
            socket.Options.KeepAliveInterval = TimeSpan.FromSeconds(20);

            try {
                if (String.IsNullOrEmpty(Id)) {
                    await socket.ConnectAsync(new Uri(addr), cancelToken);
                }
                else {
                    await socket.ConnectAsync(new Uri(addr + $"&room-code={Id}"), cancelToken);
                }

                disconnected = false;
                reconnectDelay = TimeSpan.FromSeconds(baseReconnectDelay.TotalSeconds);
                retries = 0;

                Console.WriteLine("Connected");
                closeStatus = await SocketLoop();
            }
            catch (WebSocketException wse) {
                Console.WriteLine("WebSocket Exception Triggered");
                closeStatus.statusCode = WebSocketCloseStatus.ProtocolError;
                closeStatus.statusDescription = wse.Message;
                break;
            }
            catch (OperationCanceledException) {
                closeStatus.statusCode = WebSocketCloseStatus.EndpointUnavailable;
                closeStatus.statusDescription = "Client WebSocket Operations Canceled";
                break;
            }

            Console.WriteLine("Calling from Reconnect Loop");
            await CloseSocketAsync(closeStatus.statusCode, closeStatus.statusDescription);

            if (stopReconnecting) break;

            Console.WriteLine("Planning to Reconnect");
            await Task.Delay(reconnectDelay, cancelToken);

            retries++;
            reconnectDelay = TimeSpan.FromSeconds(
                Math.Min(
                         baseReconnectDelay.TotalSeconds * Math.Pow(2, retries) + 
                         randomDelay.Next(0, 1) * 0.5 * baseReconnectDelay.TotalSeconds, 
                         maxReconnectDelay.TotalSeconds
                        )
            );
        }

        return closeStatus;
    }

    /*
     * The primary WebSocket receive loop for Client.
     *
     * Handles incoming messages using the provided MessageHandler class.
     *
     * Handles closing socket connections only when the server initiates a closure,
     * as required from the WebSocket specification.
     *
     * Returns:
     *      A WebSocketCloseStatus with (some) information about how the connection
     *      ended on this side.
     *
     * NOTE:
     *      This function allocates memory equal to the buffer size provided by a developer upon
     *      class construction (default 4096 bytes). It is guaranteed to deallocate
     *      the memory upon completion of the function (errors are handled).
    */
    private async Task<CloseStatus> SocketLoop() {
        byte[] messageBuffer = ArrayPool<byte>.Shared.Rent(messageBufferSize);
        CloseStatus closeStatus = new CloseStatus(WebSocketCloseStatus.NormalClosure, "Normal Closure");

        try {
            while (socket.State == WebSocketState.Open && !cancelToken.IsCancellationRequested) {
                WebSocketReceiveResult result = await socket.ReceiveAsync(messageBuffer, cancelToken);

                if (result.MessageType == WebSocketMessageType.Close) {
                    closeStatus.statusDescription = "Close Frame Received";

                    if (result.CloseStatus != WebSocketCloseStatus.NormalClosure) {
                        closeStatus.statusCode = WebSocketCloseStatus.ProtocolError;
                        closeStatus.statusDescription = "Close Frame Error Acknowledged";
                    }

                    Console.WriteLine("Calling from Close Frame");
                    await CloseSocketAsync(closeStatus.statusCode, closeStatus.statusDescription);
                    break;
                }
                else if (result.MessageType == WebSocketMessageType.Text) {
                    closeStatus.statusCode = WebSocketCloseStatus.InvalidMessageType;
                    closeStatus.statusDescription = "Received WebSocket Message of Type Text, Not Binary";
                    break;
                }

                Message message = messenger.DecodeBinaryToMessage(messageBuffer);
                messageHandler.HandleMessage(message, this);
            }
        }
        finally {
            ArrayPool<byte>.Shared.Return(messageBuffer);
        }

        return closeStatus;
    }

    /*
     * A secondary method for sending Messages to a Server. Intended for compatibility with
     * IClientMessaging for sending Messages upon handling a specific message type in a MessageHandler,
     * and is not used internally in any meaningful way.
     *
     * NOTE:
     *      Unlike SocketLoop(), this function ALLOCATES AND DEALLOCATES BYTE MEMORY EVERY TIME A MESSAGE IS SENT.
     *      It is a repeatable operation, so losing a Message here is not unrecoverable, and YOU (yes, you,
     *      the developer reading this) are the only person who will be calling this function. It is recommended
     *      to make note of the sizes of messages you are expecting to send / receive, as well as the frequency
     *      with which you call this function. Otherwise, this could get out of hand.
    */
    public async Task SendMessageAsync(Message message) {
        byte[] messageBytes = messenger.SerializeMessage(message);

        try {
            await socket.SendAsync(messageBytes, WebSocketMessageType.Binary, true, cancelToken);
        }
        catch (WebSocketException) {
            await CloseSocketAsync(WebSocketCloseStatus.EndpointUnavailable, "Connection is Canceled");
        }
        catch (OperationCanceledException) {
            await CloseSocketAsync(WebSocketCloseStatus.EndpointUnavailable, "Connection is Canceled");
        }
    }

    /*
     * Close and dispose of the current connection to the Server.
     *
     * If we experienced a normal closure, or we are unable to connect to the server due to
     * it going away (or the Client going away), this function signals the Client to stop
     * reconnecting entirely.
     *
     * Parameters:
     *      closeStatus (WebSocketCloseStatus): The status of the WebSocket closure to send to the Server.
     *      description (string?): An optional description for information along with closing the WebSocket.
    */
    public async Task CloseSocketAsync(WebSocketCloseStatus closeStatus, string? description) {
        Console.WriteLine($"Initial Close Status is: {closeStatus}");
        await disconnectSemaphore.WaitAsync();

        if (disconnected) {
            Console.WriteLine("Saved a redundant call.");
            disconnectSemaphore.Release();
            return;
        }

        switch (socket.State) {
            case WebSocketState.Connecting:
                Console.WriteLine("Aborting");
                socket.Abort();
                break;

            case WebSocketState.Open:
                Console.WriteLine("Starting Closure");
                await socket.CloseAsync(closeStatus, description, CancellationToken.None);
                break;

            case WebSocketState.CloseReceived:
                Console.WriteLine("Close Received");
                await socket.CloseOutputAsync(closeStatus, description, CancellationToken.None);
                break;

            default:
                // Other states are Aborted, CloseSent, None, and Closed,
                // Aborted does not need to be handled as there's nothing to send a closure to.
                // CloseSent means we already sent a CloseAsync or CloseOutputAsync.
                // None shouldn't be possible.
                // And closed means we don't need to worry about this function at all.
                break;
        }

        disconnected = true;
        disconnectSemaphore.Release();

        // Frees up resources until we reconnect.
        socket.Dispose();

        Console.WriteLine($"Status After Disposal is: {closeStatus}");
        if (closeStatus == WebSocketCloseStatus.NormalClosure || closeStatus == WebSocketCloseStatus.EndpointUnavailable) {
            Console.WriteLine("Not Going to Reconnect Again");
            stopReconnecting = true;
        }
    }

    // Deallocates any non-WebSocket resources the Client needed to function.
    // ONLY CALL THIS WHEN YOU ARE COMPLETELY FINISHED WITH USING THE CLIENT.
    public void Free() {
        disconnectSemaphore.Dispose();
    }
}

}
