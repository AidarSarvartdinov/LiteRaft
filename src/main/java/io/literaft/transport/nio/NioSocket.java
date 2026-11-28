package io.literaft.transport.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import io.literaft.transport.codec.Codec;
import io.literaft.transport.exception.ProtocolViolationException;

/**
 * Per-connection handler for a single non-blocking {@link SocketChannel}.
 *
 * <p>All methods are confined to the event-loop thread; the instance is not
 * thread-safe and is discarded when the connection dies.
 */
public class NioSocket {
    private static final List<Message> EMPTY_LIST = List.of();
    private final SocketConfig socketConfig;
    private final SocketChannel socketChannel;
    private final int peerId;
    private final Codec codec;
    private final ByteBuffer readBuffer;
    private final Queue<byte[]> outboundMessageQueue;
    private ByteBuffer currentWriteFrame;
    private int queuedBytes = 0;

    public NioSocket(SocketChannel socketChannel, int peerId) {
        this(socketChannel, peerId, SocketConfig.DEFAULT);
    }

    public NioSocket(SocketChannel socketChannel, int peerId, SocketConfig config) {
        this.socketChannel = socketChannel;
        this.peerId = peerId;
        this.socketConfig = config;
        this.codec = new Codec(config.maxPayloadBytes());
        this.readBuffer = ByteBuffer.allocate(config.readBufferBytes());
        this.outboundMessageQueue = new ArrayDeque<>();
    }
    
    /**
     * Completes a pending non-blocking connect.
     *
     * <p>Invoked by the event loop when the channel reports connectable.
     * On success the connection is ready for reading; on failure an
     * {@link IOException} is thrown so the caller can tear the connection down.
     *
     * @return true - if the connect completed
     * @throws IOException if the connect cannot be completed
     */
    public boolean onConnect() throws IOException {
        return socketChannel.finishConnect();
    }

    /**
     * Reads available bytes and returns every message that became complete.
     *
     * <p>Partial frames are retained by the codec and completed by later calls.
     * Each returned message carries this connection's peer id and a payload
     * buffer in read mode. Ownership of the returned list and buffers transfers
     * to the caller.
     *
     * @return completed messages in arrival order, or an empty list if none
     * @throws IOException on read failure or end-of-stream (peer closed)
     */
    public List<Message> onRead() throws IOException {
        int readBytes = socketChannel.read(readBuffer);

        if (readBytes == -1) {
            throw new IOException("peer closed connection (EOF), peer=" + peerId);
        }

        if (readBytes == 0) {
            return EMPTY_LIST;
        }

        readBuffer.flip();
        List<ByteBuffer> decodedMessages = codec.decode(readBuffer.array(), 0, readBytes);
        readBuffer.clear();
        
        List<Message> out = new ArrayList<>(decodedMessages.size());
        for (ByteBuffer payload : decodedMessages) {
            out.add(new Message(peerId, payload));
        }

        return out;
    }

    /**
     * Drains the outbound queue to the channel.
     *
     * <p>Writes the in-flight frame to completion before taking the next one,
     * preserving per-connection ordering. Releases the bytes of each fully
     * written frame from the outbound byte budget.
     *
     * @return {@code true} if the queue and in-flight frame are fully drained
     *         (the caller may clear {@code OP_WRITE}); {@code false} if the
     *         socket buffer is full and writing should resume on the next
     *         writable event (keep {@code OP_WRITE})
     * @throws IOException on write failure
     */
    public boolean onWrite() throws IOException {
        while (true) {
            if (currentWriteFrame != null && !currentWriteFrame.hasRemaining()) {
                queuedBytes -= currentWriteFrame.limit();
                currentWriteFrame = null;
            }
            if (currentWriteFrame == null) {
                byte[] next = outboundMessageQueue.poll();
                if (next == null) {
                    return true;
                }

                currentWriteFrame = ByteBuffer.wrap(next);
            }

            int written = socketChannel.write(currentWriteFrame);

            if (written == 0) {
                return false;
            }
        }
    }

    /**
     * Enqueues an already-framed message for writing.
     *
     * <p>Does not perform I/O; the event loop drains the queue via
     * {@link #onWrite()}. The caller must arm {@code OP_WRITE} after a
     * successful enqueue.
     *
     * @param framed framed bytes (length prefix + payload), not null
     * @return {@code true} if accepted; {@code false} if the outbound byte
     *         budget would be exceeded (backpressure)
     * @throws NullPointerException if {@code framed} is null
     * @throws ProtocolViolationException if the frame exceeds the maximum size
     */
    public boolean send(byte[] framed) {
        if (framed == null) {
            throw new NullPointerException("framed is null");
        }

        if (framed.length > socketConfig.maxFrameBytes()) {
            throw new ProtocolViolationException("frame too large: " + framed.length);
        }

        if (framed.length + queuedBytes > socketConfig.queueCapBytes()) {
            return false; // backpressure
        }
        
        outboundMessageQueue.offer(framed);
        queuedBytes += framed.length;
        return true;
    }

    public int getPeerId() {
        return peerId;
    }
}
