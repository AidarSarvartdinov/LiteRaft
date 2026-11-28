package io.literaft.transport.codec;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import io.literaft.transport.exception.ProtocolViolationException;

/**
 * Length-prefix framing codec for the LiteRaft wire protocol.
 *
 * <p>Frame layout: {@code [4-byte big-endian payload length][payload bytes]}.
 * The length prefix counts payload bytes only and excludes the prefix itself.
 *
 * <p>This class handles framing only. It never inspects payload contents:
 * message types and fields belong to the layer above (Raft). Changing the Raft
 * message set does not affect this class; changing the framing rule affects
 * only this class.
 *
 * <p><b>Ownership and lifecycle.</b> One instance belongs to exactly one
 * connection (one per {@code NioSocket}) and is confined to the event-loop
 * thread; it is not thread-safe. The instance is created with the connection
 * and discarded when the connection dies, which guarantees a fresh, empty tail
 * on reconnect. Never share an instance between connections: their tails would
 * interleave and corrupt both streams.
 *
 * <p><b>Failure policy.</b> A length prefix outside {@code [0, MAX_PAYLOAD_LEN_BYTES]}
 * is a protocol violation, not a transient condition. It is reported before any
 * allocation for the payload, so a malformed or hostile prefix cannot trigger a
 * large allocation. After a {@link ProtocolViolationException} the instance must
 * be discarded and the connection closed.
 *
 * <p><b>Buffer ownership.</b> Buffers returned by {@link #decode(byte[])} are
 * handed over to the caller; this class never reuses or mutates them afterwards.
 */
public class Codec {
    private static final int LENGTH_PREFIX_BYTES = Integer.BYTES;
    private static final List<ByteBuffer> EMPTY_LIST = List.of();
    
    private final int maxPayloadBytes;
    private ByteBuffer partialPayload = null;
    private final ByteBuffer lengthBuffer = ByteBuffer.allocate(LENGTH_PREFIX_BYTES);
    private int expectedPayloadLength = -1;

    public Codec() {
        this(64 * 1024);
    }

    public Codec(int maxPayloadBytes) {
        if (maxPayloadBytes <= 0) {
            throw new IllegalArgumentException("maxPayloadBytes must be positive");
        }
        this.maxPayloadBytes = maxPayloadBytes;
    }

    /**
     * Wraps a payload into a single wire frame.
     *
     * <p>The returned array is exactly {@code LENGTH_PREFIX_BYTES + payload.length}
     * bytes and is owned by the caller.
     *
     * @param payload serialized message bytes; may be empty
     * @return a newly allocated frame ready to be queued for writing
     * @throws NullPointerException if {@code payload} is null
     * @throws ProtocolViolationException if {@code payload} exceeds {@code MAX_PAYLOAD_LEN_BYTES}
     */
    public byte[] encode(byte[] payload) {
        if (payload == null) {
            throw new NullPointerException("payload is null");
        }

        if (payload.length > maxPayloadBytes) {
            throw new ProtocolViolationException("Message length is out of bounds: " + payload.length);
        }

        int payloadLength = payload.length;

        ByteBuffer buffer = ByteBuffer.allocate(LENGTH_PREFIX_BYTES + payloadLength);

        buffer.putInt(payloadLength);
        buffer.put(payload);

        return buffer.array();
    }

    /**
     * Consumes a chunk of bytes received from the socket and returns every frame
     * that became complete.
     *
     * <p>Any unfinished frame is retained internally and completed by later calls.
     * A single chunk may yield zero, one, or many messages.
     *
     * <p>Each returned buffer is in read mode. Ownership of the buffers and of the
     * returned list transfers to the caller. The returned list is for reading purposes only
     *
     * @param bytes bytes just read from the channel; may be empty
     * @return completed payloads in arrival order, or an empty list if none completed
     * @throws NullPointerException if {@code bytes} is null
     * @throws ProtocolViolationException if a length prefix is negative or exceeds
     *         {@code MAX_PAYLOAD_LEN_BYTES}; the instance must then be discarded
     */
    public List<ByteBuffer> decode(byte[] bytes) {
        if (bytes == null) {
            throw new NullPointerException("bytes is null");
        }

        return decode(bytes, 0, bytes.length);
    }

    /**
     * Consumes {@code length} valid bytes starting at {@code offset} and returns every
     * frame that became complete.
     *
     * <p>Only the region {@code [offset, offset + length)} is read; the rest of the
     * array is ignored, so a reusable read buffer can be passed without copying.
     * Any unfinished frame (partial length prefix or partial payload) is retained
     * internally and completed by later calls. A single call may yield zero, one or
     * many messages.
     *
     * <p>Each returned buffer is in read mode ({@code position == 0},
     * {@code limit == payload length}). Ownership of the buffers and of the returned
     * list transfers to the caller; the list is intended for reading/iteration only.
     *
     * @param bytes  backing array, e.g. a reusable read buffer; not null
     * @param offset start of the valid region, {@code >= 0}
     * @param length number of valid bytes, {@code >= 0}; {@code offset + length <= bytes.length}
     * @return completed payloads in arrival order, or an empty list if none completed
     * @throws NullPointerException if {@code bytes} is null
     * @throws IndexOutOfBoundsException if {@code offset < 0}, {@code length < 0} or
     *         {@code offset + length > bytes.length}
     * @throws ProtocolViolationException if a length prefix is negative or exceeds
     *         {@code MAX_PAYLOAD_LEN_BYTES}; the instance must then be discarded
     */
    public List<ByteBuffer> decode(byte[] bytes, int offset, int length) {
        if (bytes == null) {
            throw new NullPointerException("bytes is null");
        }
        
        if (offset < 0 || length < 0 || offset + length > bytes.length) {
            throw new IndexOutOfBoundsException("offset/length out of bounds");
        }

        if (length == 0) {
            return EMPTY_LIST;
        }

        List<ByteBuffer> fullMessages = null;
        int readOffset = offset;
        int end = offset + length;
        while (readOffset < end) {
            int payloadStart = readMessageLength(bytes, readOffset, end);

            if (payloadStart < 0) {
                break;
            }

            if (partialPayload == null) {
                partialPayload = ByteBuffer.allocate(expectedPayloadLength);
            }

            readOffset = payloadStart;
            int bytesToCopy = Math.min(
                end - readOffset,
                expectedPayloadLength - partialPayload.position());
            
            partialPayload.put(bytes, readOffset, bytesToCopy);
            readOffset += bytesToCopy;

            if (partialPayload.position() == partialPayload.limit()) {
                partialPayload.flip();

                if (fullMessages == null) fullMessages = new ArrayList<>();

                fullMessages.add(partialPayload);
                partialPayload = null;
                expectedPayloadLength = -1;
            }
        }

       return fullMessages == null ? EMPTY_LIST : fullMessages;        
    }

    /**
     * Attempts to finish reading the length prefix starting at {@code startIdx}.
     *
     * <p>Return value:
     * <ul>
     *   <li>{@code -1} — the prefix is still incomplete; more bytes are required;</li>
     *   <li>{@code >= 0} — the offset at which the payload of the current frame begins.
     *       It may equal {@code end} when the prefix ended exactly at the region
     *       boundary (payload not present in this chunk).</li>
     * </ul>
     *
     * <p>If the payload length was already known from a previous call, this method
     * returns {@code startIdx} unchanged without consuming any bytes.
     *
     * @param bytes    backing array
     * @param startIdx offset to start reading the prefix from
     * @param end      exclusive end of the valid region
     * @throws ProtocolViolationException if the decoded length is negative or exceeds
     *         {@code MAX_PAYLOAD_LEN_BYTES}; state is reset before throwing
     */
    private int readMessageLength(byte[] bytes, int startIdx, int end) {
        if (expectedPayloadLength >= 0 || startIdx >= end) {
            return startIdx;
        }

        int offset = startIdx;
        int bytesToCopy = Math.min(
            end - offset,
            LENGTH_PREFIX_BYTES - lengthBuffer.position()
        );
        lengthBuffer.put(bytes, offset, bytesToCopy);
        offset += bytesToCopy;

        if (lengthBuffer.position() == LENGTH_PREFIX_BYTES) {
            lengthBuffer.flip();
            expectedPayloadLength = lengthBuffer.getInt();
            lengthBuffer.clear();

            if (expectedPayloadLength < 0 || expectedPayloadLength > maxPayloadBytes) {
                int bad = expectedPayloadLength;
                expectedPayloadLength = -1;
                partialPayload = null;
                throw new ProtocolViolationException("Message length is out of bounds: " + bad);
            }

            return offset;
        }

        return -1;
    }
}
