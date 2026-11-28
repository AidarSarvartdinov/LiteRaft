package io.literaft.transport.nio;

/**
 * Tunable sizes for a socket connection.
 *
 * <p>{@code readBufferBytes} and {@code queueCapBytes} are local (not part of the
 * wire protocol). {@code maxPayloadBytes} is a wire contract and must be identical
 * on both peers and shared with the codec.
 */
public record SocketConfig(int readBufferBytes, int maxPayloadBytes, int queueCapBytes) {

    public static final SocketConfig DEFAULT =
        new SocketConfig(16 * 1024, 64 * 1024, 4 * (64 * 1024 + Integer.BYTES));

    public SocketConfig {
        if (readBufferBytes <= 0 || maxPayloadBytes <= 0)
            throw new IllegalArgumentException("sizes must be positive");
        if (queueCapBytes < maxPayloadBytes + Integer.BYTES)
            throw new IllegalArgumentException("queue cap must hold one max frame");
    }

    public int maxFrameBytes() { return maxPayloadBytes + Integer.BYTES; }
}