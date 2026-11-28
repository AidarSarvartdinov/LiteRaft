package io.literaft.transport.nio;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.literaft.transport.exception.ProtocolViolationException;

class NioSocketTest {
    private static final SocketConfig TINY = new SocketConfig(64, 256, 512);
    private static final SocketConfig BIG = new SocketConfig(16 * 1024, 64 * 1024, 4 * (64 * 1024 + 4));

    private ServerSocketChannel server;
    private SocketChannel client;
    private SocketChannel peer;
    private NioSocket socket;

    @BeforeEach
    void setUp() throws IOException {
        server = ServerSocketChannel.open();
        server.bind(new InetSocketAddress("127.0.0.1", 0));
        client = SocketChannel.open(server.getLocalAddress());
        peer = server.accept();
        client.configureBlocking(false);
        peer.configureBlocking(false);
        socket = new NioSocket(client, 1, TINY);
    }

    @AfterEach
    void tearDown() throws IOException {
        client.close(); peer.close(); server.close();
    }

    // ---- helpers ----

    private static byte[] frame(byte[] payload) {
        ByteBuffer b = ByteBuffer.allocate(4 + payload.length);
        b.putInt(payload.length); b.put(payload);
        return b.array();
    }

    private void peerWrite(byte[] bytes) throws IOException {
        ByteBuffer b = ByteBuffer.wrap(bytes);
        while (b.hasRemaining()) peer.write(b);
    }

    /** Push a frame from peer while pumping the read side; never blocks. */
    private List<Message> pumpRead(byte[] frameBytes) throws IOException {
        ByteBuffer wb = ByteBuffer.wrap(frameBytes);
        List<Message> acc = new ArrayList<>();
        while (wb.hasRemaining() || acc.isEmpty()) {
            if (wb.hasRemaining()) peer.write(wb);
            acc.addAll(socket.onRead());
        }
        return acc;
    }

    /** Pull n bytes at peer while pumping the write side; never blocks. */
    private byte[] pumpWrite(int n) throws IOException {
        byte[] got = new byte[n];
        ByteBuffer rb = ByteBuffer.wrap(got);
        while (rb.hasRemaining()) {
            socket.onWrite();
            peer.read(rb);
        }
        return got;
    }

    private static byte[] payloadOf(Message m) {
        ByteBuffer b = m.payload();
        byte[] out = new byte[b.remaining()];
        b.get(out);
        return out;
    }

    private static byte[] concat(byte[]... arr) {
        int n = 0; for (byte[] a : arr) n += a.length;
        byte[] out = new byte[n]; int o = 0;
        for (byte[] a : arr) { System.arraycopy(a, 0, out, o, a.length); o += a.length; }
        return out;
    }

    // ---- onRead ----

    @Test
    void readSingleFrame() throws IOException {
        byte[] p = {1, 2, 3};
        List<Message> out = pumpRead(frame(p));
        assertEquals(1, out.size());
        assertEquals(1, out.get(0).peerId());
        assertArrayEquals(p, payloadOf(out.get(0)));
    }

    @Test
    void readSplitFrameAcrossCalls() throws IOException {
        byte[] f = frame(new byte[]{1, 2, 3, 4, 5, 6});
        peerWrite(Arrays.copyOfRange(f, 0, 5));
        assertTrue(socket.onRead().isEmpty());
        peerWrite(Arrays.copyOfRange(f, 5, f.length));
        assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6}, payloadOf(socket.onRead().get(0)));
    }

    @Test
    void readTwoFramesInOneChunk() throws IOException {
        byte[] a = {1}, b = {2, 3};
        List<Message> out = pumpRead(concat(frame(a), frame(b)));
        assertEquals(2, out.size());
        assertArrayEquals(a, payloadOf(out.get(0)));
        assertArrayEquals(b, payloadOf(out.get(1)));
    }

    @Test
    void readNoDataReturnsEmpty() throws IOException {
        assertTrue(socket.onRead().isEmpty());
    }

    @Test
    void readEofThrows() throws IOException {
        peer.close();
        assertThrows(IOException.class, () -> socket.onRead());
    }

    /** Max payload exceeds tiny read buffer: reassembles across reads. */
    @Test
    void readFrameLargerThanReadBuffer() throws IOException {
        byte[] p = new byte[TINY.maxPayloadBytes()];
        new Random(7).nextBytes(p);
        List<Message> acc = pumpRead(frame(p));
        assertEquals(1, acc.size());
        assertArrayEquals(p, payloadOf(acc.get(0)));
    }

    @Test
    void twoSocketsDoNotCrossTails() throws IOException {
        SocketChannel c2 = SocketChannel.open(server.getLocalAddress());
        SocketChannel p2 = server.accept();
        c2.configureBlocking(false); p2.configureBlocking(false);
        NioSocket s2 = new NioSocket(c2, 2, TINY);

        byte[] fa = frame(new byte[]{1, 2, 3}), fb = frame(new byte[]{9, 8, 7});
        ByteBuffer wa = ByteBuffer.wrap(fa), wb = ByteBuffer.wrap(fb);
        List<Message> a = new ArrayList<>(), b = new ArrayList<>();
        while (wa.hasRemaining() || wb.hasRemaining() || a.isEmpty() || b.isEmpty()) {
            if (wa.hasRemaining()) peer.write(wa);
            if (wb.hasRemaining()) p2.write(wb);
            a.addAll(socket.onRead());
            b.addAll(s2.onRead());
        }
        assertArrayEquals(new byte[]{1, 2, 3}, payloadOf(a.get(0)));
        assertArrayEquals(new byte[]{9, 8, 7}, payloadOf(b.get(0)));
        c2.close(); p2.close();
    }

    // ---- onConnect ----

    @Test
    void onConnectCompletes() throws IOException {
        assertTrue(socket.onConnect());
    }

    // ---- send / onWrite ----

    @Test
    void writeEmptyQueueReturnsTrue() throws IOException {
        assertTrue(socket.onWrite());
    }

    @Test
    void writeSingleFrameArrivesIntact() throws IOException {
        byte[] f = frame(new byte[]{1, 2, 3});
        assertTrue(socket.send(f));
        assertTrue(socket.onWrite());
        assertArrayEquals(f, pumpWrite(f.length));
    }

    @Test
    void writePreservesOrder() throws IOException {
        byte[] f1 = frame(new byte[]{1}), f2 = frame(new byte[]{2}), f3 = frame(new byte[]{3});
        socket.send(f1); socket.send(f2); socket.send(f3);
        assertTrue(socket.onWrite());
        assertArrayEquals(concat(f1, f2, f3), pumpWrite(f1.length + f2.length + f3.length));
    }

    /** Max frame fits tiny cap once; second max frame rejected. */
    @Test
    void sendBackpressureAndRecovery() throws IOException {
        byte[] max = frame(new byte[TINY.maxPayloadBytes()]);
        assertTrue(socket.send(max));
        assertFalse(socket.send(max));
        pumpWrite(max.length); // free budget
        assertTrue(socket.send(max)); // accepted again
    }

    @Test
    void sendOversizeThrows() {
        assertThrows(ProtocolViolationException.class,
            () -> socket.send(frame(new byte[TINY.maxPayloadBytes() + 1])));
    }

    @Test
    void sendNullThrows() {
        assertThrows(NullPointerException.class, () -> socket.send(null));
    }

    // ---- partial write (big config + small socket buffers) ----

    @Test
    void writePartialThenResume() throws IOException {
        NioSocket big = new NioSocket(client, 1, BIG);
        client.setOption(StandardSocketOptions.SO_SNDBUF, 4096);
        peer.setOption(StandardSocketOptions.SO_RCVBUF, 4096);
        byte[] payload = new byte[BIG.maxPayloadBytes()];
        byte[] f = frame(payload);
        assertTrue(big.send(f));
        assertFalse(big.onWrite()); // socket full
        byte[] got = new byte[f.length];
        ByteBuffer rb = ByteBuffer.wrap(got);
        while (rb.hasRemaining()) {
            big.onWrite();
            peer.read(rb);
        }
        assertTrue(big.onWrite());
        assertArrayEquals(f, got);
    }

    // ---- config validation ----

    @Test
    void configRejectsCapSmallerThanMaxFrame() {
        assertThrows(IllegalArgumentException.class, () -> new SocketConfig(64, 256, 259));
    }

    @Test
    void configRejectsNonPositiveSizes() {
        assertThrows(IllegalArgumentException.class, () -> new SocketConfig(0, 256, 512));
        assertThrows(IllegalArgumentException.class, () -> new SocketConfig(64, 0, 512));
    }
}