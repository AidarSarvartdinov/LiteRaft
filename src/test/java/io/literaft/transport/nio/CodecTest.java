package io.literaft.transport.nio;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.literaft.transport.codec.Codec;
import io.literaft.transport.exception.ProtocolViolationException;


class CodecTest {

    private Codec codec;
    
    @BeforeEach
    void setUp() {
        codec = new Codec();
    }

    private static byte[] frame(byte[] payload) {
        ByteBuffer b = ByteBuffer.allocate(4 + payload.length);
        b.putInt(payload.length);
        b.put(payload);
        return b.array();
    }

    private static byte[] concat(byte[]... arrays) {
        int len = 0;
        for (byte[] a : arrays) len += a.length;
        byte[] out = new byte[len];
        int off = 0;
        for (byte[] a : arrays) {
            System.arraycopy(a, 0, out, off, a.length);
            off += a.length;
        }
        return out;
    }

    private static byte[] slice(byte[] src, int from, int to) {
        byte[] out = new byte[to - from];
        System.arraycopy(src, from, out, 0, to - from);
        return out;
    }

    private static void assertPayload(ByteBuffer b, byte[] expected) {
        assertEquals(0, b.position(), "buffer must be in read mode");
        assertEquals(expected.length, b.limit());
        assertEquals(expected.length, b.remaining());
        byte[] got = new byte[b.remaining()];
        b.get(got);
        assertArrayEquals(expected, got);
    }

    @Test 
    void encodeTest() {
        byte[] payload = {41, 0, 21, 1, 115};
        byte[] expected = {0, 0, 0, 5, 41, 0, 21, 1, 115};
        assertArrayEquals(expected, Codec.encode(payload));      
    }

    @Test 
    void emptyPayloadEncodeTest() {
        byte[] emptyPayload = {};
        byte[] expected = {0, 0, 0, 0};
        assertArrayEquals(expected, Codec.encode(emptyPayload));
    }

    @Test 
    void nullPayloadEncodeTest() {
        assertThrows(NullPointerException.class, () -> Codec.encode(null));
    }

    @Test
    void maxPayloadEncodeTest() {
        byte[] payload = new byte[16 * 1024 * 1024];
        byte[] encoded = Codec.encode(payload);
        assertEquals(4 + payload.length, encoded.length);
    }

    @Test
    void justOverMaxPayloadEncodeTest() {
        byte[] payload = new byte[16 * 1024 * 1024 + 1];
        assertThrows(ProtocolViolationException.class, () -> Codec.encode(payload));
    }

    @Test
    void decodeSingleFrame() {
        byte[] payload = {1, 2, 3, 4, 5};
        List<ByteBuffer> out = codec.decode(frame(payload));
        assertEquals(1, out.size());
        assertPayload(out.get(0), payload);
    }

    @Test
    void decodeEmptyChunkReturnsEmpty() {
        assertTrue(codec.decode(new byte[0]).isEmpty());
    }

    @Test
    void decodeNullThrows() {
        assertThrows(NullPointerException.class, () -> codec.decode(null));
    }

    @Test
    void decodeTwoFramesInOneChunk() {
        byte[] a = {1, 2}, b = {3, 4, 5};
        List<ByteBuffer> out = codec.decode(concat(frame(a), frame(b)));
        assertEquals(2, out.size());
        assertPayload(out.get(0), a);
        assertPayload(out.get(1), b);
    }


    @Test
    void decodeLengthPrefixSplitAcrossCalls() {
        byte[] payload = {9, 8, 7};
        byte[] f = frame(payload);
        
        assertTrue(codec.decode(slice(f, 0, 2)).isEmpty());
        List<ByteBuffer> out = codec.decode(slice(f, 2, f.length));
        assertEquals(1, out.size());
        assertPayload(out.get(0), payload);
    }

    @Test
    void decodePayloadSplitAcrossCalls() {
        byte[] payload = {1, 2, 3, 4, 5, 6};
        byte[] f = frame(payload);
        int cut = 4 + 3;
        assertTrue(codec.decode(slice(f, 0, cut)).isEmpty());
        List<ByteBuffer> out = codec.decode(slice(f, cut, f.length));
        assertEquals(1, out.size());
        assertPayload(out.get(0), payload);
    }

    @Test
    void decodePrefixEndsExactlyAtChunkBoundary() {
        byte[] payload = {1, 2, 3};
        byte[] f = frame(payload);
  
        assertTrue(codec.decode(slice(f, 0, 4)).isEmpty());
        List<ByteBuffer> out = codec.decode(slice(f, 4, f.length));
        assertEquals(1, out.size());
        assertPayload(out.get(0), payload);
    }

    @Test
    void decodeByteByByte() {
        byte[] payload = {5, 6, 7, 8};
        byte[] f = frame(payload);
        List<ByteBuffer> collected = new ArrayList<>();
        for (int i = 0; i < f.length; i++) {
            collected.addAll(codec.decode(new byte[]{f[i]}));
        }
        assertEquals(1, collected.size());
        assertPayload(collected.get(0), payload);
    }

    @Test
    void decodeSplitAtEveryOffset() {
        byte[] payload = {1, 2, 3, 4, 5};
        byte[] f = frame(payload);
        for (int cut = 1; cut < f.length; cut++) {
            Codec c = new Codec();
            List<ByteBuffer> first = c.decode(slice(f, 0, cut));
            List<ByteBuffer> second = c.decode(slice(f, cut, f.length));
            assertTrue(first.isEmpty(), "cut=" + cut + " must not complete early");
            assertEquals(1, second.size(), "cut=" + cut);
            assertPayload(second.get(0), payload);
        }
    }

    @Test
    void decodeTwoFramesSplitAtEveryOffset() {
        byte[] a = {1, 2}, b = {3, 4, 5};
        byte[] stream = concat(frame(a), frame(b));
        for (int cut = 1; cut < stream.length; cut++) {
            Codec c = new Codec();
            List<ByteBuffer> out = new ArrayList<>();
            out.addAll(c.decode(slice(stream, 0, cut)));
            out.addAll(c.decode(slice(stream, cut, stream.length)));
            assertEquals(2, out.size(), "cut=" + cut);
            assertPayload(out.get(0), a);
            assertPayload(out.get(1), b);
        }
    }


    @Test
    void decodeZeroLengthFrameAlone() {
        List<ByteBuffer> out = codec.decode(new byte[]{0, 0, 0, 0});
        assertEquals(1, out.size());
        assertPayload(out.get(0), new byte[0]);
    }

    @Test
    void decodeZeroLengthFrameAtEndOfChunk() {
        byte[] a = {1, 2};
        List<ByteBuffer> out = codec.decode(concat(frame(a), new byte[]{0, 0, 0, 0}));
        assertEquals(2, out.size());
        assertPayload(out.get(0), a);
        assertPayload(out.get(1), new byte[0]);
    }

    @Test
    void decodeZeroLengthFrameFollowedByNextFrame() {
        byte[] b = {3, 4};
        List<ByteBuffer> out = codec.decode(concat(new byte[]{0, 0, 0, 0}, frame(b)));
        assertEquals(2, out.size());
        assertPayload(out.get(0), new byte[0]);
        assertPayload(out.get(1), b);
    }


    @Test
    void decodeNegativeLengthThrows() {
        byte[] bad = ByteBuffer.allocate(4).putInt(-1).array();
        assertThrows(ProtocolViolationException.class, () -> codec.decode(bad));
    }

    @Test
    void decodeOverMaxLengthThrows() {
        ByteBuffer b = ByteBuffer.allocate(4);
        b.putInt(16 * 1024 * 1024 + 1);
        assertThrows(ProtocolViolationException.class, () -> codec.decode(b.array()));
    }

    @Test
    void decodeAfterViolationDoesNotExplodeOnReuse() {
        ByteBuffer bad = ByteBuffer.allocate(4);
        bad.putInt(16 * 1024 * 1024 + 1);
        assertThrows(ProtocolViolationException.class, () -> codec.decode(bad.array()));

        byte[] payload = {1, 2, 3};
        List<ByteBuffer> out = codec.decode(frame(payload));
        assertEquals(1, out.size());
        assertPayload(out.get(0), payload);
    }


    @Test
    void roundTripEncodeDecode() {
        byte[] payload = {10, 20, 30, 40};
        List<ByteBuffer> out = codec.decode(Codec.encode(payload));
        assertEquals(1, out.size());
        assertPayload(out.get(0), payload);
    }

    @Test
    void roundTripLargePayloadInRandomChunks() {
        byte[] payload = new byte[1024 * 1024];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i % 251);
        byte[] f = Codec.encode(payload);

        List<ByteBuffer> out = new ArrayList<>();
        int off = 0;
        java.util.Random rnd = new java.util.Random(42);
        while (off < f.length) {
            int n = Math.min(1 + rnd.nextInt(64 * 1024), f.length - off);
            out.addAll(codec.decode(slice(f, off, off + n)));
            off += n;
        }
        assertEquals(1, out.size());
        assertPayload(out.get(0), payload);
    }
}
