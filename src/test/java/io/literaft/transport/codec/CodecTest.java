package io.literaft.transport.codec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.literaft.transport.exception.ProtocolViolationException;

class CodecTest {
    private static final int TINY_MAX = 256;

    private Codec codec; // default config
    private Codec tiny; // configured max = TINY_MAX

    @BeforeEach
    void setUp() {
        codec = new Codec();
        tiny = new Codec(TINY_MAX);
    }

    // ---- helpers ----

    private static byte[] frame(byte[] payload) {
        ByteBuffer b = ByteBuffer.allocate(4 + payload.length);
        b.putInt(payload.length); b.put(payload);
        return b.array();
    }

    private static byte[] prefix(int len) {
        return ByteBuffer.allocate(4).putInt(len).array();
    }

    private static byte[] concat(byte[]... arrays) {
        int len = 0; for (byte[] a : arrays) len += a.length;
        byte[] out = new byte[len]; int off = 0;
        for (byte[] a : arrays) { System.arraycopy(a, 0, out, off, a.length); off += a.length; }
        return out;
    }

    private static byte[] slice(byte[] src, int from, int to) {
        return Arrays.copyOfRange(src, from, to);
    }

    private static void assertPayload(ByteBuffer b, byte[] expected) {
        assertEquals(0, b.position(), "buffer must be in read mode");
        assertEquals(expected.length, b.limit());
        byte[] got = new byte[b.remaining()];
        b.get(got);
        assertArrayEquals(expected, got);
    }

    // ---- encode ----

    @Test
    void encodeGoldenBytes() {
        byte[] payload = {41, 0, 21, 1, 115};
        assertArrayEquals(new byte[]{0, 0, 0, 5, 41, 0, 21, 1, 115}, codec.encode(payload));
    }

    @Test
    void encodeEmptyPayload() {
        assertArrayEquals(new byte[]{0, 0, 0, 0}, codec.encode(new byte[0]));
    }

    @Test
    void encodeNullThrows() {
        assertThrows(NullPointerException.class, () -> codec.encode(null));
    }

    /** Exactly the configured max is accepted. */
    @Test
    void encodeExactlyMaxSucceeds() {
        byte[] encoded = tiny.encode(new byte[TINY_MAX]);
        assertEquals(4 + TINY_MAX, encoded.length);
    }

    /** One byte over the configured max is rejected (tiny, no 16 MB allocation). */
    @Test
    void encodeJustOverMaxThrows() {
        assertThrows(ProtocolViolationException.class, () -> tiny.encode(new byte[TINY_MAX + 1]));
    }

    // ---- decode: basics ----

    @Test
    void decodeSingleFrame() {
        byte[] p = {1, 2, 3, 4, 5};
        List<ByteBuffer> out = codec.decode(frame(p));
        assertEquals(1, out.size());
        assertPayload(out.get(0), p);
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

    // ---- decode: tails across calls ----

    @Test
    void decodeLengthPrefixSplitAcrossCalls() {
        byte[] f = frame(new byte[]{9, 8, 7});
        assertTrue(codec.decode(slice(f, 0, 2)).isEmpty());
        assertPayload(codec.decode(slice(f, 2, f.length)).get(0), new byte[]{9, 8, 7});
    }

    @Test
    void decodePayloadSplitAcrossCalls() {
        byte[] f = frame(new byte[]{1, 2, 3, 4, 5, 6});
        int cut = 4 + 3;
        assertTrue(codec.decode(slice(f, 0, cut)).isEmpty());
        assertPayload(codec.decode(slice(f, cut, f.length)).get(0), new byte[]{1, 2, 3, 4, 5, 6});
    }

    @Test
    void decodePrefixEndsExactlyAtChunkBoundary() {
        byte[] f = frame(new byte[]{1, 2, 3});
        assertTrue(codec.decode(slice(f, 0, 4)).isEmpty());
        assertPayload(codec.decode(slice(f, 4, f.length)).get(0), new byte[]{1, 2, 3});
    }

    @Test
    void decodeByteByByte() {
        byte[] f = frame(new byte[]{5, 6, 7, 8});
        List<ByteBuffer> acc = new ArrayList<>();
        for (byte b : f) acc.addAll(codec.decode(new byte[]{b}));
        assertEquals(1, acc.size());
        assertPayload(acc.get(0), new byte[]{5, 6, 7, 8});
    }

    @Test
    void decodeSplitAtEveryOffset() {
        byte[] f = frame(new byte[]{1, 2, 3, 4, 5});
        for (int cut = 1; cut < f.length; cut++) {
            Codec c = new Codec();
            assertTrue(c.decode(slice(f, 0, cut)).isEmpty(), "cut=" + cut);
            assertPayload(c.decode(slice(f, cut, f.length)).get(0), new byte[]{1, 2, 3, 4, 5});
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

    // ---- decode: zero-length frames ----

    @Test
    void decodeZeroLengthFrameAlone() {
        List<ByteBuffer> out = codec.decode(new byte[]{0, 0, 0, 0});
        assertEquals(1, out.size());
        assertPayload(out.get(0), new byte[0]);
    }

    @Test
    void decodeZeroLengthFrameAtEndOfChunk() {
        List<ByteBuffer> out = codec.decode(concat(frame(new byte[]{1, 2}), new byte[]{0, 0, 0, 0}));
        assertEquals(2, out.size());
        assertPayload(out.get(1), new byte[0]);
    }

    @Test
    void decodeZeroLengthFrameFollowedByNextFrame() {
        List<ByteBuffer> out = codec.decode(concat(new byte[]{0, 0, 0, 0}, frame(new byte[]{3, 4})));
        assertEquals(2, out.size());
        assertPayload(out.get(0), new byte[0]);
        assertPayload(out.get(1), new byte[]{3, 4});
    }

    // ---- decode: protocol violations (tiny max) ----

    @Test
    void decodeNegativeLengthThrows() {
        assertThrows(ProtocolViolationException.class, () -> codec.decode(prefix(-1)));
    }

    @Test
    void decodeOverMaxLengthThrows() {
        assertThrows(ProtocolViolationException.class, () -> tiny.decode(prefix(TINY_MAX + 1)));
    }

    @Test
    void decodeAfterViolationDoesNotExplodeOnReuse() {
        assertThrows(ProtocolViolationException.class, () -> tiny.decode(prefix(TINY_MAX + 1)));
        assertPayload(tiny.decode(frame(new byte[]{1, 2, 3})).get(0), new byte[]{1, 2, 3});
    }

    // ---- decode(byte[], offset, length) overload ----

    /** Frame sitting in the middle of a bigger array; only the region is read. */
    @Test
    void decodeOffsetRegionInMiddleOfArray() {
        byte[] p = {1, 2, 3};
        byte[] f = frame(p);
        byte[] big = concat(new byte[]{9, 9, 9}, f, new byte[]{9, 9});
        List<ByteBuffer> out = codec.decode(big, 3, f.length);
        assertEquals(1, out.size());
        assertPayload(out.get(0), p);
    }

    /** Bytes outside the region must be ignored. */
    @Test
    void decodeIgnoresBytesOutsideRegion() {
        byte[] p = {1, 2, 3};
        byte[] f = frame(p);
        byte[] big = concat(f, frame(new byte[]{7, 7}));   // second frame outside region
        List<ByteBuffer> out = codec.decode(big, 0, f.length);
        assertEquals(1, out.size());
        assertPayload(out.get(0), p);
    }

    @Test
    void decodeZeroLengthRegionReturnsEmpty() {
        assertTrue(codec.decode(new byte[]{1, 2, 3}, 0, 0).isEmpty());
    }

    @Test
    void decodeOffsetBoundsThrow() {
        byte[] arr = new byte[8];
        assertThrows(IndexOutOfBoundsException.class, () -> codec.decode(arr, -1, 4));
        assertThrows(IndexOutOfBoundsException.class, () -> codec.decode(arr, 0, -1));
        assertThrows(IndexOutOfBoundsException.class, () -> codec.decode(arr, 4, 8));
    }

    /** Tail retention works with offset regions too. */
    @Test
    void decodeOffsetSplitAcrossCalls() {
        byte[] f = frame(new byte[]{1, 2, 3, 4});
        byte[] big = concat(new byte[]{9}, f);
        assertTrue(codec.decode(big, 1, 3).isEmpty());
        assertPayload(codec.decode(big, 4, f.length - 3).get(0), new byte[]{1, 2, 3, 4});
    }

    // ---- round trips ----

    @Test
    void roundTripEncodeDecode() {
        byte[] p = {10, 20, 30, 40};
        assertPayload(codec.decode(codec.encode(p)).get(0), p);
    }

    @Test
    void roundTripInRandomChunks() {
        byte[] p = new byte[TINY_MAX];
        for (int i = 0; i < p.length; i++) p[i] = (byte) (i % 251);
        byte[] f = tiny.encode(p);
        List<ByteBuffer> out = new ArrayList<>();
        int off = 0;
        Random rnd = new Random(42);
        while (off < f.length) {
            int n = Math.min(1 + rnd.nextInt(64), f.length - off);
            out.addAll(tiny.decode(slice(f, off, off + n)));
            off += n;
        }
        assertEquals(1, out.size());
        assertPayload(out.get(0), p);
    }

    // ---- constructor validation ----

    @Test
    void constructorRejectsNonPositiveMax() {
        assertThrows(IllegalArgumentException.class, () -> new Codec(0));
        assertThrows(IllegalArgumentException.class, () -> new Codec(-1));
    }
}