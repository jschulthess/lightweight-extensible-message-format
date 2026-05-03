package io.lxmf;

import io.lxmf.rns.RNS;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the pure-Java crypto helpers in {@link RNS}.
 * None of these call {@code provider()}, so no initialization is needed.
 */
@DisplayName("RNS crypto helpers")
class RNSCryptoTest {

    // ── fullHash ──────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("fullHash")
    class FullHashTests {

        @Test
        @DisplayName("produces 32 bytes")
        void produces32Bytes() {
            byte[] hash = RNS.fullHash(new byte[]{1, 2, 3});
            assertEquals(32, hash.length);
        }

        @Test
        @DisplayName("is deterministic")
        void isDeterministic() {
            byte[] input = "hello world".getBytes();
            assertArrayEquals(RNS.fullHash(input), RNS.fullHash(input));
        }

        @Test
        @DisplayName("different inputs produce different hashes")
        void differentInputsDifferentHashes() {
            byte[] h1 = RNS.fullHash("foo".getBytes());
            byte[] h2 = RNS.fullHash("bar".getBytes());
            assertFalse(Arrays.equals(h1, h2));
        }
    }

    // ── truncatedHash ─────────────────────────────────────────────────────────

    @Nested
    @DisplayName("truncatedHash")
    class TruncatedHashTests {

        @Test
        @DisplayName("produces 16 bytes")
        void produces16Bytes() {
            assertEquals(16, RNS.truncatedHash("test".getBytes()).length);
        }

        @Test
        @DisplayName("equals first 16 bytes of fullHash")
        void equalsFirstHalfOfFullHash() {
            byte[] input = "data".getBytes();
            byte[] full = RNS.fullHash(input);
            byte[] trunc = RNS.truncatedHash(input);
            assertArrayEquals(Arrays.copyOf(full, 16), trunc);
        }
    }

    // ── hkdf ─────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("hkdf")
    class HkdfTests {

        @Test
        @DisplayName("produces the requested number of bytes")
        void correctLength() {
            byte[] out = RNS.hkdf(32, new byte[]{1, 2, 3}, null, null);
            assertEquals(32, out.length);
        }

        @Test
        @DisplayName("is deterministic")
        void isDeterministic() {
            byte[] ikm = "key-material".getBytes();
            byte[] salt = "salt".getBytes();
            byte[] ctx = "context".getBytes();
            assertArrayEquals(
                    RNS.hkdf(32, ikm, salt, ctx),
                    RNS.hkdf(32, ikm, salt, ctx));
        }

        @Test
        @DisplayName("different salt produces different output")
        void differentSaltDifferentOutput() {
            byte[] ikm = "key".getBytes();
            byte[] out1 = RNS.hkdf(32, ikm, "salt1".getBytes(), null);
            byte[] out2 = RNS.hkdf(32, ikm, "salt2".getBytes(), null);
            assertFalse(Arrays.equals(out1, out2));
        }

        @Test
        @DisplayName("different context produces different output")
        void differentContextDifferentOutput() {
            byte[] ikm = "key".getBytes();
            byte[] salt = "salt".getBytes();
            byte[] out1 = RNS.hkdf(32, ikm, salt, "ctx1".getBytes());
            byte[] out2 = RNS.hkdf(32, ikm, salt, "ctx2".getBytes());
            assertFalse(Arrays.equals(out1, out2));
        }

        @Test
        @DisplayName("null salt is treated as 32 zero bytes")
        void nullSaltUsesZeroBytes() {
            byte[] ikm = "ikm".getBytes();
            byte[] withNull = RNS.hkdf(32, ikm, null, null);
            byte[] withZeros = RNS.hkdf(32, ikm, new byte[32], null);
            assertArrayEquals(withNull, withZeros);
        }
    }

    // ── randomBytes ──────────────────────────────────────────────────────────

    @Nested
    @DisplayName("randomBytes")
    class RandomBytesTests {

        @Test
        @DisplayName("produces the requested length")
        void correctLength() {
            assertEquals(32, RNS.randomBytes(32).length);
            assertEquals(16, RNS.randomBytes(16).length);
        }

        @Test
        @DisplayName("not all zeros for a 32-byte request")
        void notAllZeros() {
            byte[] r = RNS.randomBytes(32);
            boolean allZero = true;
            for (byte b : r) if (b != 0) { allZero = false; break; }
            assertFalse(allZero, "randomBytes returned all-zero array (astronomically unlikely)");
        }
    }

    // ── prettyhexrep ──────────────────────────────────────────────────────────

    @Nested
    @DisplayName("prettyhexrep")
    class PrettyhexrepTests {

        @Test
        @DisplayName("wraps hex in angle brackets")
        void wrapsInBrackets() {
            byte[] bytes = new byte[]{(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef};
            assertEquals("<deadbeef>", RNS.prettyhexrep(bytes));
        }

        @Test
        @DisplayName("null input returns <null>")
        void nullInput() {
            assertEquals("<null>", RNS.prettyhexrep(null));
        }

        @Test
        @DisplayName("empty array returns <>")
        void emptyArray() {
            assertEquals("<>", RNS.prettyhexrep(new byte[0]));
        }
    }

    // ── prettytime ────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("prettytime")
    class PrettytimeTests {

        @Test
        @DisplayName("sub-minute shows seconds with 2 decimals")
        void subMinute() {
            String s = RNS.prettytime(30.5);
            assertTrue(s.endsWith("s"), "Expected seconds suffix, got: " + s);
        }

        @Test
        @DisplayName("minutes range shows minutes with 1 decimal")
        void minutes() {
            String s = RNS.prettytime(120.0);
            assertTrue(s.endsWith("m"), "Expected minutes suffix, got: " + s);
        }

        @Test
        @DisplayName("hours range shows hours with 1 decimal")
        void hours() {
            String s = RNS.prettytime(7200.0);
            assertTrue(s.endsWith("h"), "Expected hours suffix, got: " + s);
        }

        @Test
        @DisplayName("days range shows days with 1 decimal")
        void days() {
            String s = RNS.prettytime(172800.0);
            assertTrue(s.endsWith("d"), "Expected days suffix, got: " + s);
        }
    }

    // ── prettysize ────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("prettysize")
    class PrettysizeTests {

        @Test
        @DisplayName("bytes range")
        void bytes() {
            assertTrue(RNS.prettysize(500).endsWith(" B"));
        }

        @Test
        @DisplayName("kilobytes range")
        void kilobytes() {
            assertTrue(RNS.prettysize(5_000).endsWith("KB"));
        }

        @Test
        @DisplayName("megabytes range")
        void megabytes() {
            assertTrue(RNS.prettysize(5_000_000).endsWith("MB"));
        }

        @Test
        @DisplayName("gigabytes range")
        void gigabytes() {
            assertTrue(RNS.prettysize(5_000_000_000L).endsWith("GB"));
        }
    }

    // ── concat ────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("concat")
    class ConcatTests {

        @Test
        @DisplayName("length is sum of parts")
        void correctLength() {
            byte[] a = {1, 2, 3};
            byte[] b = {4, 5};
            assertEquals(5, RNS.concat(a, b).length);
        }

        @Test
        @DisplayName("content is a followed by b")
        void correctContent() {
            byte[] a = {1, 2, 3};
            byte[] b = {4, 5};
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, RNS.concat(a, b));
        }

        @Test
        @DisplayName("concat with empty array is identity")
        void withEmptyArray() {
            byte[] a = {1, 2};
            assertArrayEquals(a, RNS.concat(a, new byte[0]));
            assertArrayEquals(a, RNS.concat(new byte[0], a));
        }
    }
}
