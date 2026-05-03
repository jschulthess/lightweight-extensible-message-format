package io.lxmf;

import io.lxmf.support.TestProvider;
import io.lxmf.rns.RNS;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link LXStamper} — workblock generation, stamp validation, and stamp generation.
 *
 * <p>All tests use WORKBLOCK_EXPAND_ROUNDS_PEERING (25 rounds) to stay fast.
 */
@DisplayName("LXStamper")
class LXStamperTest {

    @BeforeAll
    static void setup() {
        TestProvider.install();
    }

    private static final int FAST_ROUNDS = LXStamper.WORKBLOCK_EXPAND_ROUNDS_PEERING; // 25

    // ── stampWorkblock ────────────────────────────────────────────────────────

    @Nested
    @DisplayName("stampWorkblock")
    class WorkblockTests {

        @Test
        @DisplayName("deterministic — same input produces same workblock")
        void deterministic() {
            byte[] msgId = RNS.fullHash("test-msg-id".getBytes());
            byte[] wb1 = LXStamper.stampWorkblock(msgId, 1);
            byte[] wb2 = LXStamper.stampWorkblock(msgId, 1);
            assertArrayEquals(wb1, wb2);
        }

        @Test
        @DisplayName("correct length: expandRounds * 256 bytes")
        void correctLength() {
            byte[] msgId = RNS.fullHash("len-test".getBytes());
            byte[] wb = LXStamper.stampWorkblock(msgId, 1);
            assertEquals(256, wb.length);

            byte[] wb3 = LXStamper.stampWorkblock(msgId, 3);
            assertEquals(768, wb3.length);
        }

        @Test
        @DisplayName("different message ID → different workblock")
        void differentMessageId() {
            byte[] msgId1 = RNS.fullHash("message-a".getBytes());
            byte[] msgId2 = RNS.fullHash("message-b".getBytes());
            byte[] wb1 = LXStamper.stampWorkblock(msgId1, 1);
            byte[] wb2 = LXStamper.stampWorkblock(msgId2, 1);
            assertFalse(Arrays.equals(wb1, wb2));
        }
    }

    // ── stampValue / stampValid ────────────────────────────────────────────────

    @Nested
    @DisplayName("stampValue and stampValid")
    class StampValueTests {

        @Test
        @DisplayName("cost=0 always passes stampValid")
        void costZeroAlwaysValid() {
            byte[] msgId = RNS.randomBytes(32);
            byte[] wb = LXStamper.stampWorkblock(msgId, 1);
            byte[] anyStamp = RNS.randomBytes(LXStamper.STAMP_SIZE);
            assertTrue(LXStamper.stampValid(anyStamp, 0, wb));
        }

        @Test
        @DisplayName("all-zero stamp fails stampValid with cost=1 (overwhelmingly likely)")
        void allZeroStampFails() {
            byte[] msgId = RNS.randomBytes(32);
            byte[] wb = LXStamper.stampWorkblock(msgId, 1);
            byte[] zeroStamp = new byte[LXStamper.STAMP_SIZE]; // all zeros
            // P(valid) = 2^-1 = 0.5 per random wb, overwhelmingly likely to fail
            // We generate a fresh random wb for each attempt — we just test the logic works
            // There's a 1-in-2 chance of accidental pass; accept that by using a harder cost.
            assertFalse(LXStamper.stampValid(zeroStamp, 8, wb),
                    "All-zero stamp should almost never satisfy cost=8 — if this fails repeatedly, investigate");
        }

        @Test
        @DisplayName("stampValue of a valid stamp is >= target cost")
        void stampValueAtLeastCost() {
            byte[] msgId = RNS.fullHash("sv-test".getBytes());
            byte[] wb = LXStamper.stampWorkblock(msgId, FAST_ROUNDS);

            // Brute-force a valid stamp (cost=1, very fast)
            byte[] stamp = null;
            for (int i = 0; i < 10_000; i++) {
                byte[] candidate = RNS.randomBytes(LXStamper.STAMP_SIZE);
                if (LXStamper.stampValid(candidate, 1, wb)) {
                    stamp = candidate;
                    break;
                }
            }
            assertNotNull(stamp, "Could not find a valid stamp in 10000 tries (extremely unlikely)");
            int value = LXStamper.stampValue(wb, stamp);
            assertTrue(value >= 1, "stampValue should be >= 1 for a cost-1-valid stamp");
        }
    }

    // ── generateStamp ─────────────────────────────────────────────────────────

    @Nested
    @DisplayName("generateStamp (peering rounds, cost=1)")
    class GenerateStampTests {

        @Test
        @DisplayName("result is non-null")
        void resultNonNull() {
            byte[] msgId = RNS.fullHash("gen-test".getBytes());
            LXStamper.GeneratedStamp gs = LXStamper.generateStamp(msgId, 1, FAST_ROUNDS);
            assertNotNull(gs);
        }

        @Test
        @DisplayName("generated stamp passes stampValid")
        void stampPassesValidation() {
            byte[] msgId = RNS.fullHash("gen-valid".getBytes());
            LXStamper.GeneratedStamp gs = LXStamper.generateStamp(msgId, 1, FAST_ROUNDS);
            assertNotNull(gs);

            byte[] wb = LXStamper.stampWorkblock(msgId, FAST_ROUNDS);
            assertTrue(LXStamper.stampValid(gs.stamp, 1, wb));
        }

        @Test
        @DisplayName("stamp value >= 1")
        void stampValueAtLeastOne() {
            byte[] msgId = RNS.fullHash("gen-value".getBytes());
            LXStamper.GeneratedStamp gs = LXStamper.generateStamp(msgId, 1, FAST_ROUNDS);
            assertNotNull(gs);
            assertTrue(gs.value >= 1);
        }
    }

    // ── validatePeeringKey ────────────────────────────────────────────────────

    @Nested
    @DisplayName("validatePeeringKey")
    class ValidatePeeringKeyTests {

        @Test
        @DisplayName("a stamp generated with peering rounds passes validatePeeringKey")
        void validKey() {
            byte[] peeringId = RNS.fullHash("peering-id".getBytes());
            // generate a valid peering stamp with cost=1
            LXStamper.GeneratedStamp gs = LXStamper.generateStamp(peeringId, 1, FAST_ROUNDS);
            assertNotNull(gs);
            assertTrue(LXStamper.validatePeeringKey(peeringId, gs.stamp, 1));
        }

        @Test
        @DisplayName("a random byte array as key returns false")
        void randomKeyFails() {
            byte[] peeringId = RNS.fullHash("peering-id-2".getBytes());
            byte[] randomKey = RNS.randomBytes(LXStamper.STAMP_SIZE);
            // cost=8 makes a false positive astronomically unlikely
            assertFalse(LXStamper.validatePeeringKey(peeringId, randomKey, 8));
        }
    }
}
