package io.lxmf;

import io.lxmf.support.TestProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link LXMF} announce-data parsing helpers.
 */
@DisplayName("LXMF announce-data parsing")
class LXMFTest {

    @BeforeAll
    static void setup() {
        TestProvider.install();
    }

    // ── Helper: build a minimal new-format delivery announce payload ──────────

    /**
     * Builds a v0.5+ delivery announce app_data:
     * msgpack [nameBytes, stampCost, [SF_COMPRESSION]]
     */
    private static byte[] newFormatAppData(String name, Integer stampCost,
                                            boolean includeCompression) throws Exception {
        MessageBufferPacker p = MessagePack.newDefaultBufferPacker();
        p.packArrayHeader(3);
        // element 0: display name bytes
        if (name == null) {
            p.packNil();
        } else {
            byte[] nb = name.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            p.packBinaryHeader(nb.length);
            p.writePayload(nb);
        }
        // element 1: stamp cost
        if (stampCost == null) {
            p.packNil();
        } else {
            p.packInt(stampCost);
        }
        // element 2: functionality list
        if (includeCompression) {
            p.packArrayHeader(1);
            p.packInt(LXMF.SF_COMPRESSION);
        } else {
            p.packArrayHeader(0);
        }
        return p.toByteArray();
    }

    /**
     * Builds a propagation-node announce payload:
     * [false, 1234567890L, true, 256L, 10240L, [16, 3, 18], {}]
     */
    private static byte[] validPnPayload() throws Exception {
        MessageBufferPacker p = MessagePack.newDefaultBufferPacker();
        p.packArrayHeader(7);
        p.packBoolean(false);          // 0: legacy flag
        p.packLong(1234567890L);       // 1: timebase
        p.packBoolean(true);           // 2: propagation enabled
        p.packLong(256L);              // 3: transfer limit
        p.packLong(10240L);            // 4: sync limit
        p.packArrayHeader(3);          // 5: [stamp_cost, flexibility, peering_cost]
        p.packLong(16L);
        p.packLong(3L);
        p.packLong(18L);
        p.packMapHeader(0);            // 6: metadata map (empty)
        return p.toByteArray();
    }

    // ── displayNameFromAppData ────────────────────────────────────────────────

    @Nested
    @DisplayName("displayNameFromAppData")
    class DisplayNameTests {

        @Test
        @DisplayName("null → null")
        void nullInput() {
            assertNull(LXMF.displayNameFromAppData(null));
        }

        @Test
        @DisplayName("empty → null")
        void emptyInput() {
            assertNull(LXMF.displayNameFromAppData(new byte[0]));
        }

        @Test
        @DisplayName("legacy raw UTF-8 → the string")
        void legacyUtf8() {
            byte[] appData = "Alice".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            assertEquals("Alice", LXMF.displayNameFromAppData(appData));
        }

        @Test
        @DisplayName("new format with name → the name")
        void newFormatWithName() throws Exception {
            assertEquals("Alice", LXMF.displayNameFromAppData(newFormatAppData("Alice", 16, true)));
        }

        @Test
        @DisplayName("new format with nil name → null")
        void newFormatNilName() throws Exception {
            assertNull(LXMF.displayNameFromAppData(newFormatAppData(null, 16, true)));
        }
    }

    // ── stampCostFromAppData ──────────────────────────────────────────────────

    @Nested
    @DisplayName("stampCostFromAppData")
    class StampCostTests {

        @Test
        @DisplayName("null → null")
        void nullInput() {
            assertNull(LXMF.stampCostFromAppData(null));
        }

        @Test
        @DisplayName("legacy UTF-8 → null (no cost in legacy format)")
        void legacyFormat() {
            byte[] appData = "Alice".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            assertNull(LXMF.stampCostFromAppData(appData));
        }

        @Test
        @DisplayName("new format with cost 16 → 16")
        void newFormatWithCost() throws Exception {
            assertEquals(16, LXMF.stampCostFromAppData(newFormatAppData("Alice", 16, true)));
        }

        @Test
        @DisplayName("new format with nil cost → null")
        void newFormatNilCost() throws Exception {
            assertNull(LXMF.stampCostFromAppData(newFormatAppData("Alice", null, true)));
        }
    }

    // ── compressionSupportFromAppData ─────────────────────────────────────────

    @Nested
    @DisplayName("compressionSupportFromAppData")
    class CompressionSupportTests {

        @Test
        @DisplayName("null → false")
        void nullInput() {
            assertFalse(LXMF.compressionSupportFromAppData(null));
        }

        @Test
        @DisplayName("empty → false")
        void emptyInput() {
            assertFalse(LXMF.compressionSupportFromAppData(new byte[0]));
        }

        @Test
        @DisplayName("legacy UTF-8 → true (default)")
        void legacyFormat() {
            byte[] appData = "Alice".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            assertTrue(LXMF.compressionSupportFromAppData(appData));
        }

        @Test
        @DisplayName("new format with SF_COMPRESSION → true")
        void newFormatWithCompression() throws Exception {
            assertTrue(LXMF.compressionSupportFromAppData(newFormatAppData("Alice", 16, true)));
        }

        @Test
        @DisplayName("new format without SF_COMPRESSION → false")
        void newFormatWithoutCompression() throws Exception {
            assertFalse(LXMF.compressionSupportFromAppData(newFormatAppData("Alice", 16, false)));
        }
    }

    // ── pnAnnounceDataIsValid ─────────────────────────────────────────────────

    @Nested
    @DisplayName("pnAnnounceDataIsValid")
    class PnAnnounceDataIsValidTests {

        @Test
        @DisplayName("valid 7-element array → true")
        void validPayload() throws Exception {
            assertTrue(LXMF.pnAnnounceDataIsValid(validPnPayload()));
        }

        @Test
        @DisplayName("null → false")
        void nullInput() {
            assertFalse(LXMF.pnAnnounceDataIsValid(null));
        }

        @Test
        @DisplayName("too-short array (< 7 elements) → false")
        void tooShort() throws Exception {
            MessageBufferPacker p = MessagePack.newDefaultBufferPacker();
            p.packArrayHeader(3);
            p.packBoolean(false);
            p.packLong(1234L);
            p.packBoolean(true);
            assertFalse(LXMF.pnAnnounceDataIsValid(p.toByteArray()));
        }

        @Test
        @DisplayName("wrong type at position 1 (non-integer timebase) → false")
        void wrongTypeAtPosition1() throws Exception {
            MessageBufferPacker p = MessagePack.newDefaultBufferPacker();
            p.packArrayHeader(7);
            p.packBoolean(false);
            p.packString("not-an-integer");   // position 1 must be integer
            p.packBoolean(true);
            p.packLong(256L);
            p.packLong(10240L);
            p.packArrayHeader(3);
            p.packLong(16L); p.packLong(3L); p.packLong(18L);
            p.packMapHeader(0);
            assertFalse(LXMF.pnAnnounceDataIsValid(p.toByteArray()));
        }
    }

    // ── pnStampCostFromAppData ────────────────────────────────────────────────

    @Nested
    @DisplayName("pnStampCostFromAppData")
    class PnStampCostTests {

        @Test
        @DisplayName("valid payload → 16")
        void validPayload() throws Exception {
            assertEquals(16, LXMF.pnStampCostFromAppData(validPnPayload()));
        }

        @Test
        @DisplayName("null → null")
        void nullInput() {
            assertNull(LXMF.pnStampCostFromAppData(null));
        }
    }

    // ── pnNameFromAppData ─────────────────────────────────────────────────────

    @Nested
    @DisplayName("pnNameFromAppData")
    class PnNameTests {

        @Test
        @DisplayName("payload without PN_META_NAME → null")
        void noNameEntry() throws Exception {
            // valid payload but empty metadata map → no name
            assertNull(LXMF.pnNameFromAppData(validPnPayload()));
        }

        @Test
        @DisplayName("null → null")
        void nullInput() {
            assertNull(LXMF.pnNameFromAppData(null));
        }

        @Test
        @DisplayName("payload with PN_META_NAME → the name")
        void withName() throws Exception {
            MessageBufferPacker p = MessagePack.newDefaultBufferPacker();
            p.packArrayHeader(7);
            p.packBoolean(false);
            p.packLong(1234567890L);
            p.packBoolean(true);
            p.packLong(256L);
            p.packLong(10240L);
            p.packArrayHeader(3);
            p.packLong(16L); p.packLong(3L); p.packLong(18L);
            // metadata map with PN_META_NAME
            p.packMapHeader(1);
            p.packInt(LXMF.PN_META_NAME);
            byte[] nameBytes = "TestNode".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            p.packBinaryHeader(nameBytes.length);
            p.writePayload(nameBytes);
            assertEquals("TestNode", LXMF.pnNameFromAppData(p.toByteArray()));
        }
    }
}
