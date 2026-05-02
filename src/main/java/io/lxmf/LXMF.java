package io.lxmf;

import io.lxmf.rns.RNS;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.util.List;

/**
 * LXMF constants and wire-level helper functions.
 *
 * <p>Field-type constants, audio modes, renderer specs, propagation-node metadata tags, and
 * supported-functionality codes match the Python reference implementation exactly, ensuring
 * binary interoperability.
 */
public final class LXMF {

    public static final String VERSION  = "0.9.6";
    public static final String APP_NAME = "lxmf";

    private LXMF() {}

    // ── Core message field types ──────────────────────────────────────────────

    public static final int FIELD_EMBEDDED_LXMS    = 0x01;
    public static final int FIELD_TELEMETRY        = 0x02;
    public static final int FIELD_TELEMETRY_STREAM = 0x03;
    public static final int FIELD_ICON_APPEARANCE  = 0x04;
    public static final int FIELD_FILE_ATTACHMENTS = 0x05;
    public static final int FIELD_IMAGE            = 0x06;
    public static final int FIELD_AUDIO            = 0x07;
    public static final int FIELD_THREAD           = 0x08;
    public static final int FIELD_COMMANDS         = 0x09;
    public static final int FIELD_RESULTS          = 0x0A;
    public static final int FIELD_GROUP            = 0x0B;
    public static final int FIELD_TICKET           = 0x0C;
    public static final int FIELD_EVENT            = 0x0D;
    public static final int FIELD_RNR_REFS         = 0x0E;
    public static final int FIELD_RENDERER         = 0x0F;

    public static final int FIELD_CUSTOM_TYPE      = 0xFB;
    public static final int FIELD_CUSTOM_DATA      = 0xFC;
    public static final int FIELD_CUSTOM_META      = 0xFD;
    public static final int FIELD_NON_SPECIFIC     = 0xFE;
    public static final int FIELD_DEBUG            = 0xFF;

    // ── Audio modes for FIELD_AUDIO ───────────────────────────────────────────

    public static final int AM_CODEC2_450PWB  = 0x01;
    public static final int AM_CODEC2_450     = 0x02;
    public static final int AM_CODEC2_700C    = 0x03;
    public static final int AM_CODEC2_1200    = 0x04;
    public static final int AM_CODEC2_1300    = 0x05;
    public static final int AM_CODEC2_1400    = 0x06;
    public static final int AM_CODEC2_1600    = 0x07;
    public static final int AM_CODEC2_2400    = 0x08;
    public static final int AM_CODEC2_3200    = 0x09;

    public static final int AM_OPUS_OGG       = 0x10;
    public static final int AM_OPUS_LBW       = 0x11;
    public static final int AM_OPUS_MBW       = 0x12;
    public static final int AM_OPUS_PTT       = 0x13;
    public static final int AM_OPUS_RT_HDX    = 0x14;
    public static final int AM_OPUS_RT_FDX    = 0x15;
    public static final int AM_OPUS_STANDARD  = 0x16;
    public static final int AM_OPUS_HQ        = 0x17;
    public static final int AM_OPUS_BROADCAST = 0x18;
    public static final int AM_OPUS_LOSSLESS  = 0x19;

    public static final int AM_CUSTOM         = 0xFF;

    // ── Message renderer specifications for FIELD_RENDERER ───────────────────

    public static final int RENDERER_PLAIN    = 0x00;
    public static final int RENDERER_MICRON   = 0x01;
    public static final int RENDERER_MARKDOWN = 0x02;
    public static final int RENDERER_BBCODE   = 0x03;

    // ── Propagation-node metadata field keys ──────────────────────────────────

    public static final int PN_META_VERSION       = 0x00;
    public static final int PN_META_NAME          = 0x01;
    public static final int PN_META_SYNC_STRATUM  = 0x02;
    public static final int PN_META_SYNC_THROTTLE = 0x03;
    public static final int PN_META_AUTH_BAND     = 0x04;
    public static final int PN_META_UTIL_PRESSURE = 0x05;
    public static final int PN_META_CUSTOM        = 0xFF;

    // ── Supported-functionality codes ─────────────────────────────────────────

    public static final int SF_COMPRESSION = 0x00;

    // ── Helper functions ──────────────────────────────────────────────────────

    /**
     * Extract a display name from LXMF delivery announce app_data.
     * Handles both the legacy (raw UTF-8) and v0.5.0+ (msgpack list) formats.
     */
    public static String displayNameFromAppData(byte[] appData) {
        if (appData == null || appData.length == 0) return null;
        try {
            int first = appData[0] & 0xFF;
            if ((first >= 0x90 && first <= 0x9f) || first == 0xdc) {
                // v0.5.0+ format: [display_name_bytes, stamp_cost, ...]
                try (MessageUnpacker up = MessagePack.newDefaultUnpacker(appData)) {
                    int size = up.unpackArrayHeader();
                    if (size < 1) return null;
                    if (up.getNextFormat() == org.msgpack.core.MessageFormat.NIL) {
                        up.unpackNil();
                        return null;
                    }
                    byte[] dn = up.readPayload(up.unpackBinaryHeader());
                    return new String(dn, java.nio.charset.StandardCharsets.UTF_8);
                }
            } else {
                return new String(appData, java.nio.charset.StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Extract the stamp cost from LXMF delivery announce app_data.
     * Returns null if not present or on parse error.
     */
    public static Integer stampCostFromAppData(byte[] appData) {
        if (appData == null || appData.length == 0) return null;
        try {
            int first = appData[0] & 0xFF;
            if ((first >= 0x90 && first <= 0x9f) || first == 0xdc) {
                try (MessageUnpacker up = MessagePack.newDefaultUnpacker(appData)) {
                    int size = up.unpackArrayHeader();
                    if (size < 2) return null;
                    // skip element 0 (display name)
                    up.skipValue();
                    if (up.getNextFormat() == org.msgpack.core.MessageFormat.NIL) return null;
                    return up.unpackInt();
                }
            }
        } catch (Exception ignored) {}
        return null;
    }

    /**
     * Whether the peer that sent this announce supports transparent payload compression.
     * Defaults to true when the announce lacks a functionality list.
     */
    public static boolean compressionSupportFromAppData(byte[] appData) {
        if (appData == null || appData.length == 0) return false;
        try {
            int first = appData[0] & 0xFF;
            if ((first >= 0x90 && first <= 0x9f) || first == 0xdc) {
                try (MessageUnpacker up = MessagePack.newDefaultUnpacker(appData)) {
                    int size = up.unpackArrayHeader();
                    if (size < 3) return true;
                    up.skipValue(); // display name
                    up.skipValue(); // stamp cost
                    // element 2: list of supported functionality codes
                    if (up.getNextFormat() == org.msgpack.core.MessageFormat.NIL) return true;
                    int fSize = up.unpackArrayHeader();
                    for (int i = 0; i < fSize; i++) {
                        if (up.unpackInt() == SF_COMPRESSION) return true;
                    }
                    return false;
                }
            } else {
                return true;
            }
        } catch (Exception e) {
            return true;
        }
    }

    /**
     * Extract the human-readable name from a propagation-node announce payload.
     */
    public static String pnNameFromAppData(byte[] appData) {
        if (appData == null || !pnAnnounceDataIsValid(appData)) return null;
        try (MessageUnpacker up = MessagePack.newDefaultUnpacker(appData)) {
            int size = up.unpackArrayHeader();
            // skip 0..5
            for (int i = 0; i < 6; i++) up.skipValue();
            // element 6: metadata map
            int mapSize = up.unpackMapHeader();
            for (int i = 0; i < mapSize; i++) {
                int key = up.unpackInt();
                if (key == PN_META_NAME) {
                    byte[] nameBytes = up.readPayload(up.unpackBinaryHeader());
                    return new String(nameBytes, java.nio.charset.StandardCharsets.UTF_8);
                } else {
                    up.skipValue();
                }
            }
        } catch (Exception ignored) {}
        return null;
    }

    /**
     * Extract the stamp cost from a propagation-node announce payload.
     */
    public static Integer pnStampCostFromAppData(byte[] appData) {
        if (appData == null || !pnAnnounceDataIsValid(appData)) return null;
        try (MessageUnpacker up = MessagePack.newDefaultUnpacker(appData)) {
            up.unpackArrayHeader();
            up.skipValue(); // 0: legacy flag
            up.skipValue(); // 1: timebase
            up.skipValue(); // 2: propagation enabled
            up.skipValue(); // 3: transfer limit
            up.skipValue(); // 4: sync limit
            // 5: [stamp_cost, flexibility, peering_cost]
            up.unpackArrayHeader();
            return up.unpackInt();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Validate propagation-node announce payload structure.
     *
     * <p>Returns true only when the payload is a msgpack list of at least 7 elements with the
     * expected types — mirroring {@code pn_announce_data_is_valid()} in the Python reference.
     */
    public static boolean pnAnnounceDataIsValid(byte[] data) {
        if (data == null) return false;
        try (MessageUnpacker up = MessagePack.newDefaultUnpacker(data)) {
            int size = up.unpackArrayHeader();
            if (size < 7) return false;

            up.skipValue();      // 0: legacy flag (bool)
            up.unpackLong();     // 1: timebase — must be an integer
            up.skipValue();      // 2: propagation enabled (bool)
            up.unpackLong();     // 3: transfer limit
            up.unpackLong();     // 4: sync limit

            // 5: [stamp_cost, flexibility, peering_cost]
            int arrSize = up.unpackArrayHeader();
            if (arrSize < 3) return false;
            up.unpackLong();     // stamp_cost
            up.unpackLong();     // flexibility
            up.unpackLong();     // peering_cost

            // 6: metadata map
            up.unpackMapHeader();

            return true;
        } catch (Exception e) {
            RNS.log("Could not validate propagation node announce data: " + e.getMessage(),
                    RNS.LOG_DEBUG);
            return false;
        }
    }
}