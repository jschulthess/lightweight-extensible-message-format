package io.lxmf.rns;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.function.Consumer;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Static facade that mirrors the Python {@code RNS} module interface used by LXMF.
 *
 * <p>Call {@link #initialize(RNSProvider)} once at startup with a concrete implementation
 * before constructing any LXMF objects.  The cryptographic helpers ({@link #fullHash},
 * {@link #truncatedHash}, {@link #hkdf}) are implemented directly and require no provider.
 */
public final class RNS {

    // ── Log levels (mirror Python RNS) ────────────────────────────────────────
    public static final int LOG_EXTREME  = 0;
    public static final int LOG_DEBUG    = 1;
    public static final int LOG_VERBOSE  = 2;
    public static final int LOG_NOTICE   = 3;
    public static final int LOG_WARNING  = 4;
    public static final int LOG_ERROR    = 5;
    public static final int LOG_CRITICAL = 6;

    // ── Identity constants ────────────────────────────────────────────────────
    /** Ed25519 public-key hash length in bits. */
    public static final int HASHLENGTH           = 256;
    /** Truncated destination hash length in bits. */
    public static final int TRUNCATED_HASHLENGTH = 128;
    /** Ed25519 signature length in bits. */
    public static final int SIGLENGTH            = 512;

    // ── Provider ──────────────────────────────────────────────────────────────
    private static RNSProvider provider;

    private RNS() {}

    /**
     * Wire up the network implementation before using LXMF.
     */
    public static void initialize(RNSProvider p) {
        provider = p;
    }

    private static RNSProvider provider() {
        if (provider == null) {
            throw new IllegalStateException("RNS not initialised — call RNS.initialize(provider) first");
        }
        return provider;
    }

    // ── Crypto (no provider required) ─────────────────────────────────────────

    /** SHA-256 of {@code data}, returns 32 bytes. */
    public static byte[] fullHash(byte[] data) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Truncated hash: first {@link #TRUNCATED_HASHLENGTH}/8 = 16 bytes of SHA-256.
     */
    public static byte[] truncatedHash(byte[] data) {
        byte[] full = fullHash(data);
        return Arrays.copyOf(full, TRUNCATED_HASHLENGTH / 8);
    }

    /**
     * HKDF-SHA256.
     *
     * @param length     desired output length in bytes
     * @param ikm        input key material
     * @param salt       salt (may be null → 32 zero bytes)
     * @param context    info/context (may be null)
     */
    public static byte[] hkdf(int length, byte[] ikm, byte[] salt, byte[] context) {
        try {
            if (salt == null || salt.length == 0) salt = new byte[32];

            // Extract
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(salt, "HmacSHA256"));
            byte[] prk = mac.doFinal(ikm);

            // Expand
            ByteArrayOutputStream out = new ByteArrayOutputStream(length);
            byte[] t = new byte[0];
            for (int i = 1; out.size() < length; i++) {
                mac.init(new SecretKeySpec(prk, "HmacSHA256"));
                mac.update(t);
                if (context != null) mac.update(context);
                mac.update((byte) i);
                t = mac.doFinal();
                out.write(t);
            }
            return Arrays.copyOf(out.toByteArray(), length);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Cryptographically random bytes. */
    public static byte[] randomBytes(int length) {
        byte[] b = new byte[length];
        new SecureRandom().nextBytes(b);
        return b;
    }

    // ── Identity operations ───────────────────────────────────────────────────

    public static RNSIdentity createIdentity() {
        return provider().createIdentity();
    }

    /** Recall an identity by its 16-byte destination hash, or null. */
    public static RNSIdentity recallIdentity(byte[] destinationHash) {
        return provider().recallIdentity(destinationHash);
    }

    /** Recall last-seen app data for a destination hash, or null. */
    public static byte[] recallAppData(byte[] destinationHash) {
        return provider().recallAppData(destinationHash);
    }

    // ── Destination factory ───────────────────────────────────────────────────

    public static RNSDestination createDestination(RNSIdentity identity,
                                                   int direction, int type,
                                                   String... aspects) {
        return provider().createDestination(identity, direction, type, aspects);
    }

    // ── Link factory ──────────────────────────────────────────────────────────

    public static RNSLink createLink(RNSDestination destination,
                                     Consumer<RNSLink> establishedCallback,
                                     Consumer<RNSLink> closedCallback) {
        return provider().createLink(destination, establishedCallback, closedCallback);
    }

    // ── Packet factory ────────────────────────────────────────────────────────

    public static RNSPacket createPacket(RNSDestination destination, byte[] data) {
        return provider().createPacket(destination, data);
    }

    // ── Resource factory ──────────────────────────────────────────────────────

    public static RNSResource createResource(byte[] data, RNSLink link,
                                             Consumer<RNSResource> callback,
                                             Consumer<RNSResource> progressCallback,
                                             boolean autoCompress) {
        return provider().createResource(data, link, callback, progressCallback, autoCompress);
    }

    // ── Transport ─────────────────────────────────────────────────────────────

    public static void registerAnnounceHandler(RNSAnnounceHandler handler) {
        provider().registerAnnounceHandler(handler);
    }

    public static boolean hasPath(byte[] destinationHash) {
        return provider().hasPath(destinationHash);
    }

    public static void requestPath(byte[] destinationHash) {
        provider().requestPath(destinationHash);
    }

    public static int hopsTo(byte[] destinationHash) {
        return provider().hopsTo(destinationHash);
    }

    // ── Logging ───────────────────────────────────────────────────────────────

    public static void log(String message, int level) {
        provider().log(message, level);
    }

    public static void traceException(Exception e) {
        provider().traceException(e);
    }

    public static void panic() {
        provider().panic();
    }

    // ── Formatting helpers (no provider required) ─────────────────────────────

    public static String hexrep(byte[] data, boolean delimit) {
        if (data == null) return "null";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < data.length; i++) {
            if (delimit && i > 0) sb.append(':');
            sb.append(String.format("%02x", data[i] & 0xFF));
        }
        return sb.toString();
    }

    public static String hexrep(byte[] data) {
        return hexrep(data, true);
    }

    public static String prettyhexrep(byte[] data) {
        if (data == null) return "<null>";
        return "<" + hexrep(data, false) + ">";
    }

    public static String prettytime(double seconds) {
        if (seconds < 60) return String.format("%.2fs", seconds);
        if (seconds < 3600) return String.format("%.1fm", seconds / 60);
        if (seconds < 86400) return String.format("%.1fh", seconds / 3600);
        return String.format("%.1fd", seconds / 86400);
    }

    public static String prettysize(long bytes) {
        if (bytes < 1000) return bytes + " B";
        if (bytes < 1_000_000) return String.format("%.1f KB", bytes / 1000.0);
        if (bytes < 1_000_000_000) return String.format("%.1f MB", bytes / 1_000_000.0);
        return String.format("%.1f GB", bytes / 1_000_000_000.0);
    }

    public static String prettyspeed(double bps) {
        if (bps < 1000) return String.format("%.1f bps", bps);
        if (bps < 1_000_000) return String.format("%.1f Kbps", bps / 1000);
        return String.format("%.1f Mbps", bps / 1_000_000);
    }

    /** Concatenate two byte arrays. */
    public static byte[] concat(byte[] a, byte[] b) {
        byte[] result = new byte[a.length + b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }
}
