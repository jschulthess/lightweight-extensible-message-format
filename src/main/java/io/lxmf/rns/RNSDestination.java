package io.lxmf.rns;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Represents a Reticulum destination — a named, addressable endpoint backed by an identity.
 */
public interface RNSDestination {

    /* Destination types */
    int SINGLE = 0x00;
    int GROUP  = 0x01;
    int PLAIN  = 0x02;
    int LINK   = 0x03;

    /* Directions */
    int IN  = 0x11;
    int OUT = 0x12;

    /* Request-handler allow policies */
    int ALLOW_ALL  = 0x00;
    int ALLOW_LIST = 0x01;

    /** 16-byte truncated destination hash. */
    byte[] getHash();

    /** Destination type: {@link #SINGLE}, {@link #GROUP}, {@link #PLAIN}, or {@link #LINK}. */
    int getType();

    /** Direction: {@link #IN} or {@link #OUT}. */
    int getDirection();

    // ── Encryption ────────────────────────────────────────────────────────────

    /**
     * Encrypts {@code plaintext} for this destination.
     * Applicable to SINGLE and GROUP destinations.
     */
    byte[] encrypt(byte[] plaintext);

    /**
     * The ratchet ID that was used in the most recent encrypt call, or null.
     * Recorded by the sender to tell the receiver which ratchet step to use.
     */
    byte[] getLatestRatchetId();

    // ── Ratchets ──────────────────────────────────────────────────────────────

    /** Enable forward-secrecy ratchets, persisting state to {@code ratchetPath}. */
    void enableRatchets(String ratchetPath);

    /** Require that incoming packets use a valid ratchet key. */
    void enforceRatchets();

    // ── Announce ──────────────────────────────────────────────────────────────

    /** Announce this destination on the network with optional attached data. */
    void announce(byte[] appData, Object attachedInterface);

    /** Register a dynamic app-data supplier that is called before each announce. */
    void setDefaultAppData(Supplier<byte[]> supplier);

    // ── Display metadata ──────────────────────────────────────────────────────

    String getDisplayName();

    void setDisplayName(String name);

    // ── Inbound stamp cost ────────────────────────────────────────────────────

    /** Required proof-of-work cost for inbound delivery (null = no cost required). */
    Integer getStampCost();

    void setStampCost(Integer cost);

    // ── Callbacks ─────────────────────────────────────────────────────────────

    /**
     * Register a callback for raw packets received on this destination.
     *
     * @param callback accepts (packetData: byte[], packet: RNSPacket)
     */
    void setPacketCallback(PacketCallback callback);

    /**
     * Register a callback fired when a new inbound link is established to this destination.
     *
     * @param callback accepts the newly established {@link RNSLink}
     */
    void setLinkEstablishedCallback(Consumer<RNSLink> callback);

    // ── Request handlers ──────────────────────────────────────────────────────

    /**
     * Register a named request handler.
     *
     * @param path        path string (e.g. "/offer")
     * @param handler     the handler to invoke
     * @param allowPolicy {@link #ALLOW_ALL} or {@link #ALLOW_LIST}
     * @param allowedList identity hashes allowed to make requests (used when policy = ALLOW_LIST)
     */
    void registerRequestHandler(String path, RNSRequestHandler handler,
                                int allowPolicy, List<byte[]> allowedList);

    // ── Signing ───────────────────────────────────────────────────────────────

    /**
     * Sign {@code data} using this destination's private key.
     * Only valid for IN destinations backed by a local identity.
     * Returns null (or throws) if this is an OUT or remote destination.
     */
    byte[] sign(byte[] data);

    /**
     * Return the backing identity for this destination, or null if not locally owned.
     */
    RNSIdentity getParentIdentity();

    // ── Link identification ───────────────────────────────────────────────────

    /** When this destination is a LINK type, identify the local identity on it. */
    void identify(RNSIdentity identity);

    // ── Link ID ───────────────────────────────────────────────────────────────

    /** When this destination wraps a link, returns the link's 16-byte ID. */
    byte[] getLinkId();

    /**
     * When this destination is of type {@link #LINK}, returns the underlying {@link RNSLink}.
     * Returns null for non-link destinations.
     */
    RNSLink getUnderlyingLink();

    // ── Callback types ────────────────────────────────────────────────────────

    @FunctionalInterface
    interface PacketCallback {
        void onPacket(byte[] data, RNSPacket packet);
    }
}
