package io.lxmf.rns;

import java.util.function.Consumer;

/**
 * Network-layer abstraction that bridges LXMF to a concrete Reticulum implementation.
 *
 * <p>Implement this interface with a real Reticulum Java library (e.g. ReticulumJ) and call
 * {@link RNS#initialize(RNSProvider)} before constructing any LXMF objects.
 */
public interface RNSProvider {

    // ── Logging ───────────────────────────────────────────────────────────────

    void log(String message, int level);

    void traceException(Exception e);

    void panic();

    // ── Identity management ───────────────────────────────────────────────────

    /** Create a new random identity (generates a new Ed25519 key pair). */
    RNSIdentity createIdentity();

    /**
     * Recall a previously seen identity by its destination hash.
     *
     * @param destinationHash 16-byte truncated hash
     * @return the recalled identity, or null if not known
     */
    RNSIdentity recallIdentity(byte[] destinationHash);

    /**
     * Recall the app_data bytes most recently seen in an announce from {@code destinationHash}.
     */
    byte[] recallAppData(byte[] destinationHash);

    // ── Destination factory ───────────────────────────────────────────────────

    /**
     * Create a destination.
     *
     * @param identity  backing identity (may be null for PLAIN destinations)
     * @param direction {@link RNSDestination#IN} or {@link RNSDestination#OUT}
     * @param type      {@link RNSDestination#SINGLE}, {@link RNSDestination#GROUP}, etc.
     * @param aspects   one or more aspect strings (e.g. "lxmf", "delivery")
     */
    RNSDestination createDestination(RNSIdentity identity, int direction, int type, String... aspects);

    // ── Link factory ──────────────────────────────────────────────────────────

    /**
     * Open an outbound link to {@code destination}.
     *
     * @param destination       the target destination
     * @param establishedCallback invoked when the link is ready
     * @param closedCallback    invoked when the link closes
     */
    RNSLink createLink(RNSDestination destination,
                       Consumer<RNSLink> establishedCallback,
                       Consumer<RNSLink> closedCallback);

    // ── Packet factory ────────────────────────────────────────────────────────

    RNSPacket createPacket(RNSDestination destination, byte[] data);

    // ── Resource factory ──────────────────────────────────────────────────────

    RNSResource createResource(byte[] data, RNSLink link,
                               Consumer<RNSResource> callback,
                               Consumer<RNSResource> progressCallback,
                               boolean autoCompress);

    // ── Transport ─────────────────────────────────────────────────────────────

    void registerAnnounceHandler(RNSAnnounceHandler handler);

    boolean hasPath(byte[] destinationHash);

    void requestPath(byte[] destinationHash);

    int hopsTo(byte[] destinationHash);
}
