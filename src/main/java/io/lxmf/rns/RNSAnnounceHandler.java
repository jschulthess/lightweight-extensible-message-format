package io.lxmf.rns;

/**
 * Interface for handlers that process RNS announce packets.
 * Implement and register via {@link RNSTransport#registerAnnounceHandler(RNSAnnounceHandler)}.
 */
public interface RNSAnnounceHandler {

    /** The aspect filter string this handler responds to (e.g. "lxmf.delivery"). */
    String getAspectFilter();

    /** Whether this handler wants to see path-response announces in addition to normal ones. */
    boolean getReceivePathResponses();

    /**
     * Called when a matching announce is received.
     *
     * @param destinationHash    16-byte truncated hash of the announcing destination
     * @param announcedIdentity  the identity attached to the announce, may be null
     * @param appData            arbitrary application data included in the announce, may be null
     * @param announcePacketHash hash of the raw announce packet, may be null
     * @param isPathResponse     true when this announce arrived as a path-response
     */
    void receivedAnnounce(byte[] destinationHash, RNSIdentity announcedIdentity,
                          byte[] appData, byte[] announcePacketHash, boolean isPathResponse);
}