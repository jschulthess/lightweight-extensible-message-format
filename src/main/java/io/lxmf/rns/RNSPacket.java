package io.lxmf.rns;

/**
 * Represents a single Reticulum network packet.
 *
 * <p>Default MDU values below reflect standard Reticulum configuration; they are exposed here
 * so that LXMF size calculations can reference them without a live RNS instance.
 */
public interface RNSPacket {

    /** Maximum data unit for encrypted (SINGLE destination) packets: 383 bytes. */
    int ENCRYPTED_MDU = 383;

    /** Maximum data unit for plain (unencrypted) packets: 464 bytes. */
    int PLAIN_MDU = 464;

    /**
     * Transmits the packet on the network.
     *
     * @return a receipt that allows delivery/timeout callbacks to be registered, or null on error
     */
    RNSPacketReceipt send();

    /**
     * The ratchet ID that was negotiated for this packet (populated after {@link #send()}).
     * May be null.
     */
    byte[] getRatchetId();

    /**
     * Send a proof-of-receipt for this packet back to the sender.
     * Used to acknowledge delivery of opportunistic LXMF messages.
     */
    void prove();

    /** 16-byte truncated hash of the destination this packet was received on, or null. */
    byte[] getDestinationHash();

    /** Destination type at time of receipt: {@link RNSDestination#SINGLE}, {@link RNSDestination#LINK}, etc. */
    int getDestinationType();
}