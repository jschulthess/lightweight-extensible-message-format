package io.lxmf.rns;

import java.util.function.Consumer;

/**
 * Receipt returned when an {@link RNSPacket} is sent, allowing delivery-/timeout-callbacks to
 * be registered after the fact.
 */
public interface RNSPacketReceipt {

    /** Register a callback that fires when the packet is acknowledged by the remote end. */
    void setDeliveryCallback(Consumer<RNSPacketReceipt> callback);

    /** Register a callback that fires when delivery confirmation times out. */
    void setTimeoutCallback(Consumer<RNSPacketReceipt> callback);

    /** The destination (link) this receipt belongs to; useful for teardown on timeout. */
    RNSDestination getDestination();

    /** The ratchet ID that was in effect when the packet was sent, or null. */
    byte[] getRatchetId();
}