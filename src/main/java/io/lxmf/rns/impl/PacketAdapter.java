package io.lxmf.rns.impl;

import io.lxmf.rns.RNSPacket;
import io.lxmf.rns.RNSPacketReceipt;
import io.reticulum.packet.Packet;

/** Wraps a native {@link Packet} as an {@link RNSPacket}. */
public final class PacketAdapter implements RNSPacket {

    private final Packet packet;

    public PacketAdapter(Packet packet) {
        this.packet = packet;
    }

    /** Returns the underlying native Packet. */
    public Packet getNative() {
        return packet;
    }

    @Override
    public RNSPacketReceipt send() {
        io.reticulum.packet.PacketReceipt receipt = packet.send();
        return receipt != null ? new PacketReceiptAdapter(receipt) : null;
    }

    @Override
    public byte[] getRatchetId() {
        // The native Packet doesn't expose a ratchet ID directly; ratchet state is managed
        // internally by Destination/Identity after encryption.
        return null;
    }

    @Override
    public void prove() {
        packet.prove(null);
    }

    @Override
    public byte[] getDestinationHash() {
        return packet.getDestinationHash();
    }

    @Override
    public int getDestinationType() {
        io.reticulum.destination.DestinationType type = packet.getDestinationType();
        return type != null ? type.ordinal() : 0;
    }
}
