package io.lxmf.rns.impl;

import io.lxmf.rns.RNSDestination;
import io.lxmf.rns.RNSPacketReceipt;
import io.reticulum.packet.PacketReceipt;

import java.util.function.Consumer;

/** Wraps a native {@link PacketReceipt} as an {@link RNSPacketReceipt}. */
public final class PacketReceiptAdapter implements RNSPacketReceipt {

    private final PacketReceipt receipt;

    public PacketReceiptAdapter(PacketReceipt receipt) {
        this.receipt = receipt;
    }

    @Override
    public void setDeliveryCallback(Consumer<RNSPacketReceipt> callback) {
        receipt.setDeliveryCallback(r -> callback.accept(new PacketReceiptAdapter(r)));
    }

    @Override
    public void setTimeoutCallback(Consumer<RNSPacketReceipt> callback) {
        receipt.setTimeoutCallback(r -> callback.accept(new PacketReceiptAdapter(r)));
    }

    @Override
    public RNSDestination getDestination() {
        io.reticulum.destination.AbstractDestination dest = receipt.getDestination();
        if (dest instanceof io.reticulum.destination.Destination) {
            return new DestinationAdapter((io.reticulum.destination.Destination) dest);
        }
        if (dest instanceof io.reticulum.link.Link) {
            return new LinkAsDestinationAdapter((io.reticulum.link.Link) dest);
        }
        return null;
    }

    @Override
    public byte[] getRatchetId() {
        return null;
    }
}
