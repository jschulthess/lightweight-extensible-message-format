package io.lxmf.rns.impl;

import io.lxmf.rns.RNSDestination;
import io.lxmf.rns.RNSIdentity;
import io.lxmf.rns.RNSLink;
import io.lxmf.rns.RNSRequestHandler;
import io.reticulum.link.Link;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Exposes a native {@link Link} as an {@link RNSDestination} so it can be used as a send target.
 *
 * <p>Created by {@link LinkAdapter#asDestination()}.  The LXMF code sends packets and resources
 * to destinations; this adapter lets a link participate as one.  Most Destination-specific
 * operations are not applicable and are no-ops or return sensible defaults.
 */
public final class LinkAsDestinationAdapter implements RNSDestination {

    private final Link link;

    public LinkAsDestinationAdapter(Link link) {
        this.link = link;
    }

    /** Returns the underlying native Link. */
    public Link getNativeLink() {
        return link;
    }

    @Override
    public byte[] getHash() {
        return link.getLinkId();
    }

    @Override
    public int getType() {
        return RNSDestination.LINK;
    }

    @Override
    public int getDirection() {
        return RNSDestination.IN;
    }

    @Override
    public byte[] encrypt(byte[] data) {
        return link.encrypt(data);
    }

    @Override
    public byte[] getLatestRatchetId() {
        return null;
    }

    @Override
    public void enableRatchets(String ratchetPath) {}

    @Override
    public void enforceRatchets() {}

    @Override
    public void announce(byte[] appData, Object attachedInterface) {}

    @Override
    public void setDefaultAppData(Supplier<byte[]> supplier) {}

    @Override
    public String getDisplayName() {
        return null;
    }

    @Override
    public void setDisplayName(String name) {}

    @Override
    public Integer getStampCost() {
        return null;
    }

    @Override
    public void setStampCost(Integer cost) {}

    @Override
    public void setPacketCallback(PacketCallback callback) {
        link.setPacketCallback((data, packet) ->
                callback.onPacket(data, new PacketAdapter(packet)));
    }

    @Override
    public void setLinkEstablishedCallback(Consumer<RNSLink> callback) {}

    @Override
    public void registerRequestHandler(String path, RNSRequestHandler handler,
                                       int allowPolicy, List<byte[]> allowedList) {}

    @Override
    public byte[] sign(byte[] data) {
        return link.sign(data);
    }

    @Override
    public RNSIdentity getParentIdentity() {
        return null;
    }

    @Override
    public void identify(RNSIdentity identity) {
        link.identify(((IdentityAdapter) identity).getNative());
    }

    @Override
    public byte[] getLinkId() {
        return link.getLinkId();
    }

    @Override
    public RNSLink getUnderlyingLink() {
        return new LinkAdapter(link);
    }
}
