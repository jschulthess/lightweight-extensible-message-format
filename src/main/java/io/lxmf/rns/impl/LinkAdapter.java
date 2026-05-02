package io.lxmf.rns.impl;

import io.lxmf.rns.RNSDestination;
import io.lxmf.rns.RNSIdentity;
import io.lxmf.rns.RNSLink;
import io.lxmf.rns.RNSLinkRequestReceipt;
import io.lxmf.rns.RNSResource;
import io.reticulum.link.Link;
import io.reticulum.link.LinkStatus;
import io.reticulum.resource.Resource;

import java.util.function.Consumer;

/** Wraps a native {@link Link} as an {@link RNSLink}. */
public final class LinkAdapter implements RNSLink {

    private final Link link;

    public LinkAdapter(Link link) {
        this.link = link;
    }

    /** Returns the underlying native Link. */
    public Link getNative() {
        return link;
    }

    @Override
    public int getStatus() {
        LinkStatus s = link.getStatus();
        if (s == null) return PENDING;
        switch (s) {
            case ACTIVE: return ACTIVE;
            case CLOSED: return CLOSED;
            // STALE is still usable; treat as ACTIVE until it transitions to CLOSED
            case STALE:  return ACTIVE;
            default:     return PENDING; // PENDING, HANDSHAKE
        }
    }

    @Override
    public byte[] getLinkId() {
        return link.getLinkId();
    }

    @Override
    public double noDataFor() {
        return (double) link.noDataFor();
    }

    @Override
    public double getEstablishmentRate() {
        return (double) link.getEstablishmentRate();
    }

    @Override
    public void teardown() {
        link.teardown();
    }

    @Override
    public void identify(RNSIdentity identity) {
        link.identify(((IdentityAdapter) identity).getNative());
    }

    @Override
    public void request(String path, Object data,
                        Consumer<RNSLinkRequestReceipt> responseCallback,
                        Consumer<RNSLinkRequestReceipt> failedCallback) {
        request(path, data, responseCallback, failedCallback, null);
    }

    @Override
    public void request(String path, Object data,
                        Consumer<RNSLinkRequestReceipt> responseCallback,
                        Consumer<RNSLinkRequestReceipt> failedCallback,
                        Consumer<RNSLinkRequestReceipt> progressCallback) {
        byte[] encoded;
        try {
            encoded = MsgPackHelper.pack(data);
        } catch (Exception e) {
            if (failedCallback != null) failedCallback.accept(null);
            return;
        }

        Consumer<io.reticulum.link.RequestReceipt> nativeResponse = responseCallback != null
                ? r -> responseCallback.accept(new RequestReceiptAdapter(r)) : null;
        Consumer<io.reticulum.link.RequestReceipt> nativeFailed = failedCallback != null
                ? r -> failedCallback.accept(new RequestReceiptAdapter(r)) : null;
        Consumer<io.reticulum.link.RequestReceipt> nativeProgress = progressCallback != null
                ? r -> progressCallback.accept(new RequestReceiptAdapter(r)) : null;

        link.request(path, encoded, nativeResponse, nativeFailed, nativeProgress, null);
    }

    @Override
    public RNSResource sendResource(byte[] data,
                                    Consumer<RNSResource> callback,
                                    Consumer<RNSResource> progressCallback,
                                    boolean autoCompress) {
        Consumer<Resource> nativeCb = callback != null
                ? r -> callback.accept(new ResourceAdapter(r)) : null;
        Consumer<Resource> nativeProg = progressCallback != null
                ? r -> progressCallback.accept(new ResourceAdapter(r)) : null;
        Resource resource = new Resource(data, link, nativeCb, nativeProg,
                null, false, null, autoCompress, null, true);
        return new ResourceAdapter(resource);
    }

    @Override
    public void setPacketCallback(RNSDestination.PacketCallback callback) {
        link.setPacketCallback((data, packet) ->
                callback.onPacket(data, new PacketAdapter(packet)));
    }

    @Override
    public void setResourceStartCallback(ResourceStartCallback callback) {
        link.setResourceStartedCallback(resource ->
                callback.onResourceStart(new ResourceAdapter(resource)));
    }

    @Override
    public RNSDestination asDestination() {
        return new LinkAsDestinationAdapter(link);
    }
}
