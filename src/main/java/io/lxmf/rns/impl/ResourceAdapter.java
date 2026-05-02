package io.lxmf.rns.impl;

import io.lxmf.rns.RNSLink;
import io.lxmf.rns.RNSResource;
import io.reticulum.resource.Resource;
import io.reticulum.resource.ResourceStatus;

import java.util.function.Consumer;

/** Wraps a native {@link Resource} as an {@link RNSResource}. */
public final class ResourceAdapter implements RNSResource {

    private final Resource resource;

    public ResourceAdapter(Resource resource) {
        this.resource = resource;
    }

    /** Returns the underlying native Resource. */
    public Resource getNative() {
        return resource;
    }

    @Override
    public int getStatus() {
        ResourceStatus s = resource.getStatus();
        if (s == ResourceStatus.COMPLETE) return COMPLETE;
        if (s == ResourceStatus.FAILED || s == ResourceStatus.CORRUPT) return FAILED;
        return FAILED; // treat any non-complete terminal state as failure
    }

    @Override
    public RNSLink getLink() {
        io.reticulum.link.Link link = resource.getLink();
        return link != null ? new LinkAdapter(link) : null;
    }

    @Override
    public double getProgress() {
        return resource.getProgress();
    }

    @Override
    public long getTransferSize() {
        return resource.getTotalSize();
    }

    @Override
    public long getDataSize() {
        return resource.getDataSize();
    }

    @Override
    public byte[] getData() {
        return resource.getData();
    }

    @Override
    public void setConcludedCallback(Consumer<RNSResource> callback) {
        resource.setCallback(r -> callback.accept(new ResourceAdapter(r)));
    }
}
