package io.lxmf.support;

import io.lxmf.rns.RNSDestination;
import io.lxmf.rns.RNSIdentity;
import io.lxmf.rns.RNSLink;
import io.lxmf.rns.RNSRequestHandler;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Minimal {@link RNSDestination} backed by a {@link RNSIdentity} for unit tests.
 * Signing delegates to the identity; encryption is a no-op pass-through.
 */
public final class StubDestination implements RNSDestination {

    private final RNSIdentity identity;
    private final int type;

    public StubDestination(RNSIdentity identity) {
        this(identity, SINGLE);
    }

    public StubDestination(RNSIdentity identity, int type) {
        this.identity = identity;
        this.type = type;
    }

    @Override
    public byte[] getHash() {
        return identity.getHash();
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public int getDirection() {
        return IN;
    }

    @Override
    public byte[] encrypt(byte[] plaintext) {
        // Pass-through: no real encryption in tests
        return plaintext;
    }

    @Override
    public byte[] getLatestRatchetId() {
        return null;
    }

    @Override
    public void enableRatchets(String ratchetPath) {
        // no-op
    }

    @Override
    public void enforceRatchets() {
        // no-op
    }

    @Override
    public void announce(byte[] appData, Object attachedInterface) {
        // no-op
    }

    @Override
    public void setDefaultAppData(Supplier<byte[]> supplier) {
        // no-op
    }

    @Override
    public String getDisplayName() {
        return null;
    }

    @Override
    public void setDisplayName(String name) {
        // no-op
    }

    @Override
    public Integer getStampCost() {
        return null;
    }

    @Override
    public void setStampCost(Integer cost) {
        // no-op
    }

    @Override
    public void setPacketCallback(PacketCallback callback) {
        // no-op
    }

    @Override
    public void setLinkEstablishedCallback(Consumer<RNSLink> callback) {
        // no-op
    }

    @Override
    public void registerRequestHandler(String path, RNSRequestHandler handler,
                                       int allowPolicy, List<byte[]> allowedList) {
        // no-op
    }

    @Override
    public byte[] sign(byte[] data) {
        return identity.sign(data);
    }

    @Override
    public RNSIdentity getParentIdentity() {
        return identity;
    }

    @Override
    public void identify(RNSIdentity identity) {
        // no-op
    }

    @Override
    public byte[] getLinkId() {
        return null;
    }

    @Override
    public RNSLink getUnderlyingLink() {
        return null;
    }
}
