package io.lxmf.support;

import io.lxmf.rns.RNSIdentity;
import io.reticulum.identity.Identity;

/**
 * A stub {@link RNSIdentity} backed by a native Reticulum {@link Identity} for tests.
 * Uses the real Ed25519 crypto so that signature round-trips actually work.
 */
public final class StubIdentity implements RNSIdentity {

    private final Identity native_;

    public StubIdentity() {
        this.native_ = new Identity();
    }

    public StubIdentity(Identity id) {
        this.native_ = id;
    }

    public Identity getNative() {
        return native_;
    }

    @Override
    public byte[] getHash() {
        return native_.getHash();
    }

    @Override
    public byte[] sign(byte[] data) {
        return native_.sign(data);
    }

    @Override
    public boolean validate(byte[] signature, byte[] data) {
        return native_.validate(signature, data);
    }
}
