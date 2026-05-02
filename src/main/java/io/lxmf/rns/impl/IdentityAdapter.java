package io.lxmf.rns.impl;

import io.lxmf.rns.RNSIdentity;
import io.reticulum.identity.Identity;

/** Wraps a native {@link Identity} as an {@link RNSIdentity}. */
public final class IdentityAdapter implements RNSIdentity {

    private final Identity identity;

    public IdentityAdapter(Identity identity) {
        this.identity = identity;
    }

    /** Returns the underlying native Identity (for use within the impl package). */
    public Identity getNative() {
        return identity;
    }

    @Override
    public byte[] getHash() {
        return identity.getHash();
    }

    @Override
    public byte[] sign(byte[] data) {
        return identity.sign(data);
    }

    @Override
    public boolean validate(byte[] signature, byte[] data) {
        return identity.validate(signature, data);
    }
}
