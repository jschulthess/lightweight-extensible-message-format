package io.lxmf.rns;

/**
 * Represents a Reticulum cryptographic identity (Ed25519 key pair).
 *
 * <p>Instances are obtained from {@link RNS#recallIdentity(byte[])} or created via
 * {@link RNSProvider#createIdentity()}.
 */
public interface RNSIdentity {

    /**
     * Returns the 16-byte truncated hash of this identity (matches Python {@code Identity.hash}).
     * This is the first 16 bytes of the SHA-256 of the public key, and is used for addressing.
     */
    byte[] getHash();

    /**
     * Signs {@code data} with the identity's Ed25519 private key.
     *
     * @return 64-byte Ed25519 signature
     */
    byte[] sign(byte[] data);

    /**
     * Verifies an Ed25519 {@code signature} over {@code data} using this identity's public key.
     */
    boolean validate(byte[] signature, byte[] data);
}