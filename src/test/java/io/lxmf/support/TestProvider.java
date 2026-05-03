package io.lxmf.support;

import io.lxmf.rns.RNSAnnounceHandler;
import io.lxmf.rns.RNSDestination;
import io.lxmf.rns.RNSIdentity;
import io.lxmf.rns.RNSLink;
import io.lxmf.rns.RNSPacket;
import io.lxmf.rns.RNSProvider;
import io.lxmf.rns.RNSResource;
import io.lxmf.rns.RNS;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Test-only {@link RNSProvider} that uses real Ed25519 crypto (via {@link StubIdentity})
 * but stubs out all network/transport operations.
 *
 * <p>Call {@link #install()} once from a {@code @BeforeAll} to wire up the provider
 * (idempotent: subsequent calls are no-ops).
 */
public final class TestProvider implements RNSProvider {

    /** The single installed instance; null until first {@link #install()} call. */
    private static volatile TestProvider instance = null;

    /** Identities remembered for recall during unpack. Keyed by 16-byte hash. */
    private final Map<ByteBuffer, RNSIdentity> identityStore = new HashMap<>();

    // ── Public install API ────────────────────────────────────────────────────

    /**
     * Install a shared {@link TestProvider} as the RNS provider.
     * Idempotent — always returns the same singleton instance.
     *
     * @return the installed provider (same instance on every call)
     */
    public static TestProvider install() {
        synchronized (TestProvider.class) {
            if (instance == null) {
                instance = new TestProvider();
                RNS.initialize(instance);
            }
            return instance;
        }
    }

    // ── Identity store ────────────────────────────────────────────────────────

    /**
     * Register an identity so that {@link RNS#recallIdentity(byte[])} can find it.
     */
    public void remember(RNSIdentity identity) {
        identityStore.put(ByteBuffer.wrap(identity.getHash()), identity);
    }

    // ── RNSProvider implementation ────────────────────────────────────────────

    @Override
    public void log(String message, int level) {
        System.out.println("[TEST-RNS L" + level + "] " + message);
    }

    @Override
    public void traceException(Exception e) {
        // no-op in tests
    }

    @Override
    public void panic() {
        throw new RuntimeException("TestProvider.panic() called");
    }

    @Override
    public RNSIdentity createIdentity() {
        return new StubIdentity();
    }

    @Override
    public RNSIdentity recallIdentity(byte[] destinationHash) {
        return identityStore.get(ByteBuffer.wrap(destinationHash));
    }

    @Override
    public byte[] recallAppData(byte[] destinationHash) {
        return null;
    }

    @Override
    public RNSDestination createDestination(RNSIdentity identity, int direction,
                                            int type, String... aspects) {
        return new StubDestination(identity, type);
    }

    @Override
    public RNSLink createLink(RNSDestination destination,
                              Consumer<RNSLink> establishedCallback,
                              Consumer<RNSLink> closedCallback) {
        throw new UnsupportedOperationException("createLink not supported in tests");
    }

    @Override
    public RNSPacket createPacket(RNSDestination destination, byte[] data) {
        throw new UnsupportedOperationException("createPacket not supported in tests");
    }

    @Override
    public RNSResource createResource(byte[] data, RNSLink link,
                                      Consumer<RNSResource> callback,
                                      Consumer<RNSResource> progressCallback,
                                      boolean autoCompress) {
        throw new UnsupportedOperationException("createResource not supported in tests");
    }

    @Override
    public void registerAnnounceHandler(RNSAnnounceHandler handler) {
        // no-op
    }

    @Override
    public boolean hasPath(byte[] destinationHash) {
        return false;
    }

    @Override
    public void requestPath(byte[] destinationHash) {
        // no-op
    }

    @Override
    public int hopsTo(byte[] destinationHash) {
        return 0;
    }
}
