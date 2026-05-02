package io.lxmf.rns.impl;

import io.lxmf.rns.RNSAnnounceHandler;
import io.lxmf.rns.RNSDestination;
import io.lxmf.rns.RNSIdentity;
import io.lxmf.rns.RNSLink;
import io.lxmf.rns.RNSPacket;
import io.lxmf.rns.RNSProvider;
import io.lxmf.rns.RNSResource;
import io.reticulum.Transport;
import io.reticulum.destination.Destination;
import io.reticulum.destination.DestinationType;
import io.reticulum.destination.Direction;
import io.reticulum.identity.Identity;
import io.reticulum.identity.IdentityKnownDestination;
import io.reticulum.link.Link;
import io.reticulum.packet.Packet;
import io.reticulum.resource.Resource;
import io.reticulum.transport.AnnounceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.function.Consumer;

/**
 * {@link RNSProvider} backed by the native Java Reticulum implementation.
 *
 * <p>Obtain the singleton {@link Transport} from {@code Transport.getInstance()} after
 * initialising {@link io.reticulum.Reticulum}, then pass it here:
 *
 * <pre>{@code
 *   Reticulum r = new Reticulum("/path/to/config");
 *   Transport transport = Transport.start(r);
 *   RNS.initialize(new ReticulumProvider(transport));
 * }</pre>
 */
public final class ReticulumProvider implements RNSProvider {

    private static final Logger log = LoggerFactory.getLogger(ReticulumProvider.class);

    private final Transport transport;

    public ReticulumProvider(Transport transport) {
        this.transport = transport;
    }

    // ── Identity ──────────────────────────────────────────────────────────────

    @Override
    public RNSIdentity createIdentity() {
        return new IdentityAdapter(new Identity());
    }

    @Override
    public RNSIdentity recallIdentity(byte[] destinationHash) {
        Identity id = IdentityKnownDestination.recall(destinationHash);
        return id != null ? new IdentityAdapter(id) : null;
    }

    @Override
    public byte[] recallAppData(byte[] destinationHash) {
        return IdentityKnownDestination.recallAppData(destinationHash);
    }

    // ── Destination ───────────────────────────────────────────────────────────

    @Override
    public RNSDestination createDestination(RNSIdentity identity, int direction,
                                            int type, String... aspects) {
        Identity nativeId = identity instanceof IdentityAdapter
                ? ((IdentityAdapter) identity).getNative() : null;
        Direction nativeDir = direction == RNSDestination.OUT ? Direction.OUT : Direction.IN;
        DestinationType nativeType = DestinationType.values()[type];

        // aspects[0] is the app name; remaining entries are sub-aspects
        String appName = aspects.length > 0 ? aspects[0] : "";
        String[] subAspects = aspects.length > 1
                ? Arrays.copyOfRange(aspects, 1, aspects.length) : new String[0];

        Destination dest = new Destination(nativeId, nativeDir, nativeType, appName, subAspects);
        return new DestinationAdapter(dest);
    }

    // ── Link ──────────────────────────────────────────────────────────────────

    @Override
    public RNSLink createLink(RNSDestination destination,
                              Consumer<RNSLink> establishedCallback,
                              Consumer<RNSLink> closedCallback) {
        Destination nativeDest = ((DestinationAdapter) destination).getNative();
        Consumer<Link> nativeEstablished = establishedCallback != null
                ? link -> establishedCallback.accept(new LinkAdapter(link)) : null;
        Consumer<Link> nativeClosed = closedCallback != null
                ? link -> closedCallback.accept(new LinkAdapter(link)) : null;
        Link link = new Link(nativeDest, nativeEstablished, nativeClosed, null, null, null);
        return new LinkAdapter(link);
    }

    // ── Packet ────────────────────────────────────────────────────────────────

    @Override
    public RNSPacket createPacket(RNSDestination destination, byte[] data) {
        if (destination instanceof LinkAsDestinationAdapter) {
            Link link = ((LinkAsDestinationAdapter) destination).getNativeLink();
            return new PacketAdapter(new Packet(link, data));
        }
        Destination dest = ((DestinationAdapter) destination).getNative();
        return new PacketAdapter(new Packet(dest, data));
    }

    // ── Resource ──────────────────────────────────────────────────────────────

    @Override
    public RNSResource createResource(byte[] data, RNSLink link,
                                      Consumer<RNSResource> callback,
                                      Consumer<RNSResource> progressCallback,
                                      boolean autoCompress) {
        Link nativeLink = ((LinkAdapter) link).getNative();
        Consumer<Resource> nativeCb = callback != null
                ? r -> callback.accept(new ResourceAdapter(r)) : null;
        Consumer<Resource> nativeProg = progressCallback != null
                ? r -> progressCallback.accept(new ResourceAdapter(r)) : null;
        Resource resource = new Resource(data, nativeLink, nativeCb, nativeProg,
                null, false, null, autoCompress, null, true);
        return new ResourceAdapter(resource);
    }

    // ── Transport ─────────────────────────────────────────────────────────────

    @Override
    public void registerAnnounceHandler(RNSAnnounceHandler handler) {
        transport.registerAnnounceHandler(new AnnounceHandlerAdapter(handler));
    }

    @Override
    public boolean hasPath(byte[] destinationHash) {
        Boolean result = transport.hasPath(destinationHash);
        return result != null && result;
    }

    @Override
    public void requestPath(byte[] destinationHash) {
        transport.requestPath(destinationHash);
    }

    @Override
    public int hopsTo(byte[] destinationHash) {
        return transport.hopsTo(destinationHash);
    }

    // ── Logging ───────────────────────────────────────────────────────────────

    @Override
    public void log(String message, int level) {
        switch (level) {
            case 0: // EXTREME
            case 1: // DEBUG
                log.debug(message);
                break;
            case 2: // VERBOSE
                log.info(message);
                break;
            case 3: // NOTICE
                log.info(message);
                break;
            case 4: // WARNING
                log.warn(message);
                break;
            case 5: // ERROR
            case 6: // CRITICAL
                log.error(message);
                break;
            default:
                log.info(message);
        }
    }

    @Override
    public void traceException(Exception e) {
        log.error("LXMF exception", e);
    }

    @Override
    public void panic() {
        log.error("LXMF panic — unrecoverable error");
        throw new RuntimeException("LXMF panic");
    }

    // ── Inner: AnnounceHandler adapter ────────────────────────────────────────

    private static final class AnnounceHandlerAdapter implements AnnounceHandler {

        private final RNSAnnounceHandler handler;

        AnnounceHandlerAdapter(RNSAnnounceHandler handler) {
            this.handler = handler;
        }

        @Override
        public String getAspectFilter() {
            return handler.getAspectFilter();
        }

        @Override
        public Boolean receivePathResponses() {
            return handler.getReceivePathResponses();
        }

        @Override
        public void receivedAnnounce(byte[] destinationHash, Identity announcedIdentity,
                                     byte[] appData, byte[] announcePacketHash,
                                     boolean isPathResponse) {
            RNSIdentity wrapped = announcedIdentity != null
                    ? new IdentityAdapter(announcedIdentity) : null;
            handler.receivedAnnounce(destinationHash, wrapped, appData,
                    announcePacketHash, isPathResponse);
        }
    }
}
