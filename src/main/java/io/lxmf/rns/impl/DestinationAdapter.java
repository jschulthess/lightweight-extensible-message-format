package io.lxmf.rns.impl;

import io.lxmf.rns.RNSAnnounceHandler;
import io.lxmf.rns.RNSDestination;
import io.lxmf.rns.RNSIdentity;
import io.lxmf.rns.RNSLink;
import io.lxmf.rns.RNSRequestHandler;
import io.reticulum.destination.Destination;
import io.reticulum.destination.Request;
import io.reticulum.destination.RequestPolicy;
import io.reticulum.identity.Identity;

import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Wraps a native {@link Destination} as an {@link RNSDestination}.
 *
 * <p>Key differences bridged here:
 * <ul>
 *   <li>{@code encrypt()} delegates to {@code decrypt()} for IN destinations (Python convention)</li>
 *   <li>{@code setDefaultAppData(Supplier)} stores the supplier; fresh bytes are injected on each
 *       {@code announce()} call and also pushed to the native destination for path-response announces</li>
 *   <li>{@code getStampCost()} / {@code setStampCost()} are tracked locally (not in native)</li>
 *   <li>Request handlers wrap the msgpack encode/decode boundary via {@link MsgPackHelper}</li>
 * </ul>
 */
public final class DestinationAdapter implements RNSDestination {

    private final Destination destination;
    private Supplier<byte[]> appDataSupplier;
    private String displayName;
    private Integer stampCost;

    public DestinationAdapter(Destination destination) {
        this.destination = destination;
    }

    /** Returns the underlying native Destination. */
    public Destination getNative() {
        return destination;
    }

    @Override
    public byte[] getHash() {
        return destination.getHash();
    }

    @Override
    public int getType() {
        return destination.getType().ordinal();
    }

    @Override
    public int getDirection() {
        switch (destination.getDirection()) {
            case OUT: return RNSDestination.OUT;
            default:  return RNSDestination.IN;
        }
    }

    @Override
    public byte[] encrypt(byte[] data) {
        // Python convention: encrypt() on an IN destination actually decrypts
        if (destination.getDirection() == io.reticulum.destination.Direction.IN) {
            return destination.decrypt(data);
        }
        return destination.encrypt(data);
    }

    @Override
    public byte[] getLatestRatchetId() {
        return Identity.getCurrentRatchetId(destination.getHash());
    }

    @Override
    public void enableRatchets(String ratchetPath) {
        destination.enableRatchets(Path.of(ratchetPath));
    }

    @Override
    public void enforceRatchets() {
        destination.enforceRatchets();
    }

    @Override
    public void announce(byte[] appData, Object attachedInterface) {
        byte[] data = appData != null ? appData
                    : (appDataSupplier != null ? appDataSupplier.get() : null);
        if (data != null) {
            destination.setDefaultAppData(data);
        }
        destination.announce(data);
    }

    @Override
    public void setDefaultAppData(Supplier<byte[]> supplier) {
        this.appDataSupplier = supplier;
        if (supplier != null) {
            byte[] current = supplier.get();
            if (current != null) destination.setDefaultAppData(current);
        }
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public void setDisplayName(String name) {
        this.displayName = name;
    }

    @Override
    public Integer getStampCost() {
        return stampCost;
    }

    @Override
    public void setStampCost(Integer cost) {
        this.stampCost = cost;
    }

    @Override
    public void setPacketCallback(PacketCallback callback) {
        destination.setPacketCallback((data, packet) ->
                callback.onPacket(data, new PacketAdapter(packet)));
    }

    @Override
    public void setLinkEstablishedCallback(Consumer<RNSLink> callback) {
        destination.setLinkEstablishedCallback(link -> callback.accept(new LinkAdapter(link)));
    }

    @Override
    public void registerRequestHandler(String path, RNSRequestHandler handler,
                                       int allowPolicy, List<byte[]> allowedList) {
        RequestPolicy policy = allowPolicy == RNSDestination.ALLOW_LIST
                ? RequestPolicy.ALLOW_LIST : RequestPolicy.ALLOW_ALL;

        destination.registerRequestHandler(path, (Request req) -> {
            try {
                Object decodedData = MsgPackHelper.unpack(req.getData());
                RNSIdentity remoteId = req.getRemoteIdentity() != null
                        ? new IdentityAdapter(req.getRemoteIdentity()) : null;
                double requestedAt = req.getRequestedAt().toEpochMilli() / 1000.0;
                Object result = handler.handle(req.getPath(), decodedData,
                        req.getRequestId(), remoteId, requestedAt);
                return MsgPackHelper.pack(result);
            } catch (Exception e) {
                return null;
            }
        }, policy, allowedList);
    }

    @Override
    public byte[] sign(byte[] data) {
        return destination.sign(data);
    }

    @Override
    public RNSIdentity getParentIdentity() {
        Identity id = destination.getIdentity();
        return id != null ? new IdentityAdapter(id) : null;
    }

    @Override
    public void identify(RNSIdentity identity) {
        // Regular destinations are not link-type; this is a no-op here.
        // Link identification is handled by LinkAdapter and LinkAsDestinationAdapter.
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
