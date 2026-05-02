package io.lxmf.rns;

import java.util.List;
import java.util.function.Consumer;

/**
 * Represents a reliable Reticulum link between two endpoints.
 */
public interface RNSLink {

    /** Default maximum data unit for a link packet: 431 bytes. */
    int MDU = 431;

    int PENDING = 0x00;
    int ACTIVE  = 0x01;
    int CLOSED  = 0x02;

    /** Current link status. */
    int getStatus();

    /** The 16-byte link identifier. */
    byte[] getLinkId();

    /** Seconds since data was last exchanged on this link. */
    double noDataFor();

    /** Link establishment rate in bits/second, or 0 if not yet established. */
    double getEstablishmentRate();

    /** Initiate graceful shutdown of this link. */
    void teardown();

    /**
     * Identify the local identity on this link (so the remote end can authorise requests).
     */
    void identify(RNSIdentity identity);

    /**
     * Send a request over the link.
     *
     * @param path             request path (e.g. "/offer")
     * @param data             payload (must be msgpack-serialisable)
     * @param responseCallback called when a response arrives
     * @param failedCallback   called when the request fails or times out
     */
    void request(String path, Object data,
                 Consumer<RNSLinkRequestReceipt> responseCallback,
                 Consumer<RNSLinkRequestReceipt> failedCallback);

    /**
     * Send a request over the link with an additional progress callback.
     */
    void request(String path, Object data,
                 Consumer<RNSLinkRequestReceipt> responseCallback,
                 Consumer<RNSLinkRequestReceipt> failedCallback,
                 Consumer<RNSLinkRequestReceipt> progressCallback);

    /**
     * Transfer a resource (large payload) over this link.
     *
     * @param data             raw bytes to transfer
     * @param callback         called when the transfer completes (success or failure)
     * @param progressCallback called repeatedly with progress updates
     * @param autoCompress     whether to attempt transparent compression
     * @return the created resource object
     */
    RNSResource sendResource(byte[] data,
                             Consumer<RNSResource> callback,
                             Consumer<RNSResource> progressCallback,
                             boolean autoCompress);

    /**
     * Register a callback for raw packets received on this link.
     */
    void setPacketCallback(RNSDestination.PacketCallback callback);

    /**
     * Register a callback fired when an inbound resource transfer starts on this link.
     */
    void setResourceStartCallback(ResourceStartCallback callback);

    /**
     * Wrap this link as an {@link RNSDestination} so it can be used as a packet send target.
     */
    RNSDestination asDestination();

    @FunctionalInterface
    interface ResourceStartCallback {
        void onResourceStart(RNSResource resource);
    }
}