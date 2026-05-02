package io.lxmf.rns;

/**
 * Represents a Reticulum resource transfer (reliable large-data delivery over a link).
 */
public interface RNSResource {

    int COMPLETE = 0x01;
    int REJECTED = 0x02;
    int FAILED   = 0x03;

    /** Current transfer status: one of {@link #COMPLETE}, {@link #REJECTED}, {@link #FAILED}. */
    int getStatus();

    /** Returns the link over which this resource is being transferred. */
    RNSLink getLink();

    /** Transfer progress in [0.0, 1.0]. */
    double getProgress();

    /** Number of bytes actually transferred on the wire (after compression, framing, etc.). */
    long getTransferSize();

    /** Number of original payload bytes. */
    long getDataSize();

    /** The reassembled payload bytes; only valid when status is {@link #COMPLETE}. */
    byte[] getData();

    /** Register a callback to be called when the transfer concludes (any status). */
    void setConcludedCallback(java.util.function.Consumer<RNSResource> callback);
}