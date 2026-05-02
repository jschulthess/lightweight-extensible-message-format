package io.lxmf.rns;

/**
 * Receipt for a request sent over an {@link RNSLink}, carrying the response when available.
 */
public interface RNSLinkRequestReceipt {

    /** The response payload, or null if the request failed. */
    Object getResponse();

    /** The link this request was sent over. */
    RNSLink getLink();

    /** Transfer progress in [0.0, 1.0], updated during large response transfers. */
    double getProgress();
}