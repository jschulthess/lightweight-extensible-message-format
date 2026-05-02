package io.lxmf.rns.impl;

import io.lxmf.rns.RNSLink;
import io.lxmf.rns.RNSLinkRequestReceipt;
import io.reticulum.link.RequestReceipt;

/**
 * Wraps a native {@link RequestReceipt} as an {@link RNSLinkRequestReceipt}.
 *
 * <p>The native receipt carries {@code byte[]} for both request data and response, while LXMF
 * expects {@code Object}.  The response bytes are msgpack-decoded here (they were encoded by
 * {@link DestinationAdapter}'s request-handler wrapper on the server side).
 */
public final class RequestReceiptAdapter implements RNSLinkRequestReceipt {

    private final RequestReceipt receipt;

    public RequestReceiptAdapter(RequestReceipt receipt) {
        this.receipt = receipt;
    }

    @Override
    public Object getResponse() {
        if (receipt == null) return null;
        byte[] raw = receipt.getResponse();
        if (raw == null) return null;
        try {
            return MsgPackHelper.unpack(raw);
        } catch (Exception e) {
            return raw; // fall back to raw bytes if decode fails
        }
    }

    @Override
    public RNSLink getLink() {
        io.reticulum.link.Link link = receipt != null ? receipt.getLink() : null;
        return link != null ? new LinkAdapter(link) : null;
    }

    @Override
    public double getProgress() {
        return receipt != null ? receipt.getProgress() : 0.0;
    }
}
