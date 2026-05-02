package io.lxmf.rns;

/**
 * Handles a remote request received on a Reticulum link.
 */
@FunctionalInterface
public interface RNSRequestHandler {

    /**
     * @param path           the request path
     * @param data           request payload (may be null)
     * @param requestId      opaque request identifier
     * @param remoteIdentity identity of the requester, or null if unidentified
     * @param requestedAt    epoch timestamp of the request
     * @return response object to send back (must be msgpack-serialisable), or null for no response
     */
    Object handle(String path, Object data, byte[] requestId,
                  RNSIdentity remoteIdentity, double requestedAt);
}