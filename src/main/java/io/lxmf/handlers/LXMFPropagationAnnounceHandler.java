package io.lxmf.handlers;

import io.lxmf.LXMF;
import io.lxmf.LXMRouter;
import io.lxmf.rns.RNS;
import io.lxmf.rns.RNSAnnounceHandler;
import io.lxmf.rns.RNSIdentity;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Processes incoming LXMF propagation-node announces.
 *
 * <p>When the local node is a propagation node this handler:
 * <ul>
 *   <li>Validates the announce payload structure.</li>
 *   <li>Auto-peers with newly discovered nodes (if within depth and autopeer is enabled).</li>
 *   <li>Updates parameters for already-peered nodes.</li>
 *   <li>Unpeers nodes that advertise propagation as disabled.</li>
 * </ul>
 *
 * <p>Mirrors {@code LXMFPropagationAnnounceHandler} in the Python reference.
 */
public class LXMFPropagationAnnounceHandler implements RNSAnnounceHandler {

    private final String aspectFilter;
    private final LXMRouter lxmRouter;

    public LXMFPropagationAnnounceHandler(LXMRouter lxmRouter) {
        this.lxmRouter    = lxmRouter;
        this.aspectFilter = LXMF.APP_NAME + ".propagation";
    }

    @Override
    public String getAspectFilter() {
        return aspectFilter;
    }

    @Override
    public boolean getReceivePathResponses() {
        return true;
    }

    @Override
    public void receivedAnnounce(byte[] destinationHash, RNSIdentity announcedIdentity,
                                 byte[] appData, byte[] announcePacketHash, boolean isPathResponse) {
        try {
            if (appData == null || !lxmRouter.isPropagationNode()) return;
            if (!LXMF.pnAnnounceDataIsValid(appData)) return;

            // Parse the announce payload
            long nodeTimebase;
            boolean propagationEnabled;
            int propagationTransferLimit, propagationSyncLimit;
            int propagationStampCost, propagationStampCostFlexibility, peeringCost;
            Map<Integer, Object> metadata;

            try (MessageUnpacker up = MessagePack.newDefaultUnpacker(appData)) {
                int size = up.unpackArrayHeader();
                up.skipValue();                                // 0: legacy flag
                nodeTimebase                      = up.unpackLong();  // 1
                propagationEnabled                = up.unpackBoolean(); // 2
                propagationTransferLimit          = (int) up.unpackLong(); // 3
                propagationSyncLimit              = (int) up.unpackLong(); // 4
                int scSize = up.unpackArrayHeader();
                propagationStampCost              = (int) up.unpackLong(); // 5[0]
                propagationStampCostFlexibility   = (int) up.unpackLong(); // 5[1]
                peeringCost                       = (int) up.unpackLong(); // 5[2]
                metadata = new LinkedHashMap<>();
                int mapSize = up.unpackMapHeader();
                for (int i = 0; i < mapSize; i++) {
                    int k = up.unpackInt();
                    if (!up.hasNext()) break;
                    byte[] v = up.readPayload(up.unpackBinaryHeader());
                    metadata.put(k, v);
                }
            }

            boolean isStaticPeer = lxmRouter.isStaticPeer(destinationHash);

            if (isStaticPeer) {
                LXMRouter.PeerParams params = new LXMRouter.PeerParams(
                    nodeTimebase, propagationTransferLimit, propagationSyncLimit,
                    propagationStampCost, propagationStampCostFlexibility, peeringCost, metadata);
                if (!isPathResponse || lxmRouter.getPeerLastHeard(destinationHash) == 0) {
                    lxmRouter.peer(destinationHash, params);
                }
            } else {
                if (lxmRouter.isAutopeer() && !isPathResponse) {
                    if (propagationEnabled) {
                        if (RNS.hopsTo(destinationHash) <= lxmRouter.getAutopeerMaxdepth()) {
                            LXMRouter.PeerParams params = new LXMRouter.PeerParams(
                                nodeTimebase, propagationTransferLimit, propagationSyncLimit,
                                propagationStampCost, propagationStampCostFlexibility, peeringCost, metadata);
                            lxmRouter.peer(destinationHash, params);
                        } else {
                            if (lxmRouter.hasPeer(destinationHash)) {
                                RNS.log("Peer " + RNS.prettyhexrep(destinationHash)
                                        + " moved outside auto-peering range, breaking peering...",
                                        RNS.LOG_NOTICE);
                                lxmRouter.unpeer(destinationHash, nodeTimebase);
                            }
                        }
                    } else {
                        lxmRouter.unpeer(destinationHash, nodeTimebase);
                    }
                }
            }

        } catch (Exception e) {
            RNS.log("Error while evaluating propagation node announce, ignoring announce.",
                    RNS.LOG_DEBUG);
            RNS.log("The contained exception was: " + e.getMessage(), RNS.LOG_DEBUG);
        }
    }
}
