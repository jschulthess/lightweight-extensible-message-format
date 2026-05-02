package io.lxmf.handlers;

import io.lxmf.LXMF;
import io.lxmf.LXMessage;
import io.lxmf.LXMRouter;
import io.lxmf.rns.RNS;
import io.lxmf.rns.RNSAnnounceHandler;
import io.lxmf.rns.RNSIdentity;

import java.util.List;

/**
 * Processes incoming LXMF delivery-destination announces.
 *
 * <p>When a delivery announce is received:
 * <ol>
 *   <li>The announced stamp cost is cached in the router.</li>
 *   <li>Any pending outbound messages for that destination are retriggered.</li>
 * </ol>
 *
 * <p>Mirrors {@code LXMFDeliveryAnnounceHandler} in the Python reference.
 */
public class LXMFDeliveryAnnounceHandler implements RNSAnnounceHandler {

    private final String aspectFilter;
    private final LXMRouter lxmRouter;

    public LXMFDeliveryAnnounceHandler(LXMRouter lxmRouter) {
        this.lxmRouter    = lxmRouter;
        this.aspectFilter = LXMF.APP_NAME + ".delivery";
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
            Integer stampCost = LXMF.stampCostFromAppData(appData);
            lxmRouter.updateStampCost(destinationHash, stampCost);
        } catch (Exception e) {
            RNS.log("An error occurred while trying to decode announced stamp cost: " + e.getMessage(),
                    RNS.LOG_ERROR);
        }

        // Retrigger delivery for any pending outbound message targeting this destination
        List<LXMessage> pending = lxmRouter.getPendingOutbound();
        for (LXMessage lxm : pending) {
            if (java.util.Arrays.equals(destinationHash, lxm.getDestinationHash())) {
                if (lxm.getMethod() == LXMessage.DIRECT || lxm.getMethod() == LXMessage.OPPORTUNISTIC) {
                    lxm.setNextDeliveryAttempt(System.currentTimeMillis() / 1000.0);

                    Thread trigger = new Thread(() -> {
                        while (lxmRouter.isOutboundProcessingLocked()) {
                            try { Thread.sleep(100); } catch (InterruptedException ie) { return; }
                        }
                        lxmRouter.processOutbound();
                    });
                    trigger.setDaemon(true);
                    trigger.start();
                }
            }
        }
    }
}