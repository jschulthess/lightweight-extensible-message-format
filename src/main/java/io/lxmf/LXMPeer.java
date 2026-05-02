package io.lxmf;

import io.lxmf.rns.RNS;
import io.lxmf.rns.RNSDestination;
import io.lxmf.rns.RNSIdentity;
import io.lxmf.rns.RNSLink;
import io.lxmf.rns.RNSLinkRequestReceipt;
import io.lxmf.rns.RNSResource;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages synchronisation state for a single LXMF Propagation Node peer.
 *
 * <p>Faithfully translated from {@code LXMPeer.py} in the Python reference implementation.
 * Message-set tracking is stored directly in the router's propagation entries (identical to
 * Python's design) to avoid duplicating state.
 */
public class LXMPeer {

    // ── Request paths ─────────────────────────────────────────────────────────
    public static final String OFFER_REQUEST_PATH = "/offer";
    public static final String MESSAGE_GET_PATH   = "/get";

    // ── Peer states ───────────────────────────────────────────────────────────
    public static final int IDLE                  = 0x00;
    public static final int LINK_ESTABLISHING     = 0x01;
    public static final int LINK_READY            = 0x02;
    public static final int REQUEST_SENT          = 0x03;
    public static final int RESPONSE_RECEIVED     = 0x04;
    public static final int RESOURCE_TRANSFERRING = 0x05;

    // ── Error codes ───────────────────────────────────────────────────────────
    public static final int ERROR_NO_IDENTITY  = 0xf0;
    public static final int ERROR_NO_ACCESS    = 0xf1;
    public static final int ERROR_INVALID_KEY  = 0xf3;
    public static final int ERROR_INVALID_DATA = 0xf4;
    public static final int ERROR_INVALID_STAMP= 0xf5;
    public static final int ERROR_THROTTLED    = 0xf6;
    public static final int ERROR_NOT_FOUND    = 0xfd;
    public static final int ERROR_TIMEOUT      = 0xfe;

    // ── Sync strategies ───────────────────────────────────────────────────────
    public static final int STRATEGY_LAZY       = 0x01;
    public static final int STRATEGY_PERSISTENT = 0x02;
    public static final int DEFAULT_SYNC_STRATEGY = STRATEGY_PERSISTENT;

    // ── Timing ────────────────────────────────────────────────────────────────
    /** Max unreachability before a peer is dropped: 14 days. */
    public static final long MAX_UNREACHABLE   = 14L * 24 * 60 * 60;
    /** Back-off added for each consecutive link failure: 12 minutes. */
    public static final long SYNC_BACKOFF_STEP = 12L * 60;
    /** Grace period after a path request before giving up: 7.5 seconds. */
    public static final double PATH_REQUEST_GRACE = 7.5;

    // ── Fields ────────────────────────────────────────────────────────────────
    public byte[]  destinationHash;
    public RNSIdentity  identity;
    public RNSDestination destination;

    public boolean alive            = false;
    public double  lastHeard        = 0;
    public double  nextSyncAttempt  = 0;
    public double  lastSyncAttempt  = 0;
    public double  syncBackoff      = 0;
    public long    peeringTimebase  = 0;
    public double  linkEstablishmentRate = 0;
    public double  syncTransferRate = 0;

    public Double   propagationTransferLimit;
    public Integer  propagationSyncLimit;
    public Integer  propagationStampCost;
    public Integer  propagationStampCostFlexibility;
    public Integer  peeringCost;
    public int      syncStrategy;

    /** [stamp_bytes, value] or null. */
    public Object[] peeringKey;
    public Map<Integer, Object> metadata;

    public int offered  = 0;
    public int outgoing = 0;
    public int incoming = 0;
    public long rxBytes = 0;
    public long txBytes = 0;

    public int     state = IDLE;
    public RNSLink link;

    /** Transient IDs currently being transferred to this peer. */
    public List<byte[]> currentlyTransferringMessages;
    public Double       currentSyncTransferStarted;

    /** Last offer sent to this peer (list of transient IDs). */
    public List<byte[]> lastOffer = new ArrayList<>();

    private final Deque<byte[]> handledMessagesQueue   = new ArrayDeque<>();
    private final Deque<byte[]> unhandledMessagesQueue = new ArrayDeque<>();

    private int _hmCount = 0, _umCount = 0;
    private boolean _hmCountsSynced = false, _umCountsSynced = false;

    private final ReentrantLock peeringKeyLock = new ReentrantLock();

    final LXMRouter router;

    // ── Construction ──────────────────────────────────────────────────────────

    public LXMPeer(LXMRouter router, byte[] destinationHash) {
        this(router, destinationHash, DEFAULT_SYNC_STRATEGY);
    }

    public LXMPeer(LXMRouter router, byte[] destinationHash, int syncStrategy) {
        this.router           = router;
        this.destinationHash  = destinationHash;
        this.syncStrategy     = syncStrategy;

        this.identity = RNS.recallIdentity(destinationHash);
        if (this.identity != null) {
            this.destination = RNS.createDestination(identity, RNSDestination.OUT, RNSDestination.SINGLE,
                    LXMF.APP_NAME, "propagation");
        } else {
            RNS.log("Could not recall identity for LXMF propagation peer "
                    + RNS.prettyhexrep(destinationHash) + ", will retry on next sync", RNS.LOG_WARNING);
        }
    }

    // ── Peering key ───────────────────────────────────────────────────────────

    public boolean peeringKeyReady() {
        if (peeringCost == null) return false;
        if (peeringKey != null && peeringKey.length == 2) {
            int value = (int) peeringKey[1];
            if (value >= peeringCost) return true;
            RNS.log("Peering key value mismatch for " + this + ". Scheduling regeneration...", RNS.LOG_WARNING);
            peeringKey = null;
        }
        return false;
    }

    public Integer peeringKeyValue() {
        if (peeringKey != null && peeringKey.length == 2) return (int) peeringKey[1];
        return null;
    }

    public boolean generatePeeringKey() {
        if (peeringCost == null) return false;
        peeringKeyLock.lock();
        try {
            if (peeringKey != null) return true;
            RNS.log("Generating peering key for " + this, RNS.LOG_NOTICE);

            if (router.getIdentity() == null) {
                RNS.log("Could not update peering key for " + this
                        + " since the local LXMF router identity is not configured", RNS.LOG_ERROR);
                return false;
            }
            if (identity == null) {
                identity = RNS.recallIdentity(destinationHash);
                if (identity == null) {
                    RNS.log("Could not update peering key for " + this
                            + " since its identity could not be recalled", RNS.LOG_ERROR);
                    return false;
                }
            }

            byte[] keyMaterial = RNS.concat(identity.getHash(), router.getIdentity().getHash());
            LXStamper.GeneratedStamp gs = LXStamper.generateStamp(
                    keyMaterial, peeringCost, LXStamper.WORKBLOCK_EXPAND_ROUNDS_PEERING);
            if (gs != null && gs.value >= peeringCost) {
                this.peeringKey = new Object[]{gs.stamp, gs.value};
                RNS.log("Peering key successfully generated for " + this, RNS.LOG_NOTICE);
                return true;
            }
            return false;
        } finally {
            peeringKeyLock.unlock();
        }
    }

    // ── Sync ──────────────────────────────────────────────────────────────────

    public void sync() {
        RNS.log("Initiating LXMF Propagation Node sync with peer " + RNS.prettyhexrep(destinationHash),
                RNS.LOG_DEBUG);
        lastSyncAttempt = System.currentTimeMillis() / 1000.0;

        boolean syncTimeReached  = (System.currentTimeMillis() / 1000.0) > nextSyncAttempt;
        boolean stampCostsKnown  = propagationStampCost != null
                && propagationStampCostFlexibility != null && peeringCost != null;
        boolean peeringKeyIsReady = peeringKeyReady();

        if (!syncTimeReached || !stampCostsKnown || !peeringKeyIsReady) {
            String reason;
            if (!syncTimeReached) {
                reason = " due to previous failures";
                if (lastSyncAttempt > lastHeard) alive = false;
            } else if (!stampCostsKnown) {
                reason = " since its required stamp costs are not yet known";
            } else {
                reason = " since a peering key has not been generated yet";
                Thread t = new Thread(this::generatePeeringKey);
                t.setDaemon(true); t.start();
            }
            RNS.log("Postponing sync with peer " + RNS.prettyhexrep(destinationHash) + reason, RNS.LOG_DEBUG);
            return;
        }

        if (!RNS.hasPath(destinationHash)) {
            RNS.log("No path to peer " + RNS.prettyhexrep(destinationHash) + " exists, requesting...", RNS.LOG_DEBUG);
            RNS.requestPath(destinationHash);
            try { Thread.sleep((long)(PATH_REQUEST_GRACE * 1000)); } catch (InterruptedException e) { return; }
        }

        if (!RNS.hasPath(destinationHash)) {
            RNS.log("Path request was not answered, retrying sync later", RNS.LOG_DEBUG);
            return;
        }

        if (identity == null) {
            identity = RNS.recallIdentity(destinationHash);
            if (identity != null) {
                destination = RNS.createDestination(identity, RNSDestination.OUT, RNSDestination.SINGLE,
                        LXMF.APP_NAME, "propagation");
            }
        }

        if (destination == null) {
            RNS.log("Could not request sync to peer " + RNS.prettyhexrep(destinationHash)
                    + " since its identity could not be recalled.", RNS.LOG_ERROR);
            return;
        }

        List<byte[]> unhandled = getUnhandledMessages();
        if (unhandled.isEmpty()) {
            RNS.log("Sync requested for " + this + ", but no unhandled messages exist. Sync complete.", RNS.LOG_DEBUG);
            return;
        }

        if (currentlyTransferringMessages != null) {
            RNS.log("Sync requested for " + this + ", but transfer index was not clear. Aborting.", RNS.LOG_ERROR);
            return;
        }

        if (state == IDLE) {
            RNS.log("Establishing link for sync to peer " + RNS.prettyhexrep(destinationHash) + "...", RNS.LOG_DEBUG);
            syncBackoff += SYNC_BACKOFF_STEP;
            nextSyncAttempt = (System.currentTimeMillis() / 1000.0) + syncBackoff;
            this.link = RNS.createLink(destination, this::linkEstablished, this::linkClosed);
            state = LINK_ESTABLISHING;

        } else if (state == LINK_READY) {
            alive     = true;
            lastHeard = System.currentTimeMillis() / 1000.0;
            syncBackoff = 0;

            int minAcceptedCost = Math.min(0,
                    propagationStampCost - propagationStampCostFlexibility);

            RNS.log("Sync link to peer " + RNS.prettyhexrep(destinationHash) + " ready, preparing offer...", RNS.LOG_DEBUG);

            List<byte[]> purgedIds    = new ArrayList<>();
            List<byte[]> lowValueIds  = new ArrayList<>();

            List<UnhandledEntry> entries = new ArrayList<>();

            for (byte[] tid : unhandled) {
                if (router.hasPropagationEntry(tid)) {
                    Integer sv = router.getStampValue(tid);
                    if (sv != null && sv < minAcceptedCost) lowValueIds.add(tid);
                    else entries.add(new UnhandledEntry(tid, router.getWeight(tid), router.getSize(tid)));
                } else {
                    purgedIds.add(tid);
                }
            }

            for (byte[] tid : purgedIds) {
                RNS.log("Dropping unhandled message " + RNS.prettyhexrep(tid)
                        + " for peer " + RNS.prettyhexrep(destinationHash)
                        + " since it no longer exists in the message store.", RNS.LOG_DEBUG);
                removeUnhandledMessage(tid);
            }
            for (byte[] tid : lowValueIds) {
                RNS.log("Dropping low-value message " + RNS.prettyhexrep(tid)
                        + " for peer " + RNS.prettyhexrep(destinationHash), RNS.LOG_DEBUG);
                removeUnhandledMessage(tid);
            }

            entries.sort((a, b) -> Double.compare(a.weight(), b.weight()));

            int perMsgOverhead = 16;
            int cumSize = 24;
            List<byte[]> offerIds = new ArrayList<>();

            for (UnhandledEntry e : entries) {
                long transferSize = e.size() + perMsgOverhead;
                long nextSize     = cumSize + transferSize;

                if (propagationTransferLimit != null && transferSize > propagationTransferLimit * 1000) {
                    addHandledMessage(e.id()); removeUnhandledMessage(e.id()); continue;
                }
                if (propagationSyncLimit != null && nextSize >= propagationSyncLimit * 1000) continue;

                cumSize += transferSize;
                offerIds.add(e.id());
            }

            byte[] peeringKeyBytes = (byte[]) peeringKey[0];
            Object offer = new Object[]{peeringKeyBytes, offerIds};
            RNS.log("Offering " + offerIds.size() + " messages to peer "
                    + RNS.prettyhexrep(destinationHash), RNS.LOG_VERBOSE);
            lastOffer = offerIds;
            link.request(OFFER_REQUEST_PATH, offer, this::offerResponse, this::requestFailed);
            state = REQUEST_SENT;
        }
    }

    public void requestFailed(RNSLinkRequestReceipt receipt) {
        RNS.log("Sync request to peer " + destination + " failed", RNS.LOG_DEBUG);
        if (link != null) link.teardown();
        state = IDLE;
    }

    @SuppressWarnings("unchecked")
    public void offerResponse(RNSLinkRequestReceipt receipt) {
        try {
            state = RESPONSE_RECEIVED;
            Object response = receipt.getResponse();

            List<byte[]> wantedMessages   = new ArrayList<>();
            List<byte[]> wantedMessageIds = new ArrayList<>();

            if (response instanceof Integer) {
                int code = (Integer) response;
                if (code == ERROR_NO_IDENTITY) {
                    RNS.log("Remote peer indicated no identification received, retrying...", RNS.LOG_VERBOSE);
                    link.identify(router.getIdentity());
                    state = LINK_READY;
                    sync();
                    return;
                } else if (code == ERROR_NO_ACCESS) {
                    RNS.log("Remote indicated access denied, breaking peering", RNS.LOG_VERBOSE);
                    router.unpeer(destinationHash, 0);
                    return;
                } else if (code == ERROR_THROTTLED) {
                    long throttle = LXMRouter.PN_STAMP_THROTTLE;
                    RNS.log("Remote indicated we're throttled, postponing sync for "
                            + RNS.prettytime(throttle), RNS.LOG_VERBOSE);
                    nextSyncAttempt = (System.currentTimeMillis() / 1000.0) + throttle;
                    return;
                }
            } else if (response instanceof Boolean && !(Boolean) response) {
                // Peer already has all advertised messages
                for (byte[] tid : lastOffer) {
                    addHandledMessage(tid);
                    removeUnhandledMessage(tid);
                }
            } else if (response instanceof Boolean && (Boolean) response) {
                // Peer wants everything
                for (byte[] tid : lastOffer) {
                    wantedMessages.add(tid);
                    wantedMessageIds.add(tid);
                }
            } else if (response instanceof List) {
                // Peer wants a subset
                List<byte[]> wantedIds = (List<byte[]>) response;
                for (byte[] tid : lastOffer) {
                    boolean wanted = false;
                    for (byte[] wid : wantedIds) {
                        if (Arrays.equals(tid, wid)) { wanted = true; break; }
                    }
                    if (!wanted) {
                        addHandledMessage(tid); removeUnhandledMessage(tid);
                    } else {
                        wantedMessages.add(tid); wantedMessageIds.add(tid);
                    }
                }
            }

            if (!wantedMessages.isEmpty()) {
                RNS.log("Peer " + RNS.prettyhexrep(destinationHash) + " wanted "
                        + wantedMessages.size() + " messages", RNS.LOG_VERBOSE);

                List<byte[]> lxmList = new ArrayList<>();
                for (byte[] tid : wantedMessages) {
                    String filePath = router.getPropagationEntryPath(tid);
                    if (filePath != null) {
                        try {
                            byte[] data = Files.readAllBytes(Paths.get(filePath));
                            lxmList.add(data);
                        } catch (Exception e) {
                            RNS.log("Could not read message file " + filePath + ": " + e.getMessage(), RNS.LOG_ERROR);
                        }
                    }
                }

                byte[] data = packTransferBundle(lxmList);
                RNS.log("Total transfer size for this sync is " + RNS.prettysize(data.length), RNS.LOG_VERBOSE);
                link.sendResource(data, this::resourceConcluded, null, false);
                currentlyTransferringMessages = wantedMessageIds;
                currentSyncTransferStarted    = System.currentTimeMillis() / 1000.0;
                state = RESOURCE_TRANSFERRING;

            } else {
                RNS.log("Peer " + RNS.prettyhexrep(destinationHash)
                        + " did not request any messages, sync complete", RNS.LOG_VERBOSE);
                offered += lastOffer.size();
                if (link != null) link.teardown();
                link  = null;
                state = IDLE;
            }

        } catch (Exception e) {
            RNS.log("Error handling offer response from peer " + destination + ": " + e.getMessage(), RNS.LOG_ERROR);
            if (link != null) link.teardown();
            link  = null;
            state = IDLE;
        }
    }

    public void resourceConcluded(RNSResource resource) {
        if (resource.getStatus() == RNSResource.COMPLETE) {
            if (currentlyTransferringMessages == null) {
                RNS.log("Sync transfer completed on " + this
                        + " but transferred message index unavailable. Aborting.", RNS.LOG_ERROR);
                if (link != null) link.teardown();
                link = null; state = IDLE; return;
            }

            for (byte[] tid : currentlyTransferringMessages) {
                addHandledMessage(tid); removeUnhandledMessage(tid);
            }
            if (link != null) link.teardown();
            link  = null;
            state = IDLE;

            if (currentSyncTransferStarted != null) {
                double elapsed = (System.currentTimeMillis() / 1000.0) - currentSyncTransferStarted;
                syncTransferRate = (resource.getTransferSize() * 8) / elapsed;
                RNS.log("Syncing " + currentlyTransferringMessages.size() + " messages to peer "
                        + RNS.prettyhexrep(destinationHash) + " completed at "
                        + RNS.prettyspeed(syncTransferRate), RNS.LOG_VERBOSE);
            }

            alive     = true;
            lastHeard = System.currentTimeMillis() / 1000.0;
            offered  += lastOffer.size();
            outgoing += currentlyTransferringMessages.size();
            txBytes  += resource.getDataSize();

            currentlyTransferringMessages = null;
            currentSyncTransferStarted    = null;

            if (syncStrategy == STRATEGY_PERSISTENT && getUnhandledMessageCount() > 0) sync();

        } else {
            RNS.log("Resource transfer for peer sync failed to " + destination, RNS.LOG_VERBOSE);
            if (link != null) link.teardown();
            link  = null;
            state = IDLE;
            currentlyTransferringMessages = null;
            currentSyncTransferStarted    = null;
        }
    }

    public void linkEstablished(RNSLink lnk) {
        this.link = lnk;
        link.identify(router.getIdentity());
        double rate = link.getEstablishmentRate();
        if (rate > 0) this.linkEstablishmentRate = rate;
        state = LINK_READY;
        nextSyncAttempt = 0;
        sync();
    }

    public void linkClosed(RNSLink lnk) {
        link  = null;
        state = IDLE;
    }

    // ── Message queue management ──────────────────────────────────────────────

    public boolean hasQueuedItems() {
        return !handledMessagesQueue.isEmpty() || !unhandledMessagesQueue.isEmpty();
    }

    public void queueHandledMessage(byte[] transientId)   { handledMessagesQueue.addLast(transientId); }
    public void queueUnhandledMessage(byte[] transientId) { unhandledMessagesQueue.addLast(transientId); }

    public void processQueues() {
        if (handledMessagesQueue.isEmpty() && unhandledMessagesQueue.isEmpty()) return;
        List<byte[]> handled   = getHandledMessages();
        List<byte[]> unhandled = getUnhandledMessages();

        while (!handledMessagesQueue.isEmpty()) {
            byte[] tid = handledMessagesQueue.pollLast();
            if (!containsId(handled, tid))   addHandledMessage(tid);
            if (containsId(unhandled, tid))   removeUnhandledMessage(tid);
        }
        while (!unhandledMessagesQueue.isEmpty()) {
            byte[] tid = unhandledMessagesQueue.pollLast();
            if (!containsId(handled, tid) && !containsId(unhandled, tid))
                addUnhandledMessage(tid);
        }
    }

    // ── Propagation-entry-backed message tracking ─────────────────────────────

    /** Messages this peer has already received. */
    public List<byte[]> getHandledMessages() {
        List<byte[]> result = new ArrayList<>();
        for (Map.Entry<byte[], LXMRouter.PropagationEntry> e : router.getPropagationEntries().entrySet()) {
            if (containsId(e.getValue().handledPeers, destinationHash)) result.add(e.getKey());
        }
        _hmCount = result.size(); _hmCountsSynced = true;
        return result;
    }

    /** Messages this peer has not yet received. */
    public List<byte[]> getUnhandledMessages() {
        List<byte[]> result = new ArrayList<>();
        for (Map.Entry<byte[], LXMRouter.PropagationEntry> e : router.getPropagationEntries().entrySet()) {
            if (containsId(e.getValue().unhandledPeers, destinationHash)) result.add(e.getKey());
        }
        _umCount = result.size(); _umCountsSynced = true;
        return result;
    }

    public int getHandledMessageCount() {
        if (!_hmCountsSynced) getHandledMessages();
        return _hmCount;
    }

    public int getUnhandledMessageCount() {
        if (!_umCountsSynced) getUnhandledMessages();
        return _umCount;
    }

    public double getAcceptanceRate() {
        return offered == 0 ? 0 : (double) outgoing / offered;
    }

    public void addHandledMessage(byte[] transientId) {
        LXMRouter.PropagationEntry e = router.getPropagationEntry(transientId);
        if (e != null && !containsId(e.handledPeers, destinationHash)) {
            e.handledPeers.add(destinationHash);
            _hmCountsSynced = false;
        }
    }

    public void addUnhandledMessage(byte[] transientId) {
        LXMRouter.PropagationEntry e = router.getPropagationEntry(transientId);
        if (e != null && !containsId(e.unhandledPeers, destinationHash)) {
            e.unhandledPeers.add(destinationHash);
            _umCount++;
        }
    }

    public void removeHandledMessage(byte[] transientId) {
        LXMRouter.PropagationEntry e = router.getPropagationEntry(transientId);
        if (e != null) {
            e.handledPeers.removeIf(h -> Arrays.equals(h, destinationHash));
            _hmCountsSynced = false;
        }
    }

    public void removeUnhandledMessage(byte[] transientId) {
        LXMRouter.PropagationEntry e = router.getPropagationEntry(transientId);
        if (e != null) {
            e.unhandledPeers.removeIf(h -> Arrays.equals(h, destinationHash));
            _umCountsSynced = false;
        }
    }

    // ── Metadata ──────────────────────────────────────────────────────────────

    public String getName() {
        if (!(metadata instanceof Map)) return null;
        Object v = metadata.get(LXMF.PN_META_NAME);
        if (v instanceof byte[]) return new String((byte[]) v, java.nio.charset.StandardCharsets.UTF_8);
        return null;
    }

    // ── Serialization ─────────────────────────────────────────────────────────

    public byte[] toBytes() {
        try {
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
            // 22 entries: 20 scalar fields + handled_ids + unhandled_ids
            packer.packMapHeader(22);

            packEntry(packer, "destination_hash", destinationHash);
            packEntry(packer, "peering_timebase",  peeringTimebase);
            packEntry(packer, "alive",             alive);
            packEntry(packer, "last_heard",        lastHeard);
            packEntry(packer, "sync_strategy",     syncStrategy);

            // peering_key: nil or 2-element array [stamp:bytes, value:long]
            packer.packString("peering_key");
            if (peeringKey == null) {
                packer.packNil();
            } else {
                packer.packArrayHeader(2);
                byte[] stamp = (byte[]) peeringKey[0];
                packer.packBinaryHeader(stamp.length); packer.writePayload(stamp);
                long val = peeringKey[1] instanceof Number ? ((Number) peeringKey[1]).longValue() : 0L;
                packer.packLong(val);
            }

            // metadata: nil or map of int→bytes
            packer.packString("metadata");
            if (metadata == null) {
                packer.packNil();
            } else {
                packer.packMapHeader(metadata.size());
                for (Map.Entry<Integer, Object> e : metadata.entrySet()) {
                    packer.packInt(e.getKey());
                    Object v = e.getValue();
                    if (v instanceof byte[]) {
                        byte[] b = (byte[]) v;
                        packer.packBinaryHeader(b.length); packer.writePayload(b);
                    } else if (v == null) {
                        packer.packNil();
                    } else {
                        String s = v.toString();
                        packer.packString(s);
                    }
                }
            }

            packEntry(packer, "link_establishment_rate", linkEstablishmentRate);
            packEntry(packer, "sync_transfer_rate",       syncTransferRate);

            // propagation_transfer_limit: nil or double
            packer.packString("propagation_transfer_limit");
            if (propagationTransferLimit == null) packer.packNil();
            else packer.packDouble(propagationTransferLimit);

            packEntryNullableInt(packer, "propagation_sync_limit",             propagationSyncLimit);
            packEntryNullableInt(packer, "propagation_stamp_cost",             propagationStampCost);
            packEntryNullableInt(packer, "propagation_stamp_cost_flexibility", propagationStampCostFlexibility);
            packEntryNullableInt(packer, "peering_cost",                       peeringCost);
            packEntry(packer, "last_sync_attempt", lastSyncAttempt);
            packEntry(packer, "offered",  offered);
            packEntry(packer, "outgoing", outgoing);
            packEntry(packer, "incoming", incoming);
            packEntry(packer, "rx_bytes", rxBytes);
            packEntry(packer, "tx_bytes", txBytes);

            // handled / unhandled IDs — entries 21 and 22 of the map
            List<byte[]> hm = getHandledMessages();
            List<byte[]> um = getUnhandledMessages();
            packer.packString("handled_ids");
            packer.packArrayHeader(hm.size());
            for (byte[] id : hm) { packer.packBinaryHeader(id.length); packer.writePayload(id); }
            packer.packString("unhandled_ids");
            packer.packArrayHeader(um.size());
            for (byte[] id : um) { packer.packBinaryHeader(id.length); packer.writePayload(id); }

            return packer.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static LXMPeer fromBytes(byte[] data, LXMRouter router) {
        try (MessageUnpacker up = MessagePack.newDefaultUnpacker(data)) {
            int mapSize = up.unpackMapHeader();
            byte[] destHash = null; long timebase = 0; boolean alive = false;
            double lastHeard = 0, ler = 0, str = 0, lsa = 0;
            Double ptl = null; Integer psl = null, psc = null, pscf = null, pc = null, ss = null;
            int offered = 0, outgoing = 0, incoming = 0;
            long rx = 0, tx = 0;
            Object[] peeringKey = null;
            Map<Integer, Object> metadata = null;
            List<byte[]> handledIds = new ArrayList<>(), unhandledIds = new ArrayList<>();

            for (int i = 0; i < mapSize; i++) {
                String key = up.unpackString();
                switch (key) {
                    case "destination_hash": destHash = up.readPayload(up.unpackBinaryHeader()); break;
                    case "peering_timebase": timebase = up.unpackLong(); break;
                    case "alive": alive = up.unpackBoolean(); break;
                    case "last_heard": lastHeard = up.unpackDouble(); break;
                    case "link_establishment_rate": ler = up.unpackDouble(); break;
                    case "sync_transfer_rate": str = up.unpackDouble(); break;
                    case "propagation_transfer_limit":
                        if (up.getNextFormat() == MessageFormat.NIL) { up.unpackNil(); }
                        else { try { ptl = up.unpackDouble(); } catch (Exception e) { up.skipValue(); } }
                        break;
                    case "propagation_sync_limit":
                        if (up.getNextFormat() == MessageFormat.NIL) { up.unpackNil(); }
                        else { try { psl = (int) up.unpackLong(); } catch (Exception e) { up.skipValue(); } }
                        break;
                    case "propagation_stamp_cost":
                        if (up.getNextFormat() == MessageFormat.NIL) { up.unpackNil(); }
                        else { try { psc = (int) up.unpackLong(); } catch (Exception e) { up.skipValue(); } }
                        break;
                    case "propagation_stamp_cost_flexibility":
                        if (up.getNextFormat() == MessageFormat.NIL) { up.unpackNil(); }
                        else { try { pscf = (int) up.unpackLong(); } catch (Exception e) { up.skipValue(); } }
                        break;
                    case "peering_cost":
                        if (up.getNextFormat() == MessageFormat.NIL) { up.unpackNil(); }
                        else { try { pc = (int) up.unpackLong(); } catch (Exception e) { up.skipValue(); } }
                        break;
                    case "sync_strategy": ss = (int) up.unpackLong(); break;
                    case "last_sync_attempt": lsa = up.unpackDouble(); break;
                    case "offered":  offered  = (int) up.unpackLong(); break;
                    case "outgoing": outgoing = (int) up.unpackLong(); break;
                    case "incoming": incoming = (int) up.unpackLong(); break;
                    case "rx_bytes": rx = up.unpackLong(); break;
                    case "tx_bytes": tx = up.unpackLong(); break;
                    case "peering_key":
                        if (up.getNextFormat() == MessageFormat.NIL) {
                            up.unpackNil();
                        } else {
                            int pkSize = up.unpackArrayHeader();
                            if (pkSize >= 2) {
                                byte[] stamp = up.readPayload(up.unpackBinaryHeader());
                                long val = up.unpackLong();
                                peeringKey = new Object[]{stamp, val};
                                for (int j = 2; j < pkSize; j++) up.skipValue();
                            } else {
                                for (int j = 0; j < pkSize; j++) up.skipValue();
                            }
                        }
                        break;
                    case "metadata":
                        if (up.getNextFormat() == MessageFormat.NIL) {
                            up.unpackNil();
                        } else {
                            int mSize = up.unpackMapHeader();
                            metadata = new java.util.LinkedHashMap<>();
                            for (int j = 0; j < mSize; j++) {
                                int mk = up.unpackInt();
                                if (up.getNextFormat() == MessageFormat.NIL) {
                                    up.unpackNil(); metadata.put(mk, null);
                                } else {
                                    metadata.put(mk, up.readPayload(up.unpackBinaryHeader()));
                                }
                            }
                        }
                        break;
                    case "handled_ids": {
                        int n = up.unpackArrayHeader();
                        for (int j = 0; j < n; j++) handledIds.add(up.readPayload(up.unpackBinaryHeader()));
                        break;
                    }
                    case "unhandled_ids": {
                        int n = up.unpackArrayHeader();
                        for (int j = 0; j < n; j++) unhandledIds.add(up.readPayload(up.unpackBinaryHeader()));
                        break;
                    }
                    default: up.skipValue(); break;
                }
            }

            if (destHash == null) return null;
            LXMPeer peer = new LXMPeer(router, destHash,
                    ss != null ? ss : DEFAULT_SYNC_STRATEGY);
            peer.peeringTimebase        = timebase;
            peer.alive                  = alive;
            peer.lastHeard              = lastHeard;
            peer.linkEstablishmentRate  = ler;
            peer.syncTransferRate       = str;
            peer.propagationTransferLimit = ptl;
            peer.propagationSyncLimit   = psl;
            peer.propagationStampCost   = psc;
            peer.propagationStampCostFlexibility = pscf;
            peer.peeringCost            = pc;
            peer.lastSyncAttempt        = lsa;
            peer.offered                = offered;
            peer.outgoing               = outgoing;
            peer.incoming               = incoming;
            peer.rxBytes                = rx;
            peer.txBytes                = tx;
            peer.peeringKey             = peeringKey;
            peer.metadata               = metadata;

            int hmCount = 0, umCount = 0;
            for (byte[] tid : handledIds) {
                if (router.hasPropagationEntry(tid)) {
                    peer.addHandledMessage(tid); hmCount++;
                }
            }
            for (byte[] tid : unhandledIds) {
                if (router.hasPropagationEntry(tid)) {
                    peer.addUnhandledMessage(tid); umCount++;
                }
            }
            peer._hmCount = hmCount; peer._hmCountsSynced = true;
            peer._umCount = umCount; peer._umCountsSynced = true;

            return peer;
        } catch (Exception e) {
            RNS.log("Could not deserialize LXMPeer: " + e.getMessage(), RNS.LOG_ERROR);
            return null;
        }
    }

    // ── toString ──────────────────────────────────────────────────────────────

    @Override public String toString() {
        return destinationHash != null ? RNS.prettyhexrep(destinationHash) : "<Unknown>";
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private static boolean containsId(List<byte[]> list, byte[] id) {
        for (byte[] b : list) if (Arrays.equals(b, id)) return true;
        return false;
    }

    private static byte[] packTransferBundle(List<byte[]> lxmList) {
        try {
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
            packer.packArrayHeader(2);
            packer.packDouble(System.currentTimeMillis() / 1000.0);
            packer.packArrayHeader(lxmList.size());
            for (byte[] item : lxmList) {
                packer.packBinaryHeader(item.length);
                packer.writePayload(item);
            }
            return packer.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void packEntry(MessageBufferPacker p, String k, byte[] v) throws IOException {
        p.packString(k);
        if (v == null) p.packNil(); else { p.packBinaryHeader(v.length); p.writePayload(v); }
    }

    private static void packEntry(MessageBufferPacker p, String k, long v) throws IOException {
        p.packString(k); p.packLong(v);
    }

    private static void packEntry(MessageBufferPacker p, String k, double v) throws IOException {
        p.packString(k); p.packDouble(v);
    }

    private static void packEntry(MessageBufferPacker p, String k, boolean v) throws IOException {
        p.packString(k); p.packBoolean(v);
    }

    private static final class UnhandledEntry {
        final byte[] id;
        final double weight;
        final long size;
        UnhandledEntry(byte[] id, double weight, long size) {
            this.id = id; this.weight = weight; this.size = size;
        }
        byte[] id()     { return id; }
        double weight() { return weight; }
        long   size()   { return size; }
    }

    private static void packEntry(MessageBufferPacker p, String k, int v) throws IOException {
        p.packString(k); p.packInt(v);
    }

    private static void packEntryNullable(MessageBufferPacker p, String k, Object v) throws IOException {
        p.packString(k); if (v == null) p.packNil(); else p.packString(v.toString());
    }

    private static void packEntryNullableInt(MessageBufferPacker p, String k, Integer v) throws IOException {
        p.packString(k); if (v == null) p.packNil(); else p.packInt(v);
    }
}
