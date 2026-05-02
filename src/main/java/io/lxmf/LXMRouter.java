package io.lxmf;

import io.lxmf.handlers.LXMFDeliveryAnnounceHandler;
import io.lxmf.handlers.LXMFPropagationAnnounceHandler;
import io.lxmf.rns.RNS;
import io.lxmf.rns.RNSDestination;
import io.lxmf.rns.RNSIdentity;
import io.lxmf.rns.RNSLink;
import io.lxmf.rns.RNSLinkRequestReceipt;
import io.lxmf.rns.RNSPacket;
import io.lxmf.rns.RNSRequestHandler;
import io.lxmf.rns.RNSResource;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Central LXMF router: manages delivery destinations, outbound messages, and optionally
 * acts as a Propagation Node for the LXMF network.
 *
 * <p>Faithfully translated from {@code LXMRouter.py} in the Python reference implementation.
 */
public class LXMRouter {

    // ── Router constants ──────────────────────────────────────────────────────
    public static final int    MAX_DELIVERY_ATTEMPTS = 5;
    public static final int    PROCESSING_INTERVAL   = 4;   // seconds
    public static final int    DELIVERY_RETRY_WAIT   = 10;
    public static final int    PATH_REQUEST_WAIT     = 7;
    public static final int    MAX_PATHLESS_TRIES    = 1;
    public static final int    LINK_MAX_INACTIVITY   = 10 * 60;
    public static final int    P_LINK_MAX_INACTIVITY = 3  * 60;

    public static final long   MESSAGE_EXPIRY        = 30L * 24 * 60 * 60;
    public static final long   STAMP_COST_EXPIRY     = 45L * 24 * 60 * 60;

    public static final int    NODE_ANNOUNCE_DELAY   = 20;

    public static final int    MAX_PEERS             = 20;
    public static final boolean AUTOPEER             = true;
    public static final int    AUTOPEER_MAXDEPTH     = 4;
    public static final int    FASTEST_N_RANDOM_POOL = 2;
    public static final int    ROTATION_HEADROOM_PCT = 10;
    public static final double ROTATION_AR_MAX       = 0.5;

    public static final int    PEERING_COST           = 18;
    public static final int    MAX_PEERING_COST       = 26;
    public static final int    PROPAGATION_COST_MIN   = 13;
    public static final int    PROPAGATION_COST_FLEX  = 3;
    public static final int    PROPAGATION_COST       = 16;
    public static final int    PROPAGATION_LIMIT      = 256;
    public static final int    SYNC_LIMIT             = PROPAGATION_LIMIT * 40;
    public static final int    DELIVERY_LIMIT         = 1000;

    public static final int    PR_PATH_TIMEOUT        = 10;
    public static final long   PN_STAMP_THROTTLE      = 180;

    // Propagation transfer states
    public static final int PR_IDLE              = 0x00;
    public static final int PR_PATH_REQUESTED    = 0x01;
    public static final int PR_LINK_ESTABLISHING = 0x02;
    public static final int PR_LINK_ESTABLISHED  = 0x03;
    public static final int PR_REQUEST_SENT      = 0x04;
    public static final int PR_RECEIVING         = 0x05;
    public static final int PR_RESPONSE_RECEIVED = 0x06;
    public static final int PR_COMPLETE          = 0x07;
    public static final int PR_NO_PATH           = 0xf0;
    public static final int PR_LINK_FAILED       = 0xf1;
    public static final int PR_TRANSFER_FAILED   = 0xf2;
    public static final int PR_NO_IDENTITY_RCVD  = 0xf3;
    public static final int PR_NO_ACCESS         = 0xf4;
    public static final int PR_FAILED            = 0xfe;

    public static final int PR_ALL_MESSAGES      = 0x00;

    // Control/stats request paths
    public static final String STATS_GET_PATH   = "/pn/get/stats";
    public static final String SYNC_REQUEST_PATH  = "/pn/peer/sync";
    public static final String UNPEER_REQUEST_PATH = "/pn/peer/unpeer";

    // Job intervals (in processing cycles)
    private static final int JOB_OUTBOUND_INTERVAL  = 1;
    private static final int JOB_STAMPS_INTERVAL    = 1;
    private static final int JOB_LINKS_INTERVAL     = 1;
    private static final int JOB_TRANSIENT_INTERVAL = 60;
    private static final int JOB_STORE_INTERVAL     = 120;
    private static final int JOB_PEERSYNC_INTERVAL  = 6;
    private static final int JOB_PEERINGEST_INTERVAL= JOB_PEERSYNC_INTERVAL;
    private static final int JOB_ROTATE_INTERVAL    = 56 * JOB_PEERINGEST_INTERVAL;

    // ── Data structures ───────────────────────────────────────────────────────

    /** Entry stored in propagation_entries for each stored message. */
    public static final class PropagationEntry {
        public byte[]       destinationHash;
        public String       filePath;
        public double       received;
        public long         msgSize;
        public List<byte[]> handledPeers   = new ArrayList<>();
        public List<byte[]> unhandledPeers = new ArrayList<>();
        public int          stampValue;

        public PropagationEntry(byte[] destHash, String path, double received, long size, int stamp) {
            this.destinationHash = destHash;
            this.filePath        = path;
            this.received        = received;
            this.msgSize         = size;
            this.stampValue      = stamp;
        }
    }

    /** Parameters carried in a propagation-node announce, used by the announce handler. */
    public static final class PeerParams {
        public final long  timebase;
        public final int   transferLimit, syncLimit, stampCost, stampFlexibility, peeringCost;
        public final Map<Integer, Object> metadata;

        public PeerParams(long timebase, int transferLimit, int syncLimit,
                          int stampCost, int stampFlexibility, int peeringCost,
                          Map<Integer, Object> metadata) {
            this.timebase         = timebase;
            this.transferLimit    = transferLimit;
            this.syncLimit        = syncLimit;
            this.stampCost        = stampCost;
            this.stampFlexibility = stampFlexibility;
            this.peeringCost      = peeringCost;
            this.metadata         = metadata;
        }
    }

    /** Outbound stamp cost entry: [timestamp, cost]. */
    private static final class StampCostEntry {
        double timestamp;
        Integer cost;
        StampCostEntry(double ts, Integer cost) { this.timestamp = ts; this.cost = cost; }
    }

    // ── Instance state ────────────────────────────────────────────────────────

    private final RNSIdentity identity;
    private final String storagePath;
    private final String ratchetPath;
    private final String messagePath;

    private final Map<byte[], RNSDestination> deliveryDestinations = new ByteArrayMap<>();
    private final Map<byte[], LXMPeer>        peers                = new ByteArrayMap<>();
    private final Map<byte[], PropagationEntry> propagationEntries = new ByteArrayMap<>();

    private final List<LXMessage>  pendingInbound    = new ArrayList<>();
    private final List<LXMessage>  pendingOutbound   = new ArrayList<>();
    private final List<LXMessage>  failedOutbound    = new ArrayList<>();
    private final Map<byte[], RNSLink> directLinks       = new ByteArrayMap<>();
    private final Map<byte[], RNSLink> backchannelLinks  = new ByteArrayMap<>();
    private final List<RNSLink>    activePropagationLinks = new ArrayList<>();

    private final List<byte[]> prioritisedList    = new ArrayList<>();
    private final List<byte[]> ignoredList        = new ArrayList<>();
    private final List<byte[]> allowedList        = new ArrayList<>();
    private final List<byte[]> controlAllowedList = new ArrayList<>();
    private final List<byte[]> staticPeers;

    private final Map<byte[], StampCostEntry> outboundStampCosts = new ByteArrayMap<>();
    /** available_tickets: { "outbound": {hash -> [expiry, ticket]}, "inbound": {hash -> {ticket -> [expiry]}}, ... } */
    private final Map<byte[], long[]>   outboundTickets   = new ByteArrayMap<>();
    private final Map<byte[], Map<byte[], long[]>> inboundTickets  = new ByteArrayMap<>();
    private final Map<byte[], Long>      lastDeliveries    = new ByteArrayMap<>();

    private final Map<byte[], Double>   locallyDeliveredTransientIds = new ByteArrayMap<>();
    private final Map<byte[], Double>   locallyProcessedTransientIds = new ByteArrayMap<>();
    private final Map<byte[], Long>     throttledPeers               = new ByteArrayMap<>();
    private final Map<byte[], Long>     validatedPeerLinks            = new ByteArrayMap<>();

    private Consumer<LXMessage> deliveryCallback;

    private boolean propagationNode       = false;
    private double  propagationNodeStartTime = 0;
    private boolean authRequired          = false;
    private boolean retainSyncedOnNode    = false;
    private boolean fromStaticOnly;
    private boolean autopeer;
    private int     autopeerMaxdepth;
    private int     maxPeers;
    private String  name;

    private int     propagationPerTransferLimit;
    private int     propagationPerSyncLimit;
    private int     deliveryPerTransferLimit;
    private int     propagationStampCost;
    private int     propagationStampCostFlexibility;
    private int     peeringCost;
    private int     maxPeeringCost;
    private boolean enforceRatchets;
    private boolean enforceStamps;
    private int     defaultSyncStrategy;

    private Long   messageStorageLimit = null;

    private byte[]          outboundPropagationNode = null;
    private RNSLink         outboundPropagationLink = null;
    private RNSDestination  propagationDestination;
    private RNSDestination  controlDestination;

    private int    propagationTransferState    = PR_IDLE;
    private double propagationTransferProgress = 0.0;
    private int    propagationTransferMaxMessages = PR_ALL_MESSAGES;
    private Object propagationTransferLastResult = null;
    private int    propagationTransferLastDuplicates = 0;

    // Path-wait state for client-side PN sync
    private byte[]       wantsDownloadFrom    = null;
    private RNSIdentity  wantsDownloadTo      = null;
    private double       wantsDownloadTimeout = 0;

    // Client stats
    private long clientPropagationMessagesReceived = 0;
    private long clientPropagationMessagesServed   = 0;
    private long unpeeredPropagationIncoming       = 0;
    private long unpeeredPropagationRxBytes        = 0;

    // Locks
    private final ReentrantLock outboundProcessingLock = new ReentrantLock();
    private final ReentrantLock costFileLock           = new ReentrantLock();
    private final ReentrantLock ticketFileLock         = new ReentrantLock();
    private final ReentrantLock stampGenLock           = new ReentrantLock();

    private volatile boolean exitHandlerRunning = false;
    private int processingCount = 0;

    private final Map<byte[], Object>  pendingDeferredStamps  = new ByteArrayMap<>();
    // [transient_id, from_peer] pairs awaiting distribution to all other peers
    private final java.util.ArrayDeque<Object[]> peerDistributionQueue = new java.util.ArrayDeque<>();

    // ── Construction ──────────────────────────────────────────────────────────

    public LXMRouter(RNSIdentity identity, String storagePath) {
        this(identity, storagePath, new Builder());
    }

    public LXMRouter(RNSIdentity identity, String storagePath, Builder cfg) {
        this.identity   = identity != null ? identity : RNS.createIdentity();
        this.storagePath = storagePath + "/lxmf";
        this.ratchetPath = this.storagePath + "/ratchets";
        this.messagePath = this.storagePath + "/messagestore";

        this.autopeer                       = cfg.autopeer;
        this.autopeerMaxdepth               = cfg.autopeerMaxdepth;
        this.maxPeers                       = cfg.maxPeers;
        this.fromStaticOnly                 = cfg.fromStaticOnly;
        this.staticPeers                    = cfg.staticPeers;
        this.propagationPerTransferLimit    = cfg.propagationLimit;
        this.propagationPerSyncLimit        = Math.max(cfg.syncLimit, cfg.propagationLimit);
        this.deliveryPerTransferLimit       = cfg.deliveryLimit;
        this.propagationStampCost           = Math.max(cfg.propagationCost, PROPAGATION_COST_MIN);
        this.propagationStampCostFlexibility = cfg.propagationCostFlexibility;
        this.peeringCost                    = cfg.peeringCost;
        this.maxPeeringCost                 = cfg.maxPeeringCost;
        this.enforceRatchets                = cfg.enforceRatchets;
        this.enforceStamps                  = cfg.enforceStamps;
        this.defaultSyncStrategy            = cfg.syncStrategy;
        this.name                           = cfg.name;

        propagationDestination = RNS.createDestination(this.identity,
                RNSDestination.IN, RNSDestination.SINGLE, LXMF.APP_NAME, "propagation");
        propagationDestination.setDefaultAppData(this::getPropagationNodeAppData);

        RNS.registerAnnounceHandler(new LXMFDeliveryAnnounceHandler(this));
        RNS.registerAnnounceHandler(new LXMFPropagationAnnounceHandler(this));

        loadPersistedState();

        Runtime.getRuntime().addShutdownHook(new Thread(this::exitHandler));
        startJobLoop();
    }

    // ── Builder ───────────────────────────────────────────────────────────────

    public static final class Builder {
        boolean  autopeer               = AUTOPEER;
        int      autopeerMaxdepth       = AUTOPEER_MAXDEPTH;
        int      maxPeers               = MAX_PEERS;
        boolean  fromStaticOnly         = false;
        List<byte[]> staticPeers        = new ArrayList<>();
        int      propagationLimit       = PROPAGATION_LIMIT;
        int      syncLimit              = SYNC_LIMIT;
        int      deliveryLimit          = DELIVERY_LIMIT;
        int      propagationCost        = PROPAGATION_COST;
        int      propagationCostFlexibility = PROPAGATION_COST_FLEX;
        int      peeringCost            = PEERING_COST;
        int      maxPeeringCost         = MAX_PEERING_COST;
        boolean  enforceRatchets        = false;
        boolean  enforceStamps          = false;
        int      syncStrategy           = LXMPeer.STRATEGY_PERSISTENT;
        String   name                   = null;

        public Builder autopeer(boolean v)              { autopeer = v; return this; }
        public Builder autopeerMaxdepth(int v)          { autopeerMaxdepth = v; return this; }
        public Builder maxPeers(int v)                  { maxPeers = v; return this; }
        public Builder fromStaticOnly(boolean v)        { fromStaticOnly = v; return this; }
        public Builder staticPeers(List<byte[]> v)      { staticPeers = v; return this; }
        public Builder propagationLimit(int v)          { propagationLimit = v; return this; }
        public Builder syncLimit(int v)                 { syncLimit = v; return this; }
        public Builder deliveryLimit(int v)             { deliveryLimit = v; return this; }
        public Builder propagationCost(int v)           { propagationCost = v; return this; }
        public Builder propagationCostFlexibility(int v){ propagationCostFlexibility = v; return this; }
        public Builder peeringCost(int v)               { peeringCost = v; return this; }
        public Builder maxPeeringCost(int v)            { maxPeeringCost = v; return this; }
        public Builder enforceRatchets(boolean v)       { enforceRatchets = v; return this; }
        public Builder enforceStamps(boolean v)         { enforceStamps = v; return this; }
        public Builder syncStrategy(int v)              { syncStrategy = v; return this; }
        public Builder name(String v)                   { name = v; return this; }
    }

    // ── Public API ────────────────────────────────────────────────────────────

    public RNSIdentity getIdentity() { return identity; }

    public void announce(byte[] destinationHash, Object attachedInterface) {
        RNSDestination d = deliveryDestinations.get(destinationHash);
        if (d != null) d.announce(getAnnounceAppData(destinationHash), attachedInterface);
    }

    /**
     * Register a local identity to receive inbound LXMF messages.
     *
     * @param identity    the identity backing this delivery endpoint
     * @param displayName human-readable name (announced to network), may be null
     * @param stampCost   required proof-of-work cost for inbound delivery, null = no cost
     * @return the created delivery destination
     */
    public RNSDestination registerDeliveryIdentity(RNSIdentity identity,
                                                    String displayName, Integer stampCost) {
        if (!deliveryDestinations.isEmpty()) {
            RNS.log("Currently only one delivery identity is supported per LXMRouter", RNS.LOG_ERROR);
            return null;
        }

        new File(ratchetPath).mkdirs();

        RNSDestination dest = RNS.createDestination(identity,
                RNSDestination.IN, RNSDestination.SINGLE, LXMF.APP_NAME, "delivery");
        dest.enableRatchets(ratchetPath + "/" + RNS.hexrep(dest.getHash(), false) + ".ratchets");
        dest.setPacketCallback(this::deliveryPacket);
        dest.setLinkEstablishedCallback(this::deliveryLinkEstablished);
        dest.setDisplayName(displayName);

        if (enforceRatchets) dest.enforceRatchets();
        if (displayName != null) dest.setDefaultAppData(() -> getAnnounceAppData(dest.getHash()));

        deliveryDestinations.put(dest.getHash(), dest);
        setInboundStampCost(dest.getHash(), stampCost);
        return dest;
    }

    public void registerDeliveryCallback(Consumer<LXMessage> callback) {
        this.deliveryCallback = callback;
    }

    public boolean setInboundStampCost(byte[] destinationHash, Integer stampCost) {
        RNSDestination d = deliveryDestinations.get(destinationHash);
        if (d == null) return false;
        if (stampCost == null || stampCost < 1) {
            d.setStampCost(null);
        } else if (stampCost < 255) {
            d.setStampCost(stampCost);
        } else {
            return false;
        }
        return true;
    }

    /** Send an LXMessage. The message will be queued for delivery. */
    public void sendMessage(LXMessage lxm) {
        if (!lxm.isPacked()) lxm.pack();
        lxm.state = LXMessage.OUTBOUND;
        synchronized (pendingOutbound) { pendingOutbound.add(lxm); }
    }

    // ── Stamp cost management ─────────────────────────────────────────────────

    public Integer getOutboundStampCost(byte[] destinationHash) {
        StampCostEntry e = outboundStampCosts.get(destinationHash);
        return e != null ? e.cost : null;
    }

    public void updateStampCost(byte[] destinationHash, Integer stampCost) {
        RNS.log("Updating outbound stamp cost for " + RNS.prettyhexrep(destinationHash) + " to " + stampCost,
                RNS.LOG_DEBUG);
        outboundStampCosts.put(destinationHash, new StampCostEntry(
                System.currentTimeMillis() / 1000.0, stampCost));
        Thread t = new Thread(this::saveOutboundStampCosts);
        t.setDaemon(true); t.start();
    }

    // ── Ticket management ─────────────────────────────────────────────────────

    public byte[] getOutboundTicket(byte[] destinationHash) {
        long[] entry = outboundTickets.get(destinationHash);
        if (entry != null && entry[0] > System.currentTimeMillis() / 1000L) {
            return longToBytes(entry[1]);
        }
        return null;
    }

    public Double getOutboundTicketExpiry(byte[] destinationHash) {
        long[] entry = outboundTickets.get(destinationHash);
        if (entry != null && entry[0] > System.currentTimeMillis() / 1000L) {
            return (double) entry[0];
        }
        return null;
    }

    public List<byte[]> getInboundTickets(byte[] destinationHash) {
        long now = System.currentTimeMillis() / 1000L;
        Map<byte[], long[]> tmap = inboundTickets.get(destinationHash);
        if (tmap == null) return null;
        List<byte[]> valid = new ArrayList<>();
        for (Map.Entry<byte[], long[]> e : tmap.entrySet()) {
            if (now < e.getValue()[0]) valid.add(e.getKey());
        }
        return valid.isEmpty() ? null : valid;
    }

    public long[] generateTicket(byte[] destinationHash, long expiry) {
        long now = System.currentTimeMillis() / 1000L;
        Long lastDelivery = lastDeliveries.get(destinationHash);
        if (lastDelivery != null && (now - lastDelivery) < LXMessage.TICKET_INTERVAL) return null;

        Map<byte[], long[]> tmap = inboundTickets.get(destinationHash);
        if (tmap != null) {
            for (Map.Entry<byte[], long[]> e : tmap.entrySet()) {
                long validityLeft = e.getValue()[0] - now;
                if (validityLeft > LXMessage.TICKET_RENEW) return new long[]{e.getValue()[0], 0L};
            }
        } else {
            tmap = new ByteArrayMap<>();
            inboundTickets.put(destinationHash, tmap);
        }

        long expires = now + expiry;
        byte[] ticket = RNS.randomBytes(LXMessage.TICKET_LENGTH);
        tmap.put(ticket, new long[]{expires});
        saveAvailableTickets();
        return new long[]{expires, 0L};
    }

    public void rememberTicket(byte[] destinationHash, long expiry, byte[] ticket) {
        outboundTickets.put(destinationHash, new long[]{expiry, 0L});
    }

    // ── Propagation node ──────────────────────────────────────────────────────

    public boolean isPropagationNode() { return propagationNode; }

    public void enablePropagation() {
        try {
            new File(storagePath).mkdirs();
            new File(messagePath).mkdirs();

            propagationEntries.clear();

            // Index existing message store
            long st = System.currentTimeMillis();
            RNS.log("Indexing messagestore...", RNS.LOG_NOTICE);
            File msgDir = new File(messagePath);
            if (msgDir.isDirectory()) {
                for (String filename : msgDir.list()) {
                    String[] parts = filename.split("_");
                    if (parts.length >= 3) {
                        try {
                            byte[] transientId = hexToBytes(parts[0]);
                            double received    = Double.parseDouble(parts[1]);
                            int    stampVal    = Integer.parseInt(parts[2]);
                            String filePath    = messagePath + "/" + filename;
                            long   size        = new File(filePath).length();
                            try (FileInputStream fis = new FileInputStream(filePath)) {
                                byte[] destHash = new byte[LXMessage.DESTINATION_LENGTH];
                                fis.read(destHash);
                                propagationEntries.put(transientId,
                                        new PropagationEntry(destHash, filePath, received, size, stampVal));
                            }
                        } catch (Exception e) {
                            RNS.log("Could not read LXM from message store: " + e.getMessage(), RNS.LOG_ERROR);
                        }
                    }
                }
            }
            RNS.log("Indexed " + propagationEntries.size() + " messages in "
                    + RNS.prettytime((System.currentTimeMillis() - st) / 1000.0), RNS.LOG_NOTICE);

            // Load peers
            loadPeers();

            // Load node stats
            loadNodeStats();

            propagationNode          = true;
            propagationNodeStartTime = System.currentTimeMillis() / 1000.0;

            propagationDestination.setLinkEstablishedCallback(this::propagationLinkEstablished);
            propagationDestination.setPacketCallback(this::propagationPacket);
            propagationDestination.registerRequestHandler(LXMPeer.OFFER_REQUEST_PATH,
                    this::offerRequest, RNSDestination.ALLOW_ALL, null);
            propagationDestination.registerRequestHandler(LXMPeer.MESSAGE_GET_PATH,
                    this::messageGetRequest, RNSDestination.ALLOW_ALL, null);

            controlAllowedList.clear();
            controlAllowedList.add(identity.getHash());
            controlDestination = RNS.createDestination(identity,
                    RNSDestination.IN, RNSDestination.SINGLE, LXMF.APP_NAME, "propagation", "control");
            controlDestination.registerRequestHandler(STATS_GET_PATH,
                    this::statsGetRequest, RNSDestination.ALLOW_LIST, controlAllowedList);
            controlDestination.registerRequestHandler(SYNC_REQUEST_PATH,
                    this::peerSyncRequest, RNSDestination.ALLOW_LIST, controlAllowedList);
            controlDestination.registerRequestHandler(UNPEER_REQUEST_PATH,
                    this::peerUnpeerRequest, RNSDestination.ALLOW_LIST, controlAllowedList);

            announcePropagationNode();

        } catch (Exception e) {
            RNS.log("Could not enable propagation node: " + e.getMessage(), RNS.LOG_ERROR);
            RNS.traceException(e);
        }
    }

    public void disablePropagation() {
        propagationNode = false;
        announcePropagationNode();
    }

    // ── Peer management ───────────────────────────────────────────────────────

    public void peer(byte[] destinationHash, PeerParams params) {
        if (peers.containsKey(destinationHash)) {
            LXMPeer existing = peers.get(destinationHash);
            existing.peeringTimebase              = params.timebase;
            existing.propagationTransferLimit     = (double) params.transferLimit;
            existing.propagationSyncLimit         = params.syncLimit;
            existing.propagationStampCost         = params.stampCost;
            existing.propagationStampCostFlexibility = params.stampFlexibility;
            existing.peeringCost                  = params.peeringCost;
            existing.metadata                     = params.metadata;
        } else {
            if (peers.size() >= maxPeers && maxPeers > 0) {
                RNS.log("Max peers (" + maxPeers + ") reached, not adding new peer "
                        + RNS.prettyhexrep(destinationHash), RNS.LOG_DEBUG);
                return;
            }
            LXMPeer newPeer = new LXMPeer(this, destinationHash, defaultSyncStrategy);
            newPeer.peeringTimebase              = params.timebase;
            newPeer.propagationTransferLimit     = (double) params.transferLimit;
            newPeer.propagationSyncLimit         = params.syncLimit;
            newPeer.propagationStampCost         = params.stampCost;
            newPeer.propagationStampCostFlexibility = params.stampFlexibility;
            newPeer.peeringCost                  = params.peeringCost;
            newPeer.metadata                     = params.metadata;
            peers.put(destinationHash, newPeer);

            // Mark all existing messages as unhandled for this new peer
            for (byte[] tid : propagationEntries.keySet()) {
                newPeer.addUnhandledMessage(tid);
            }
            RNS.log("Peered with " + RNS.prettyhexrep(destinationHash), RNS.LOG_NOTICE);
        }
    }

    public void unpeer(byte[] destinationHash, long timebase) {
        if (peers.containsKey(destinationHash)) {
            if (!isStaticPeer(destinationHash)) {
                peers.remove(destinationHash);
                RNS.log("Unpeered " + RNS.prettyhexrep(destinationHash), RNS.LOG_NOTICE);
            }
        }
    }

    public void syncPeers() {
        for (LXMPeer peer : peers.values()) {
            if (peer.alive || peer.lastHeard == 0 ||
                    (System.currentTimeMillis() / 1000.0 - peer.lastHeard) < LXMPeer.MAX_UNREACHABLE) {
                Thread t = new Thread(peer::sync);
                t.setDaemon(true); t.start();
            }
        }
    }

    public void rotatePeers() {
        // Simplified: drop peers that haven't been heard from in MAX_UNREACHABLE
        long now = System.currentTimeMillis() / 1000L;
        List<byte[]> toRemove = new ArrayList<>();
        for (Map.Entry<byte[], LXMPeer> e : peers.entrySet()) {
            LXMPeer p = e.getValue();
            if (!isStaticPeer(e.getKey()) &&
                    p.lastHeard > 0 &&
                    (now - p.lastHeard) > LXMPeer.MAX_UNREACHABLE) {
                toRemove.add(e.getKey());
            }
        }
        for (byte[] k : toRemove) {
            RNS.log("Dropping unreachable peer " + RNS.prettyhexrep(k), RNS.LOG_NOTICE);
            peers.remove(k);
        }
    }

    // ── Message store operations ──────────────────────────────────────────────

    public long messageStorageSize() {
        if (!propagationNode) return 0;
        long total = 0;
        for (PropagationEntry e : propagationEntries.values()) total += e.msgSize;
        return total;
    }

    public void setMessageStorageLimit(long bytes) { messageStorageLimit = bytes; }

    public void cleanMessageStore() {
        RNS.log("Cleaning message store", RNS.LOG_VERBOSE);
        long now = System.currentTimeMillis() / 1000L;
        List<byte[]> expired = new ArrayList<>();
        for (Map.Entry<byte[], PropagationEntry> e : propagationEntries.entrySet()) {
            if (now > e.getValue().received + MESSAGE_EXPIRY) expired.add(e.getKey());
        }
        for (byte[] tid : expired) {
            PropagationEntry pe = propagationEntries.remove(tid);
            if (pe != null) new File(pe.filePath).delete();
            RNS.log("Removed expired message " + RNS.prettyhexrep(tid), RNS.LOG_DEBUG);
        }

        if (messageStorageLimit != null) {
            while (messageStorageSize() > messageStorageLimit && !propagationEntries.isEmpty()) {
                byte[] heaviest = null;
                double maxWeight = -1;
                for (Map.Entry<byte[], PropagationEntry> e : propagationEntries.entrySet()) {
                    double w = getWeight(e.getKey());
                    if (w > maxWeight) { maxWeight = w; heaviest = e.getKey(); }
                }
                if (heaviest != null) {
                    PropagationEntry pe = propagationEntries.remove(heaviest);
                    if (pe != null) new File(pe.filePath).delete();
                }
            }
        }
    }

    // ── Propagation entry accessors (for LXMPeer) ─────────────────────────────

    public Map<byte[], PropagationEntry> getPropagationEntries() { return propagationEntries; }

    public PropagationEntry getPropagationEntry(byte[] transientId) {
        return propagationEntries.get(transientId);
    }

    public boolean hasPropagationEntry(byte[] transientId) {
        return propagationEntries.containsKey(transientId);
    }

    public String getPropagationEntryPath(byte[] transientId) {
        PropagationEntry e = propagationEntries.get(transientId);
        return e != null ? e.filePath : null;
    }

    public double getWeight(byte[] transientId) {
        PropagationEntry e = propagationEntries.get(transientId);
        if (e == null) return 0;
        double ageWeight = Math.max(1, (System.currentTimeMillis() / 1000.0 - e.received) / 60 / 60 / 24 / 4);
        double priorityWeight = containsId(prioritisedList, e.destinationHash) ? 0.1 : 1.0;
        return priorityWeight * ageWeight * e.msgSize;
    }

    public long getSize(byte[] transientId) {
        PropagationEntry e = propagationEntries.get(transientId);
        return e != null ? e.msgSize : 0;
    }

    public Integer getStampValue(byte[] transientId) {
        PropagationEntry e = propagationEntries.get(transientId);
        return e != null ? e.stampValue : null;
    }

    // ── Peer accessors ────────────────────────────────────────────────────────

    public boolean hasPeer(byte[] destinationHash) { return peers.containsKey(destinationHash); }
    public boolean isStaticPeer(byte[] destinationHash) { return containsId(staticPeers, destinationHash); }
    public boolean isAutopeer()      { return autopeer; }
    public int     getAutopeerMaxdepth() { return autopeerMaxdepth; }
    public double  getPeerLastHeard(byte[] hash) {
        LXMPeer p = peers.get(hash); return p != null ? p.lastHeard : 0;
    }

    // ── Outbound processing state ─────────────────────────────────────────────

    public boolean isOutboundProcessingLocked() { return outboundProcessingLock.isLocked(); }
    public List<LXMessage> getPendingOutbound()  { return pendingOutbound; }

    // ── Access control ────────────────────────────────────────────────────────

    public void allow(byte[] identityHash)     { if (!containsId(allowedList, identityHash)) allowedList.add(identityHash); }
    public void disallow(byte[] identityHash)  { allowedList.removeIf(b -> Arrays.equals(b, identityHash)); }
    public void allowControl(byte[] h)         { if (!containsId(controlAllowedList, h)) controlAllowedList.add(h); }
    public void disallowControl(byte[] h)      { controlAllowedList.removeIf(b -> Arrays.equals(b, h)); }
    public void prioritise(byte[] h)           { if (!containsId(prioritisedList, h)) prioritisedList.add(h); }
    public void unprioritise(byte[] h)         { prioritisedList.removeIf(b -> Arrays.equals(b, h)); }
    public void ignoreDestination(byte[] h)    { if (!containsId(ignoredList, h)) ignoredList.add(h); }
    public void unignoreDestination(byte[] h)  { ignoredList.removeIf(b -> Arrays.equals(b, h)); }
    public void setAuthentication(boolean v)   { authRequired = v; }
    public boolean requiresAuthentication()    { return authRequired; }
    public void enforceStamps()                { enforceStamps = true; }
    public void ignoreStamps()                 { enforceStamps = false; }
    public void setRetainNodeLxms(boolean v)   { retainSyncedOnNode = v; }

    // ── Propagation node management ───────────────────────────────────────────

    public void setOutboundPropagationNode(byte[] destinationHash) {
        outboundPropagationNode = destinationHash;
        if (outboundPropagationLink != null &&
                !Arrays.equals(outboundPropagationLink.getLinkId(), destinationHash)) {
            outboundPropagationLink.teardown();
            outboundPropagationLink = null;
        }
    }

    public byte[] getOutboundPropagationNode() { return outboundPropagationNode; }

    public void requestMessagesFromPropagationNode(RNSIdentity identity, int maxMessages) {
        if (maxMessages == 0) maxMessages = PR_ALL_MESSAGES;
        if (outboundPropagationNode == null) {
            RNS.log("Cannot request LXMF PN sync, no propagation node configured", RNS.LOG_WARNING);
            return;
        }
        propagationTransferProgress    = 0.0;
        propagationTransferMaxMessages = maxMessages;

        if (outboundPropagationLink != null
                && outboundPropagationLink.getStatus() == RNSLink.ACTIVE) {
            propagationTransferState = PR_LINK_ESTABLISHED;
            RNS.log("Requesting message list from propagation node", RNS.LOG_DEBUG);
            outboundPropagationLink.identify(identity);
            outboundPropagationLink.request(LXMPeer.MESSAGE_GET_PATH,
                    new Object[]{null, null},
                    this::messageListResponse, this::messageGetFailed);
            propagationTransferState = PR_REQUEST_SENT;
        } else {
            if (outboundPropagationLink == null) {
                if (RNS.hasPath(outboundPropagationNode)) {
                    propagationTransferState = PR_LINK_ESTABLISHING;
                    RNSIdentity pnIdentity = RNS.recallIdentity(outboundPropagationNode);
                    RNSDestination pnDest  = RNS.createDestination(pnIdentity,
                            RNSDestination.OUT, RNSDestination.SINGLE, LXMF.APP_NAME, "propagation");
                    RNSIdentity reqIdentity = identity;
                    outboundPropagationLink = RNS.createLink(pnDest,
                            link -> requestMessagesFromPropagationNode(reqIdentity, propagationTransferMaxMessages),
                            link -> {});
                } else {
                    RNS.log("No path known for message download from PN "
                            + RNS.prettyhexrep(outboundPropagationNode) + ". Requesting path...", RNS.LOG_DEBUG);
                    RNS.requestPath(outboundPropagationNode);
                    wantsDownloadFrom    = outboundPropagationNode;
                    wantsDownloadTo      = identity;
                    wantsDownloadTimeout = System.currentTimeMillis() / 1000.0 + PR_PATH_TIMEOUT;
                    propagationTransferState = PR_PATH_REQUESTED;
                    startPathWaitJob();
                }
            } else {
                RNS.log("Waiting for propagation node link to become active", RNS.LOG_EXTREME);
            }
        }
    }

    private void startPathWaitJob() {
        Thread t = new Thread(() -> {
            byte[] waitFrom    = wantsDownloadFrom;
            RNSIdentity waitTo = wantsDownloadTo;
            double timeout     = wantsDownloadTimeout;
            while (!RNS.hasPath(waitFrom) && System.currentTimeMillis() / 1000.0 < timeout) {
                try { Thread.sleep(100); } catch (InterruptedException e) { return; }
            }
            if (RNS.hasPath(waitFrom)) {
                requestMessagesFromPropagationNode(waitTo, propagationTransferMaxMessages);
            } else {
                RNS.log("Propagation node path request timed out", RNS.LOG_DEBUG);
                acknowledgeSync(false, PR_NO_PATH);
            }
        });
        t.setDaemon(true); t.start();
    }

    public void acknowledgeSync(boolean resetState, Integer failureState) {
        propagationTransferLastResult = null;
        if (resetState || propagationTransferState <= PR_COMPLETE) {
            propagationTransferState = failureState != null ? failureState : PR_IDLE;
        }
        propagationTransferProgress = 0.0;
        wantsDownloadFrom    = null;
        wantsDownloadTo      = null;
        wantsDownloadTimeout = 0;
    }

    public boolean hasMessage(byte[] transientId) {
        return locallyDeliveredTransientIds.containsKey(transientId);
    }

    public void cancelPropagationNodeRequests() {
        if (outboundPropagationLink != null) {
            outboundPropagationLink.teardown();
            outboundPropagationLink = null;
        }
        acknowledgeSync(true, null);
    }

    public int  getPropagationTransferState()    { return propagationTransferState; }
    public double getPropagationTransferProgress() { return propagationTransferProgress; }

    // ── Stats ─────────────────────────────────────────────────────────────────

    public Map<String, Object> compileStats() {
        if (!propagationNode) return null;
        Map<String, Object> peerStats = new LinkedHashMap<>();
        for (Map.Entry<byte[], LXMPeer> e : peers.entrySet()) {
            LXMPeer p = e.getValue();
            Map<String, Object> ps = new LinkedHashMap<>();
            ps.put("type",               isStaticPeer(e.getKey()) ? "static" : "discovered");
            ps.put("state",              p.state);
            ps.put("alive",              p.alive);
            ps.put("name",               p.getName());
            ps.put("last_heard",         (long) p.lastHeard);
            ps.put("next_sync_attempt",  p.nextSyncAttempt);
            ps.put("last_sync_attempt",  p.lastSyncAttempt);
            ps.put("sync_backoff",       p.syncBackoff);
            ps.put("peering_timebase",   p.peeringTimebase);
            ps.put("ler",                (long) p.linkEstablishmentRate);
            ps.put("str",                (long) p.syncTransferRate);
            ps.put("transfer_limit",     p.propagationTransferLimit);
            ps.put("sync_limit",         p.propagationSyncLimit);
            ps.put("target_stamp_cost",  p.propagationStampCost);
            ps.put("stamp_cost_flexibility", p.propagationStampCostFlexibility);
            ps.put("peering_cost",       p.peeringCost);
            ps.put("peering_key",        p.peeringKeyValue());
            ps.put("network_distance",   RNS.hopsTo(e.getKey()));
            ps.put("rx_bytes",           p.rxBytes);
            ps.put("tx_bytes",           p.txBytes);
            ps.put("acceptance_rate",    p.getAcceptanceRate());
            Map<String, Object> msgStats = new LinkedHashMap<>();
            msgStats.put("offered",     p.offered);
            msgStats.put("outgoing",    p.outgoing);
            msgStats.put("incoming",    p.incoming);
            msgStats.put("unhandled",   p.getUnhandledMessageCount());
            ps.put("messages", msgStats);
            peerStats.put(RNS.hexrep(e.getKey(), false), ps);
        }

        Map<String, Object> nodeStats = new LinkedHashMap<>();
        nodeStats.put("identity_hash",   identity.getHash());
        nodeStats.put("destination_hash", propagationDestination.getHash());
        nodeStats.put("uptime",          System.currentTimeMillis() / 1000.0 - propagationNodeStartTime);
        nodeStats.put("delivery_limit",  deliveryPerTransferLimit);
        nodeStats.put("propagation_limit", propagationPerTransferLimit);
        nodeStats.put("sync_limit",      propagationPerSyncLimit);
        nodeStats.put("target_stamp_cost", propagationStampCost);
        nodeStats.put("stamp_cost_flexibility", propagationStampCostFlexibility);
        nodeStats.put("peering_cost",    peeringCost);
        nodeStats.put("max_peering_cost", maxPeeringCost);
        nodeStats.put("autopeer_maxdepth", autopeerMaxdepth);
        nodeStats.put("from_static_only", fromStaticOnly);
        Map<String, Object> msStats = new LinkedHashMap<>();
        msStats.put("count", propagationEntries.size());
        msStats.put("bytes", messageStorageSize());
        msStats.put("limit", messageStorageLimit);
        nodeStats.put("messagestore", msStats);
        Map<String, Object> clientStats = new LinkedHashMap<>();
        clientStats.put("client_propagation_messages_received", clientPropagationMessagesReceived);
        clientStats.put("client_propagation_messages_served",   clientPropagationMessagesServed);
        nodeStats.put("clients", clientStats);
        nodeStats.put("unpeered_propagation_incoming", unpeeredPropagationIncoming);
        nodeStats.put("unpeered_propagation_rx_bytes", unpeeredPropagationRxBytes);
        nodeStats.put("static_peers",    staticPeers.size());
        nodeStats.put("discovered_peers", peers.size() - staticPeers.size());
        nodeStats.put("total_peers",     peers.size());
        nodeStats.put("max_peers",       maxPeers);
        nodeStats.put("peers",           peerStats);
        return nodeStats;
    }

    // ── Request handlers (exposed to RNS) ─────────────────────────────────────

    private Object statsGetRequest(String path, Object data, byte[] requestId,
                                   RNSIdentity remoteId, double requestedAt) {
        if (remoteId == null)                                    return LXMPeer.ERROR_NO_IDENTITY;
        if (!containsId(controlAllowedList, remoteId.getHash())) return LXMPeer.ERROR_NO_ACCESS;
        return compileStats();
    }

    private Object peerSyncRequest(String path, Object data, byte[] requestId,
                                   RNSIdentity remoteId, double requestedAt) {
        if (remoteId == null)                                    return LXMPeer.ERROR_NO_IDENTITY;
        if (!containsId(controlAllowedList, remoteId.getHash())) return LXMPeer.ERROR_NO_ACCESS;
        if (!(data instanceof byte[]))                           return LXMPeer.ERROR_INVALID_DATA;
        byte[] hash = (byte[]) data;
        LXMPeer p = peers.get(hash);
        if (p == null) return LXMPeer.ERROR_NOT_FOUND;
        Thread t = new Thread(p::sync); t.setDaemon(true); t.start();
        return true;
    }

    private Object peerUnpeerRequest(String path, Object data, byte[] requestId,
                                     RNSIdentity remoteId, double requestedAt) {
        if (remoteId == null)                                    return LXMPeer.ERROR_NO_IDENTITY;
        if (!containsId(controlAllowedList, remoteId.getHash())) return LXMPeer.ERROR_NO_ACCESS;
        if (!(data instanceof byte[]))                           return LXMPeer.ERROR_INVALID_DATA;
        byte[] hash = (byte[]) data;
        if (!peers.containsKey(hash)) return LXMPeer.ERROR_NOT_FOUND;
        unpeer(hash, 0);
        return true;
    }

    // ── Packet / link callbacks ───────────────────────────────────────────────

    private void deliveryPacket(byte[] data, RNSPacket packet) {
        packet.prove();
        new Thread(() -> {
            try {
                byte[] lxmfData;
                int method;
                if (packet.getDestinationType() != RNSDestination.LINK) {
                    // OPPORTUNISTIC: RNS strips the destination hash before calling the callback,
                    // so we prepend it back to reconstruct the full LXMF wire format.
                    byte[] dstHash = packet.getDestinationHash();
                    lxmfData = concat2(dstHash, data);
                    method = LXMessage.OPPORTUNISTIC;
                } else {
                    lxmfData = data;
                    method = LXMessage.DIRECT;
                }
                LXMessage lxm = LXMessage.unpackFromBytes(lxmfData, method);
                if (packet.getRatchetId() != null && lxm != null && lxm.ratchetId == null)
                    lxm.ratchetId = packet.getRatchetId();
                processInboundMessage(lxm, packet);
            } catch (Exception e) {
                RNS.log("Could not process incoming delivery packet: " + e.getMessage(), RNS.LOG_ERROR);
            }
        }, "lxmf-delivery-packet").start();
    }

    private void deliveryLinkEstablished(RNSLink link) {
        link.setPacketCallback((data, pkt) -> {
            try {
                LXMessage lxm = LXMessage.unpackFromBytes(data, LXMessage.DIRECT);
                processInboundMessage(lxm, pkt);
            } catch (Exception e) {
                RNS.log("Could not process incoming link packet: " + e.getMessage(), RNS.LOG_ERROR);
            }
        });
        // Register large-message resource handler
        link.setResourceStartCallback(resource -> {
            resource.setConcludedCallback(res -> {
                if (res.getStatus() == RNSResource.COMPLETE) {
                    try {
                        LXMessage lxm = LXMessage.unpackFromBytes(res.getData(), LXMessage.DIRECT);
                        processInboundMessage(lxm, null);
                    } catch (Exception e) {
                        RNS.log("Could not process incoming link resource: " + e.getMessage(), RNS.LOG_ERROR);
                    }
                }
            });
        });
        directLinks.put(link.getLinkId(), link);
    }

    private void propagationPacket(byte[] data, RNSPacket packet) {
        processInboundPropagation(data);
    }

    private void propagationLinkEstablished(RNSLink link) {
        activePropagationLinks.add(link);
        link.setPacketCallback((data, pkt) -> processInboundPropagation(data));
        link.setResourceStartCallback(resource ->
                resource.setConcludedCallback(this::propagationResourceConcluded));
    }

    private void propagationResourceConcluded(RNSResource resource) {
        if (resource.getStatus() != RNSResource.COMPLETE) return;
        try {
            byte[] raw = resource.getData();
            MessageUnpacker up = MessagePack.newDefaultUnpacker(raw);
            int arrSize = up.unpackArrayHeader();
            if (arrSize < 2) { up.close(); return; }
            up.skipValue(); // remote_timebase
            int msgCount = up.unpackArrayHeader();
            List<byte[]> blobs = new ArrayList<>(msgCount);
            for (int i = 0; i < msgCount; i++) {
                blobs.add(up.readPayload(up.unpackBinaryHeader()));
            }
            up.close();

            RNSLink link = resource.getLink();
            byte[] linkId = link != null ? link.getLinkId() : null;
            boolean peeringKeyValid = linkId != null && validatedPeerLinks.containsKey(linkId);

            if (!peeringKeyValid && blobs.size() > 1) {
                if (link != null) link.teardown();
                RNS.log("Received multiple propagation messages without valid peering key, ignoring.", RNS.LOG_WARNING);
                return;
            }

            int minAcceptedCost = Math.max(0, propagationStampCost - propagationStampCostFlexibility);
            List<LXStamper.ValidatedPnStamp> validated = LXStamper.validatePnStamps(blobs, minAcceptedCost);

            RNS.log("Received " + blobs.size() + " message(s), validating stamps...", RNS.LOG_VERBOSE);
            if (validated.size() < blobs.size())
                RNS.log("Transfer contained " + (blobs.size() - validated.size()) + " invalid stamp(s).", RNS.LOG_WARNING);

            for (LXStamper.ValidatedPnStamp v : validated) {
                // Look up the peer by the lxmfData destination hash
                byte[] destHash = Arrays.copyOf(v.lxmData, LXMessage.DESTINATION_LENGTH);
                LXMPeer fromPeer = peers.get(destHash);
                if (fromPeer != null) {
                    fromPeer.incoming++;
                    fromPeer.rxBytes += v.lxmData.length;
                } else {
                    unpeeredPropagationIncoming++;
                    unpeeredPropagationRxBytes += v.lxmData.length;
                }
                lxmfPropagation(v.lxmData, fromPeer, v.value, v.stamp);
                if (fromPeer != null) fromPeer.queueHandledMessage(v.transientId);
            }

            if (validated.size() < blobs.size() && link != null) link.teardown();
        } catch (Exception e) {
            RNS.log("Error processing propagation resource: " + e.getMessage(), RNS.LOG_DEBUG);
        }
    }

    // ── Offer / message-get request handlers (PN mode) ───────────────────────

    private Object offerRequest(String path, Object data, byte[] requestId,
                                RNSIdentity remoteId, double requestedAt) {
        if (remoteId == null) return LXMPeer.ERROR_NO_IDENTITY;

        // Compute the remote propagation destination hash for access checks
        byte[] remoteHash = RNS.truncatedHash(
                concat2(remoteId.getHash(),
                        RNS.fullHash((LXMF.APP_NAME + ".propagation").getBytes(java.nio.charset.StandardCharsets.UTF_8))));

        if (throttledPeers.containsKey(remoteHash)) {
            long throttleUntil = throttledPeers.get(remoteHash);
            if (System.currentTimeMillis() / 1000L < throttleUntil) {
                RNS.log("Propagation offer from " + RNS.prettyhexrep(remoteHash) + " rejected, throttled.", RNS.LOG_NOTICE);
                return LXMPeer.ERROR_THROTTLED;
            } else {
                throttledPeers.remove(remoteHash);
            }
        }

        if (fromStaticOnly && !containsId(staticPeers, remoteHash))
            return LXMPeer.ERROR_NO_ACCESS;

        if (!(data instanceof Object[])) return LXMPeer.ERROR_INVALID_DATA;
        Object[] offer = (Object[]) data;
        if (offer.length < 2) return LXMPeer.ERROR_INVALID_DATA;

        byte[] peeringKey = (byte[]) offer[0];
        @SuppressWarnings("unchecked")
        List<byte[]> transientIds = (List<byte[]>) offer[1];

        // Validate peering key (stamp proving the remote node's identity)
        byte[] peeringId = concat2(identity.getHash(), remoteId.getHash());
        boolean keyValid = LXStamper.validatePeeringKey(peeringId, peeringKey, peeringCost);
        if (!keyValid) {
            RNS.log("Invalid peering key for incoming sync offer from " + RNS.prettyhexrep(remoteHash), RNS.LOG_DEBUG);
            return LXMPeer.ERROR_INVALID_KEY;
        }

        // Mark this link as having presented a valid peering key
        validatedPeerLinks.put(requestId, System.currentTimeMillis() / 1000L);

        List<byte[]> wantedIds = new ArrayList<>();
        for (byte[] tid : transientIds) {
            if (!hasPropagationEntry(tid)
                    && !locallyDeliveredTransientIds.containsKey(tid)
                    && !locallyProcessedTransientIds.containsKey(tid)) {
                wantedIds.add(tid);
            }
        }
        if (wantedIds.isEmpty()) return false;
        if (wantedIds.size() == transientIds.size()) return true;
        return wantedIds;
    }

    private boolean identityAllowed(RNSIdentity id) {
        if (!authRequired) return true;
        return containsId(allowedList, id.getHash());
    }

    @SuppressWarnings("unchecked")
    private Object messageGetRequest(String path, Object data, byte[] requestId,
                                     RNSIdentity remoteId, double requestedAt) {
        if (remoteId == null) return LXMPeer.ERROR_NO_IDENTITY;
        if (!identityAllowed(remoteId)) return LXMPeer.ERROR_NO_ACCESS;

        try {
            RNSDestination remoteDest = RNS.createDestination(remoteId,
                    RNSDestination.OUT, RNSDestination.SINGLE, LXMF.APP_NAME, "delivery");
            byte[] remoteHash = remoteDest.getHash();

            List<Object> req = (List<Object>) data;
            Object wantField = req.size() > 0 ? req.get(0) : null;
            Object haveField = req.size() > 1 ? req.get(1) : null;

            if (wantField == null && haveField == null) {
                // List mode: return available message IDs for this destination sorted by size
                List<long[]> available = new ArrayList<>(); // [size, index into sorted list]
                List<byte[]> ids = new ArrayList<>();
                for (Map.Entry<byte[], PropagationEntry> e : propagationEntries.entrySet()) {
                    if (Arrays.equals(e.getValue().destinationHash, remoteHash)) {
                        available.add(new long[]{e.getValue().msgSize, ids.size()});
                        ids.add(e.getKey());
                    }
                }
                available.sort(Comparator.comparingLong(a -> a[0]));
                List<byte[]> result = new ArrayList<>(available.size());
                for (long[] entry : available) result.add(ids.get((int) entry[1]));
                return result;
            } else {
                // Get/purge mode
                if (haveField instanceof List) {
                    List<byte[]> haves = (List<byte[]>) haveField;
                    for (byte[] tid : haves) {
                        PropagationEntry pe = propagationEntries.get(tid);
                        if (pe != null && Arrays.equals(pe.destinationHash, remoteHash)) {
                            try {
                                propagationEntries.remove(tid);
                                new File(pe.filePath).delete();
                            } catch (Exception e) {
                                RNS.log("Error purging message " + RNS.prettyhexrep(tid)
                                        + " for " + RNS.prettyhexrep(remoteHash) + ": " + e.getMessage(), RNS.LOG_ERROR);
                            }
                        }
                    }
                }

                List<byte[]> responseMessages = new ArrayList<>();
                if (wantField instanceof List) {
                    List<byte[]> wants = (List<byte[]>) wantField;
                    Double clientLimitMb = null;
                    if (req.size() >= 3 && req.get(2) instanceof Number) {
                        try { clientLimitMb = ((Number) req.get(2)).doubleValue() * 1000; } catch (Exception ignored) {}
                    }
                    int perMsgOverhead = 16;
                    int cumSize = 24;
                    for (byte[] tid : wants) {
                        PropagationEntry pe = propagationEntries.get(tid);
                        if (pe != null && Arrays.equals(pe.destinationHash, remoteHash)) {
                            try {
                                byte[] fileData = Files.readAllBytes(Paths.get(pe.filePath));
                                // Strip stamp (last STAMP_SIZE bytes) before sending
                                int lxmSize = fileData.length;
                                int nextSize = cumSize + lxmSize + perMsgOverhead;
                                if (clientLimitMb != null && nextSize > clientLimitMb) continue;
                                int payloadLen = Math.max(0, lxmSize - LXStamper.STAMP_SIZE);
                                byte[] lxmfData = Arrays.copyOf(fileData, payloadLen);
                                responseMessages.add(lxmfData);
                                cumSize += lxmSize + perMsgOverhead;
                                RNS.log("Client " + RNS.prettyhexrep(remoteHash)
                                        + " requested message " + RNS.prettyhexrep(tid), RNS.LOG_DEBUG);
                            } catch (Exception e) {
                                RNS.log("Error reading message " + RNS.prettyhexrep(tid)
                                        + " for " + RNS.prettyhexrep(remoteHash) + ": " + e.getMessage(), RNS.LOG_ERROR);
                            }
                        }
                    }
                }
                clientPropagationMessagesServed += responseMessages.size();
                return responseMessages;
            }
        } catch (Exception e) {
            RNS.log("Error generating message get response: " + e.getMessage(), RNS.LOG_DEBUG);
            return null;
        }
    }

    // ── Inbound processing ────────────────────────────────────────────────────

    private void processInboundMessage(LXMessage lxm, Object context) {
        if (lxm == null) return;
        if (containsId(ignoredList, lxm.getDestinationHash())) return;

        // Duplicate check
        if (lxm.transientId != null && locallyDeliveredTransientIds.containsKey(lxm.transientId)) {
            RNS.log("Ignored already received message from " + RNS.prettyhexrep(lxm.getSourceHash()), RNS.LOG_DEBUG);
            return;
        }

        // If the message carries a ticket, remember it for future stamp bypass
        if (lxm.signatureValidated && lxm.fields != null && lxm.fields.containsKey(LXMF.FIELD_TICKET)) {
            Object ticketEntry = lxm.fields.get(LXMF.FIELD_TICKET);
            if (ticketEntry instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> te = (List<Object>) ticketEntry;
                if (te.size() > 1) {
                    double expires = te.get(0) instanceof Number ? ((Number) te.get(0)).doubleValue() : 0;
                    Object rawTicket = te.get(1);
                    if (System.currentTimeMillis() / 1000.0 < expires && rawTicket instanceof byte[]) {
                        rememberTicket(lxm.getSourceHash(), (long) expires, (byte[]) rawTicket);
                        new Thread(this::saveAvailableTickets).start();
                    }
                }
            }
        }

        // Stamp validation
        RNSDestination dest = deliveryDestinations.get(lxm.getDestinationHash());
        if (dest != null && dest.getStampCost() != null) {
            List<byte[]> tickets = getInboundTickets(lxm.getSourceHash());
            boolean stampValid = lxm.validateStamp(dest.getStampCost(), tickets);
            lxm.stampChecked = true;
            lxm.stampValid = stampValid;
            if (!stampValid) {
                if (enforceStamps) {
                    RNS.log("Dropping " + lxm + " with invalid stamp", RNS.LOG_NOTICE);
                    return;
                } else {
                    RNS.log("Received " + lxm + " with invalid stamp, allowing anyway", RNS.LOG_NOTICE);
                }
            } else {
                RNS.log("Received " + lxm + " with valid stamp", RNS.LOG_DEBUG);
            }
        }

        // Tag transport encryption type
        int destType = lxm.getMethod();
        if (destType == LXMessage.OPPORTUNISTIC) {
            lxm.transportEncrypted = true;
            lxm.transportEncryption = LXMessage.ENCRYPTION_DESCRIPTION_EC;
        } else if (destType == LXMessage.DIRECT || destType == LXMessage.PROPAGATED) {
            lxm.transportEncrypted = true;
            lxm.transportEncryption = LXMessage.ENCRYPTION_DESCRIPTION_EC;
        }

        if (deliveryCallback != null) {
            try {
                deliveryCallback.accept(lxm);
            } catch (Exception e) {
                RNS.log("An error occurred in the delivery callback: " + e.getMessage(), RNS.LOG_ERROR);
            }
        }

        lxm.state = LXMessage.DELIVERED;
        if (lxm.transientId != null) {
            locallyDeliveredTransientIds.put(lxm.transientId, System.currentTimeMillis() / 1000.0);
        }
    }

    /**
     * Handle a small propagation packet received directly on the propagation destination or link.
     * Wire format (msgpack): [remote_timebase:float, messages:array of binary(lxmfData+stamp)]
     */
    private void processInboundPropagation(byte[] data) {
        try {
            MessageUnpacker up = MessagePack.newDefaultUnpacker(data);
            int arrSize = up.unpackArrayHeader();
            if (arrSize < 2) { up.close(); return; }
            up.skipValue(); // remote_timebase
            int msgCount = up.unpackArrayHeader();
            List<byte[]> blobs = new ArrayList<>(msgCount);
            for (int i = 0; i < msgCount; i++) {
                blobs.add(up.readPayload(up.unpackBinaryHeader()));
            }
            up.close();

            int minAcceptedCost = Math.max(0, propagationStampCost - propagationStampCostFlexibility);
            List<LXStamper.ValidatedPnStamp> validated = LXStamper.validatePnStamps(blobs, minAcceptedCost);

            for (LXStamper.ValidatedPnStamp v : validated) {
                lxmfPropagation(v.lxmData, null, v.value, v.stamp);
                clientPropagationMessagesReceived++;
            }
        } catch (Exception e) {
            RNS.log("Exception parsing incoming LXMF propagation packet: " + e.getMessage(), RNS.LOG_ERROR);
        }
    }

    /**
     * Core propagation handler. Mirrors Python's lxmf_propagation().
     * Delivers locally if we have the destination; stores in PN message store otherwise.
     */
    private boolean lxmfPropagation(byte[] lxmfData, LXMPeer fromPeer, int stampValue, byte[] stampData) {
        try {
            if (lxmfData.length < LXMessage.LXMF_OVERHEAD) return false;

            byte[] transientId = RNS.fullHash(lxmfData);

            if (locallyProcessedTransientIds.containsKey(transientId)) return false;

            double received = System.currentTimeMillis() / 1000.0;
            byte[] destHash = Arrays.copyOf(lxmfData, LXMessage.DESTINATION_LENGTH);

            locallyProcessedTransientIds.put(transientId, received);

            RNSDestination deliveryDest = deliveryDestinations.get(destHash);
            if (deliveryDest != null) {
                // Deliver locally: strip dest hash, decrypt, deliver
                byte[] encryptedPart = Arrays.copyOfRange(lxmfData, LXMessage.DESTINATION_LENGTH, lxmfData.length);
                byte[] decryptedPart = deliveryDest.encrypt(encryptedPart); // note: encrypt=decrypt for in-dest
                if (decryptedPart != null) {
                    byte[] deliveryData = new byte[LXMessage.DESTINATION_LENGTH + decryptedPart.length];
                    System.arraycopy(destHash, 0, deliveryData, 0, LXMessage.DESTINATION_LENGTH);
                    System.arraycopy(decryptedPart, 0, deliveryData, LXMessage.DESTINATION_LENGTH, decryptedPart.length);
                    byte[] ratchetId = deliveryDest.getLatestRatchetId();
                    LXMessage lxm = LXMessage.unpackFromBytes(deliveryData, LXMessage.PROPAGATED);
                    if (lxm != null) {
                        if (ratchetId != null) lxm.ratchetId = ratchetId;
                        processInboundMessage(lxm, null);
                        locallyDeliveredTransientIds.put(transientId, System.currentTimeMillis() / 1000.0);
                    }
                }
                return true;
            } else if (propagationNode) {
                // Save to disk and add to propagation entries
                byte[] stampedData = stampData != null
                        ? concat2(lxmfData, stampData)
                        : lxmfData;
                String valueComponent = stampValue > 0 ? "_" + stampValue : "";
                String filename = RNS.hexrep(transientId) + "_" + received + valueComponent;
                String filePath = messagePath + "/" + filename;
                try (java.io.FileOutputStream fos = new java.io.FileOutputStream(filePath)) {
                    fos.write(stampedData);
                }
                PropagationEntry entry = new PropagationEntry(destHash, filePath, received, stampedData.length, stampValue);
                propagationEntries.put(transientId, entry);
                RNS.log("Stored propagated LXMF message " + RNS.prettyhexrep(transientId)
                        + " with stamp value " + stampValue, RNS.LOG_EXTREME);
                enqueuePeerDistribution(transientId, fromPeer);
                return true;
            } else {
                RNS.log("Received propagated LXMF message " + RNS.prettyhexrep(transientId)
                        + " but not a PN, discarding.", RNS.LOG_DEBUG);
                return true;
            }
        } catch (Exception e) {
            RNS.log("Could not assemble propagated LXMF message: " + e.getMessage(), RNS.LOG_DEBUG);
            return false;
        }
    }

    private void enqueuePeerDistribution(byte[] transientId, LXMPeer fromPeer) {
        synchronized (peerDistributionQueue) {
            peerDistributionQueue.add(new Object[]{transientId, fromPeer});
        }
    }

    /** Distribute newly received messages from peerDistributionQueue to all other peers. */
    private void flushPeerDistributionQueue() {
        List<Object[]> entries = new ArrayList<>();
        synchronized (peerDistributionQueue) {
            while (!peerDistributionQueue.isEmpty()) entries.add(peerDistributionQueue.poll());
        }
        for (Object[] entry : entries) {
            byte[] transientId = (byte[]) entry[0];
            LXMPeer fromPeer   = (LXMPeer) entry[1];
            for (LXMPeer peer : peers.values()) {
                if (peer != fromPeer) peer.queueUnhandledMessage(transientId);
            }
        }
    }

    public void failMessage(LXMessage lxm) {
        RNS.log(lxm + " failed to send", RNS.LOG_DEBUG);
        lxm.progress = 0.0;
        synchronized (pendingOutbound) { pendingOutbound.remove(lxm); }
        failedOutbound.add(lxm);
        if (lxm.state != LXMessage.REJECTED) lxm.state = LXMessage.FAILED;
        if (lxm.failedCallback != null) lxm.failedCallback.accept(lxm);
    }

    /**
     * Ingest a paper LXM URI (lxm://base64url...) and deliver locally or store on PN.
     */
    public boolean ingestLxmUri(String uri) {
        try {
            String schema = LXMessage.URI_SCHEMA + "://";
            if (!uri.toLowerCase().startsWith(schema)) {
                RNS.log("Cannot ingest LXM, invalid URI provided.", RNS.LOG_ERROR);
                return false;
            }
            String encoded = uri.substring(schema.length()).replace("/", "");
            // base64url padding
            while (encoded.length() % 4 != 0) encoded += "=";
            byte[] lxmfData = java.util.Base64.getUrlDecoder().decode(encoded);
            byte[] transientId = RNS.fullHash(lxmfData);
            boolean result = lxmfPropagation(lxmfData, null, 0, null);
            if (result) {
                RNS.log("LXM " + RNS.prettyhexrep(transientId) + " ingested via URI.", RNS.LOG_DEBUG);
            } else {
                RNS.log("No valid LXM could be ingested from the provided URI.", RNS.LOG_DEBUG);
            }
            return result;
        } catch (Exception e) {
            RNS.log("Error decoding URI-encoded LXMF message: " + e.getMessage(), RNS.LOG_ERROR);
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private void messageListResponse(RNSLinkRequestReceipt receipt) {
        Object response = receipt.getResponse();
        if (response instanceof Integer && (Integer) response == LXMPeer.ERROR_NO_IDENTITY) {
            RNS.log("Propagation node indicated missing identification on list request, tearing down link.", RNS.LOG_DEBUG);
            if (outboundPropagationLink != null) outboundPropagationLink.teardown();
            propagationTransferState = PR_NO_IDENTITY_RCVD;
        } else if (response instanceof Integer && (Integer) response == LXMPeer.ERROR_NO_ACCESS) {
            RNS.log("Propagation node did not allow list request, tearing down link.", RNS.LOG_DEBUG);
            if (outboundPropagationLink != null) outboundPropagationLink.teardown();
            propagationTransferState = PR_NO_ACCESS;
        } else if (response instanceof List) {
            List<byte[]> available = (List<byte[]>) response;
            if (available.isEmpty()) {
                propagationTransferState = PR_COMPLETE;
                propagationTransferProgress = 1.0;
                propagationTransferLastResult = 0;
            } else {
                List<byte[]> wants = new ArrayList<>();
                List<byte[]> haves = new ArrayList<>();
                for (byte[] tid : available) {
                    if (hasMessage(tid)) {
                        if (!retainSyncedOnNode) haves.add(tid);
                    } else {
                        if (propagationTransferMaxMessages == PR_ALL_MESSAGES
                                || wants.size() < propagationTransferMaxMessages) {
                            wants.add(tid);
                        }
                    }
                }
                String ms = wants.size() == 1 ? "" : "s";
                RNS.log("Requesting " + wants.size() + " message" + ms + " from propagation node", RNS.LOG_DEBUG);
                receipt.getLink().request(LXMPeer.MESSAGE_GET_PATH,
                        new Object[]{wants, haves, deliveryPerTransferLimit},
                        this::messageGetResponse, this::messageGetFailed, this::messageGetProgress);
            }
        } else {
            RNS.log("Invalid message list data received from propagation node", RNS.LOG_DEBUG);
            if (outboundPropagationLink != null) outboundPropagationLink.teardown();
        }
    }

    @SuppressWarnings("unchecked")
    private void messageGetResponse(RNSLinkRequestReceipt receipt) {
        Object response = receipt.getResponse();
        if (response instanceof Integer && (Integer) response == LXMPeer.ERROR_NO_IDENTITY) {
            RNS.log("Propagation node indicated missing identification on get request, tearing down link.", RNS.LOG_DEBUG);
            if (outboundPropagationLink != null) outboundPropagationLink.teardown();
            propagationTransferState = PR_NO_IDENTITY_RCVD;
        } else if (response instanceof Integer && (Integer) response == LXMPeer.ERROR_NO_ACCESS) {
            RNS.log("Propagation node did not allow get request, tearing down link.", RNS.LOG_DEBUG);
            if (outboundPropagationLink != null) outboundPropagationLink.teardown();
            propagationTransferState = PR_NO_ACCESS;
        } else {
            int duplicates = 0;
            if (response instanceof List) {
                List<byte[]> messages = (List<byte[]>) response;
                if (!messages.isEmpty()) {
                    List<byte[]> haves = new ArrayList<>(messages.size());
                    for (byte[] lxmfData : messages) {
                        byte[] tid = RNS.fullHash(lxmfData);
                        boolean isDuplicate = locallyProcessedTransientIds.containsKey(tid);
                        lxmfPropagation(lxmfData, null, 0, null);
                        if (isDuplicate) duplicates++;
                        haves.add(tid);
                    }
                    receipt.getLink().request(LXMPeer.MESSAGE_GET_PATH,
                            new Object[]{null, haves},
                            null, this::messageGetFailed);
                }
                propagationTransferLastResult = messages.size();
            } else {
                propagationTransferLastResult = 0;
            }
            propagationTransferState = PR_COMPLETE;
            propagationTransferProgress = 1.0;
            propagationTransferLastDuplicates = duplicates;
            saveLocallyDeliveredTransientIds();
        }
    }

    private void messageGetProgress(RNSLinkRequestReceipt receipt) {
        propagationTransferState = PR_RECEIVING;
        propagationTransferProgress = receipt.getProgress();
    }

    private void messageGetFailed(RNSLinkRequestReceipt receipt) {
        RNS.log("Message list/get request failed", RNS.LOG_DEBUG);
        if (outboundPropagationLink != null) outboundPropagationLink.teardown();
    }

    // ── Outbound processing ───────────────────────────────────────────────────

    public void processOutbound() {
        if (outboundProcessingLock.tryLock()) {
            try {
                List<LXMessage> snapshot;
                synchronized (pendingOutbound) { snapshot = new ArrayList<>(pendingOutbound); }

                for (LXMessage lxm : snapshot) {
                    if (lxm.state == LXMessage.DELIVERED) {
                        RNS.log("Delivery has occurred for " + lxm + ", removing from queue", RNS.LOG_DEBUG);
                        synchronized (pendingOutbound) { pendingOutbound.remove(lxm); }

                    } else if (lxm.method == LXMessage.PROPAGATED && lxm.state == LXMessage.SENT) {
                        RNS.log("Propagation has occurred for " + lxm + ", removing from queue", RNS.LOG_DEBUG);
                        synchronized (pendingOutbound) { pendingOutbound.remove(lxm); }

                    } else if (lxm.state == LXMessage.CANCELLED) {
                        RNS.log("Cancellation for " + lxm + ", removing from queue", RNS.LOG_DEBUG);
                        synchronized (pendingOutbound) { pendingOutbound.remove(lxm); }
                        if (lxm.failedCallback != null) lxm.failedCallback.accept(lxm);

                    } else if (lxm.state == LXMessage.REJECTED) {
                        RNS.log("Receiver rejected " + lxm + ", removing from queue", RNS.LOG_DEBUG);
                        synchronized (pendingOutbound) { pendingOutbound.remove(lxm); }
                        if (lxm.failedCallback != null) lxm.failedCallback.accept(lxm);

                    } else {
                        if (lxm.progress < 0.01) lxm.progress = 0.01;
                        processOutboundMessage(lxm);
                    }
                }
            } finally {
                outboundProcessingLock.unlock();
            }
        }
    }

    private void processOutboundMessage(LXMessage lxm) {
        double now = System.currentTimeMillis() / 1000.0;

        // Refresh stamp cost and ticket for this destination
        Integer outboundStampCost = getOutboundStampCost(lxm.getDestinationHash());
        if (outboundStampCost != null) lxm.stampCost = outboundStampCost;
        byte[] ticket = getOutboundTicket(lxm.getDestinationHash());
        if (ticket != null) lxm.outboundTicket = ticket;

        if (lxm.method == LXMessage.OPPORTUNISTIC) {
            if (lxm.deliveryAttempts <= MAX_DELIVERY_ATTEMPTS) {
                if (lxm.nextDeliveryAttempt > now) return;
                if (!RNS.hasPath(lxm.getDestinationHash())) {
                    RNS.log("No path for opportunistic " + lxm + ", requesting...", RNS.LOG_DEBUG);
                    RNS.requestPath(lxm.getDestinationHash());
                    lxm.nextDeliveryAttempt = now + PATH_REQUEST_WAIT;
                    lxm.deliveryAttempts++;
                    lxm.progress = 0.01;
                } else {
                    lxm.deliveryAttempts++;
                    lxm.nextDeliveryAttempt = now + DELIVERY_RETRY_WAIT;
                    RNS.log("Opportunistic attempt " + lxm.deliveryAttempts + " for " + lxm, RNS.LOG_DEBUG);
                    lxm.send();
                }
            } else {
                RNS.log("Max delivery attempts for opportunistic " + lxm, RNS.LOG_DEBUG);
                failMessage(lxm);
            }

        } else if (lxm.method == LXMessage.DIRECT) {
            if (lxm.deliveryAttempts <= MAX_DELIVERY_ATTEMPTS) {
                byte[] destHash = lxm.getDestinationHash();

                // Use existing active link if available
                RNSLink activeLink = directLinks.get(destHash);
                if (activeLink == null) activeLink = backchannelLinks.get(destHash);

                if (activeLink != null) {
                    if (activeLink.getStatus() == RNSLink.ACTIVE) {
                        if (lxm.progress < 0.05) lxm.progress = 0.05;
                        if (lxm.state != LXMessage.SENDING) {
                            RNS.log("Sending " + lxm + " over direct link", RNS.LOG_DEBUG);
                            lxm.setDeliveryDestination(activeLink.asDestination());
                            lxm.send();
                        }
                    } else if (activeLink.getStatus() == RNSLink.CLOSED) {
                        directLinks.remove(destHash);
                        backchannelLinks.remove(destHash);
                        lxm.setDeliveryDestination(null);
                        lxm.nextDeliveryAttempt = now + DELIVERY_RETRY_WAIT;
                        RNS.requestPath(destHash);
                    }
                } else {
                    if (lxm.nextDeliveryAttempt > now) return;
                    lxm.deliveryAttempts++;
                    lxm.nextDeliveryAttempt = now + DELIVERY_RETRY_WAIT;
                    if (lxm.deliveryAttempts < MAX_DELIVERY_ATTEMPTS) {
                        if (RNS.hasPath(destHash)) {
                            RNS.log("Establishing link to " + RNS.prettyhexrep(destHash)
                                    + " for direct delivery attempt " + lxm.deliveryAttempts, RNS.LOG_DEBUG);
                            RNSIdentity di = RNS.recallIdentity(destHash);
                            if (di != null) {
                                RNSDestination dd = RNS.createDestination(di,
                                        RNSDestination.OUT, RNSDestination.SINGLE, LXMF.APP_NAME, "delivery");
                                RNSLink newLink = RNS.createLink(dd, l -> processOutbound(), l -> {});
                                directLinks.put(destHash, newLink);
                                lxm.progress = 0.03;
                            }
                        } else {
                            RNS.log("No path for direct " + lxm + ", requesting...", RNS.LOG_DEBUG);
                            RNS.requestPath(destHash);
                            lxm.nextDeliveryAttempt = now + PATH_REQUEST_WAIT;
                            lxm.progress = 0.01;
                        }
                    }
                }
            } else {
                RNS.log("Max delivery attempts for direct " + lxm, RNS.LOG_DEBUG);
                failMessage(lxm);
            }

        } else if (lxm.method == LXMessage.PROPAGATED) {
            if (outboundPropagationNode == null) {
                RNS.log("No outbound propagation node configured, failing " + lxm, RNS.LOG_ERROR);
                failMessage(lxm);
                return;
            }
            if (lxm.deliveryAttempts <= MAX_DELIVERY_ATTEMPTS) {
                if (outboundPropagationLink != null) {
                    if (outboundPropagationLink.getStatus() == RNSLink.ACTIVE) {
                        if (lxm.state != LXMessage.SENDING) {
                            RNS.log("Starting propagation transfer of " + lxm
                                    + " via " + RNS.prettyhexrep(outboundPropagationNode), RNS.LOG_DEBUG);
                            lxm.setDeliveryDestination(outboundPropagationLink.asDestination());
                            lxm.send();
                        }
                    } else if (outboundPropagationLink.getStatus() == RNSLink.CLOSED) {
                        outboundPropagationLink = null;
                        lxm.nextDeliveryAttempt = now + DELIVERY_RETRY_WAIT;
                    }
                } else {
                    if (lxm.nextDeliveryAttempt > now) return;
                    lxm.deliveryAttempts++;
                    lxm.nextDeliveryAttempt = now + DELIVERY_RETRY_WAIT;
                    if (lxm.deliveryAttempts < MAX_DELIVERY_ATTEMPTS) {
                        if (RNS.hasPath(outboundPropagationNode)) {
                            RNS.log("Establishing link to PN " + RNS.prettyhexrep(outboundPropagationNode)
                                    + " for propagation attempt " + lxm.deliveryAttempts, RNS.LOG_DEBUG);
                            RNSIdentity pni = RNS.recallIdentity(outboundPropagationNode);
                            if (pni != null) {
                                RNSDestination pnd = RNS.createDestination(pni,
                                        RNSDestination.OUT, RNSDestination.SINGLE, LXMF.APP_NAME, "propagation");
                                outboundPropagationLink = RNS.createLink(pnd, l -> processOutbound(), l -> {});
                            }
                        } else {
                            RNS.log("No path to PN " + RNS.prettyhexrep(outboundPropagationNode)
                                    + ", requesting...", RNS.LOG_DEBUG);
                            RNS.requestPath(outboundPropagationNode);
                            lxm.nextDeliveryAttempt = now + PATH_REQUEST_WAIT;
                        }
                    }
                }
            } else {
                RNS.log("Max delivery attempts for propagated " + lxm, RNS.LOG_DEBUG);
                failMessage(lxm);
            }
        }
    }

    // ── Deferred stamps ───────────────────────────────────────────────────────

    public void processDeferredStamps() {
        List<LXMessage> messages;
        synchronized (pendingOutbound) { messages = new ArrayList<>(pendingOutbound); }
        for (LXMessage lxm : messages) {
            if (lxm.deferStamp && lxm.stampCost != null && lxm.stamp == null
                    && !lxm.deferredStampGenerating) {
                lxm.deferredStampGenerating = true;
                stampGenLock.lock();
                try {
                    lxm.stamp = lxm.getStamp();
                    if (lxm.stamp != null) {
                        lxm.pack(true);
                    }
                } finally {
                    stampGenLock.unlock();
                    lxm.deferredStampGenerating = false;
                }
            }
        }
    }

    // ── Job loop ──────────────────────────────────────────────────────────────

    private void startJobLoop() {
        Thread t = new Thread(() -> {
            while (true) {
                try {
                    if (!exitHandlerRunning) jobs();
                } catch (Exception e) {
                    RNS.log("An error occurred while running LXMRouter jobs: " + e.getMessage(), RNS.LOG_ERROR);
                }
                try { Thread.sleep(PROCESSING_INTERVAL * 1000L); } catch (InterruptedException e) { break; }
            }
        });
        t.setDaemon(true);
        t.setName("LXMRouter-JobLoop");
        t.start();
    }

    private void jobs() {
        processingCount++;
        if (processingCount % JOB_OUTBOUND_INTERVAL == 0)   processOutbound();
        if (processingCount % JOB_STAMPS_INTERVAL == 0) {
            Thread t = new Thread(this::processDeferredStamps); t.setDaemon(true); t.start();
        }
        if (processingCount % JOB_LINKS_INTERVAL == 0)      cleanLinks();
        if (processingCount % JOB_TRANSIENT_INTERVAL == 0)  cleanTransientIdCaches();
        if (processingCount % JOB_STORE_INTERVAL == 0 && propagationNode) cleanMessageStore();
        if (processingCount % JOB_PEERINGEST_INTERVAL == 0 && propagationNode) flushQueues();
        if (processingCount % JOB_ROTATE_INTERVAL == 0 && propagationNode) rotatePeers();
        if (processingCount % JOB_PEERSYNC_INTERVAL == 0) {
            if (propagationNode) syncPeers();
            cleanThrottledPeers();
        }
    }

    private void flushQueues() {
        flushPeerDistributionQueue();
        for (LXMPeer peer : peers.values()) {
            if (peer.hasQueuedItems()) peer.processQueues();
        }
    }

    // ── Link cleanup ──────────────────────────────────────────────────────────

    private void cleanLinks() {
        List<byte[]> toClose = new ArrayList<>();
        for (Map.Entry<byte[], RNSLink> e : directLinks.entrySet()) {
            if (e.getValue().noDataFor() > LINK_MAX_INACTIVITY) {
                e.getValue().teardown();
                toClose.add(e.getKey());
            }
        }
        toClose.forEach(directLinks::remove);

        List<RNSLink> inactivePN = new ArrayList<>();
        for (RNSLink link : activePropagationLinks) {
            if (link.noDataFor() > P_LINK_MAX_INACTIVITY) inactivePN.add(link);
        }
        for (RNSLink link : inactivePN) {
            activePropagationLinks.remove(link);
            link.teardown();
        }
    }

    // ── Cache cleanup ─────────────────────────────────────────────────────────

    private void cleanTransientIdCaches() {
        long now = System.currentTimeMillis() / 1000L;
        locallyDeliveredTransientIds.entrySet().removeIf(e -> now > e.getValue() + MESSAGE_EXPIRY * 6);
        locallyProcessedTransientIds.entrySet().removeIf(e -> now > e.getValue() + MESSAGE_EXPIRY * 6);
    }

    private void cleanThrottledPeers() {
        long now = System.currentTimeMillis() / 1000L;
        throttledPeers.entrySet().removeIf(e -> now > e.getValue());
    }

    // ── Persistence ───────────────────────────────────────────────────────────

    private void loadPersistedState() {
        new File(storagePath).mkdirs();
        loadLocalDeliveries();
        loadLocallyProcessed();
        loadOutboundStampCosts();
        loadAvailableTickets();
    }

    private void loadLocalDeliveries() {
        File f = new File(storagePath + "/local_deliveries");
        if (!f.exists()) return;
        try (MessageUnpacker up = MessagePack.newDefaultUnpacker(Files.readAllBytes(f.toPath()))) {
            int size = up.unpackMapHeader();
            for (int i = 0; i < size; i++) {
                byte[] k = up.readPayload(up.unpackBinaryHeader());
                double v = up.unpackDouble();
                locallyDeliveredTransientIds.put(k, v);
            }
        } catch (Exception e) {
            RNS.log("Could not load local deliveries: " + e.getMessage(), RNS.LOG_ERROR);
        }
    }

    private void loadLocallyProcessed() {
        File f = new File(storagePath + "/locally_processed");
        if (!f.exists()) return;
        try (MessageUnpacker up = MessagePack.newDefaultUnpacker(Files.readAllBytes(f.toPath()))) {
            int size = up.unpackMapHeader();
            for (int i = 0; i < size; i++) {
                byte[] k = up.readPayload(up.unpackBinaryHeader());
                double v = up.unpackDouble();
                locallyProcessedTransientIds.put(k, v);
            }
        } catch (Exception e) {
            RNS.log("Could not load locally processed: " + e.getMessage(), RNS.LOG_ERROR);
        }
    }

    private void loadOutboundStampCosts() {
        File f = new File(storagePath + "/outbound_stamp_costs");
        if (!f.exists()) return;
        costFileLock.lock();
        try (MessageUnpacker up = MessagePack.newDefaultUnpacker(Files.readAllBytes(f.toPath()))) {
            int size = up.unpackMapHeader();
            for (int i = 0; i < size; i++) {
                byte[] k = up.readPayload(up.unpackBinaryHeader());
                int arrSz = up.unpackArrayHeader();
                double ts = up.unpackDouble();
                Integer cost = null;
                if (up.getNextFormat() != MessageFormat.NIL) cost = up.unpackInt();
                else up.unpackNil();
                outboundStampCosts.put(k, new StampCostEntry(ts, cost));
            }
            cleanOutboundStampCosts();
            saveOutboundStampCosts();
        } catch (Exception e) {
            RNS.log("Could not load outbound stamp costs: " + e.getMessage(), RNS.LOG_ERROR);
        } finally {
            costFileLock.unlock();
        }
    }

    private void loadAvailableTickets() {
        File f = new File(storagePath + "/available_tickets");
        if (!f.exists()) return;
        // Simplified ticket loading
    }

    private void loadPeers() {
        File f = new File(storagePath + "/peers");
        if (!f.isFile()) {
            // Add static peers
            for (byte[] sp : staticPeers) {
                if (!peers.containsKey(sp)) {
                    peers.put(sp, new LXMPeer(this, sp, defaultSyncStrategy));
                    RNS.requestPath(sp);
                }
            }
            return;
        }
        try (MessageUnpacker up = MessagePack.newDefaultUnpacker(Files.readAllBytes(f.toPath()))) {
            int count = up.unpackArrayHeader();
            for (int i = 0; i < count; i++) {
                byte[] peerBytes = up.readPayload(up.unpackBinaryHeader());
                LXMPeer peer = LXMPeer.fromBytes(peerBytes, this);
                if (peer != null && peer.identity != null) {
                    peers.put(peer.destinationHash, peer);
                }
            }
        } catch (Exception e) {
            RNS.log("Could not load peers: " + e.getMessage(), RNS.LOG_ERROR);
        }
        for (byte[] sp : staticPeers) {
            if (!peers.containsKey(sp)) {
                peers.put(sp, new LXMPeer(this, sp, defaultSyncStrategy));
                RNS.requestPath(sp);
            }
        }
        RNS.log("Loaded " + peers.size() + " peers", RNS.LOG_NOTICE);
    }

    private void loadNodeStats() {
        File f = new File(storagePath + "/node_stats");
        if (!f.isFile()) return;
        try (MessageUnpacker up = MessagePack.newDefaultUnpacker(Files.readAllBytes(f.toPath()))) {
            int size = up.unpackMapHeader();
            for (int i = 0; i < size; i++) {
                String k = up.unpackString();
                switch (k) {
                    case "client_propagation_messages_received":
                        clientPropagationMessagesReceived = up.unpackLong(); break;
                    case "client_propagation_messages_served":
                        clientPropagationMessagesServed = up.unpackLong(); break;
                    case "unpeered_propagation_incoming":
                        unpeeredPropagationIncoming = up.unpackLong(); break;
                    case "unpeered_propagation_rx_bytes":
                        unpeeredPropagationRxBytes = up.unpackLong(); break;
                    default: up.skipValue(); break;
                }
            }
        } catch (Exception e) {
            RNS.log("Could not load node stats: " + e.getMessage(), RNS.LOG_ERROR);
        }
    }

    private void saveOutboundStampCosts() {
        costFileLock.lock();
        try {
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
            packer.packMapHeader(outboundStampCosts.size());
            for (Map.Entry<byte[], StampCostEntry> e : outboundStampCosts.entrySet()) {
                packer.packBinaryHeader(e.getKey().length); packer.writePayload(e.getKey());
                packer.packArrayHeader(2);
                packer.packDouble(e.getValue().timestamp);
                if (e.getValue().cost == null) packer.packNil(); else packer.packInt(e.getValue().cost);
            }
            try (FileOutputStream fos = new FileOutputStream(storagePath + "/outbound_stamp_costs")) {
                fos.write(packer.toByteArray());
            }
        } catch (Exception e) {
            RNS.log("Could not save outbound stamp costs: " + e.getMessage(), RNS.LOG_ERROR);
        } finally {
            costFileLock.unlock();
        }
    }

    private void saveAvailableTickets() {
        ticketFileLock.lock();
        try { /* simplified */ } finally { ticketFileLock.unlock(); }
    }

    public void saveLocallyDeliveredTransientIds() {
        try {
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
            packer.packMapHeader(locallyDeliveredTransientIds.size());
            for (Map.Entry<byte[], Double> e : locallyDeliveredTransientIds.entrySet()) {
                packer.packBinaryHeader(e.getKey().length); packer.writePayload(e.getKey());
                packer.packDouble(e.getValue());
            }
            try (FileOutputStream fos = new FileOutputStream(storagePath + "/local_deliveries")) {
                fos.write(packer.toByteArray());
            }
        } catch (Exception e) {
            RNS.log("Could not save local deliveries: " + e.getMessage(), RNS.LOG_ERROR);
        }
    }

    private void savePeers() {
        if (!propagationNode) return;
        try {
            List<byte[]> serializedPeers = new ArrayList<>();
            for (LXMPeer p : peers.values()) serializedPeers.add(p.toBytes());
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
            packer.packArrayHeader(serializedPeers.size());
            for (byte[] pb : serializedPeers) {
                packer.packBinaryHeader(pb.length); packer.writePayload(pb);
            }
            try (FileOutputStream fos = new FileOutputStream(storagePath + "/peers")) {
                fos.write(packer.toByteArray());
            }
        } catch (Exception e) {
            RNS.log("Could not save peers: " + e.getMessage(), RNS.LOG_ERROR);
        }
    }

    private void saveNodeStats() {
        if (!propagationNode) return;
        try {
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
            packer.packMapHeader(4);
            packer.packString("client_propagation_messages_received"); packer.packLong(clientPropagationMessagesReceived);
            packer.packString("client_propagation_messages_served");   packer.packLong(clientPropagationMessagesServed);
            packer.packString("unpeered_propagation_incoming");        packer.packLong(unpeeredPropagationIncoming);
            packer.packString("unpeered_propagation_rx_bytes");        packer.packLong(unpeeredPropagationRxBytes);
            try (FileOutputStream fos = new FileOutputStream(storagePath + "/node_stats")) {
                fos.write(packer.toByteArray());
            }
        } catch (Exception e) {
            RNS.log("Could not save node stats: " + e.getMessage(), RNS.LOG_ERROR);
        }
    }

    private void cleanOutboundStampCosts() {
        long now = System.currentTimeMillis() / 1000L;
        outboundStampCosts.entrySet().removeIf(e -> now > e.getValue().timestamp + STAMP_COST_EXPIRY);
    }

    // ── Announce helpers ──────────────────────────────────────────────────────

    private byte[] getAnnounceAppData(byte[] destinationHash) {
        RNSDestination d = deliveryDestinations.get(destinationHash);
        if (d == null) return null;
        try {
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
            packer.packArrayHeader(2);
            String dn = d.getDisplayName();
            if (dn != null) {
                byte[] dnBytes = dn.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                packer.packBinaryHeader(dnBytes.length); packer.writePayload(dnBytes);
            } else {
                packer.packNil();
            }
            Integer sc = d.getStampCost();
            if (sc != null) packer.packInt(sc); else packer.packNil();
            return packer.toByteArray();
        } catch (IOException e) { return null; }
    }

    private byte[] getPropagationNodeAppData() {
        try {
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
            packer.packArrayHeader(7);
            packer.packBoolean(false);                               // 0: legacy
            packer.packLong(System.currentTimeMillis() / 1000L);   // 1: timebase
            packer.packBoolean(propagationNode && !fromStaticOnly); // 2: node state
            packer.packInt(propagationPerTransferLimit);            // 3: transfer limit
            packer.packInt(propagationPerSyncLimit);                // 4: sync limit
            packer.packArrayHeader(3);
            packer.packInt(propagationStampCost);                   // 5[0]: stamp cost
            packer.packInt(propagationStampCostFlexibility);        // 5[1]: flexibility
            packer.packInt(peeringCost);                            // 5[2]: peering cost
            // 6: metadata
            Map<Integer, Object> metadata = new LinkedHashMap<>();
            if (name != null) metadata.put(LXMF.PN_META_NAME, name.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            packer.packMapHeader(metadata.size());
            for (Map.Entry<Integer, Object> e : metadata.entrySet()) {
                packer.packInt(e.getKey());
                if (e.getValue() instanceof byte[]) {
                    byte[] v = (byte[]) e.getValue();
                    packer.packBinaryHeader(v.length); packer.writePayload(v);
                }
            }
            return packer.toByteArray();
        } catch (IOException e) { return null; }
    }

    private void announcePropagationNode() {
        Thread t = new Thread(() -> {
            try { Thread.sleep(NODE_ANNOUNCE_DELAY * 1000L); } catch (InterruptedException e) { return; }
            propagationDestination.announce(getPropagationNodeAppData(), null);
        });
        t.setDaemon(true); t.start();
    }

    // ── Shutdown ──────────────────────────────────────────────────────────────

    private void exitHandler() {
        exitHandlerRunning = true;
        RNS.log("LXMRouter exiting, saving state...", RNS.LOG_NOTICE);
        saveLocallyDeliveredTransientIds();
        saveOutboundStampCosts();
        saveAvailableTickets();
        if (propagationNode) { savePeers(); saveNodeStats(); }
    }

    // ── Utility ───────────────────────────────────────────────────────────────

    private static byte[] concat2(byte[] a, byte[] b) {
        byte[] r = new byte[a.length + b.length];
        System.arraycopy(a, 0, r, 0, a.length);
        System.arraycopy(b, 0, r, a.length, b.length);
        return r;
    }

    private static boolean containsId(List<byte[]> list, byte[] id) {
        for (byte[] b : list) if (Arrays.equals(b, id)) return true;
        return false;
    }

    private static byte[] hexToBytes(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }

    private static byte[] longToBytes(long v) {
        byte[] b = new byte[8];
        for (int i = 7; i >= 0; i--) { b[i] = (byte)(v & 0xFF); v >>= 8; }
        return b;
    }

    /** Wrap an RNSLink into an RNSDestination stub for delivery. */
    private static RNSDestination wrapLink(RNSLink link) {
        // Real implementations would return the link-wrapping destination from the provider.
        // This is a placeholder that adapters must implement properly.
        return link.asDestination();
    }

    // ── ByteArrayMap — map with byte[] keys using value equality ─────────────

    private static final class ByteArrayMap<V> extends LinkedHashMap<byte[], V> {

        @Override
        public V get(Object key) {
            if (!(key instanceof byte[])) return null;
            byte[] k = (byte[]) key;
            for (Map.Entry<byte[], V> e : entrySet()) {
                if (Arrays.equals(e.getKey(), k)) return e.getValue();
            }
            return null;
        }

        @Override
        public boolean containsKey(Object key) {
            return get(key) != null;
        }

        @Override
        public V put(byte[] key, V value) {
            for (Map.Entry<byte[], V> e : entrySet()) {
                if (Arrays.equals(e.getKey(), key)) {
                    return super.put(e.getKey(), value);
                }
            }
            return super.put(key, value);
        }

        @Override
        public V remove(Object key) {
            if (!(key instanceof byte[])) return null;
            byte[] k = (byte[]) key;
            byte[] found = null;
            for (byte[] existing : keySet()) {
                if (Arrays.equals(existing, k)) { found = existing; break; }
            }
            return found != null ? super.remove(found) : null;
        }
    }
}
