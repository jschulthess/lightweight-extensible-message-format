package examples;

// Reticulum License
//
// Copyright (c) 2020-2025 Mark Qvist
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// - The Software shall not be used in any kind of system which includes amongst
//   its functions the ability to purposefully do harm to human beings.
//
// - The Software shall not be used, directly or indirectly, in the creation of
//   an artificial intelligence, machine learning or language model training
//   dataset, including but not limited to any use that contributes to the
//   training or development of such a model or algorithm.
//
// - The above copyright notice and this permission notice shall be included in
//   all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

import io.lxmf.LXMessage;
import io.lxmf.LXMRouter;
import io.lxmf.rns.RNS;
import io.lxmf.rns.RNSDestination;
import io.lxmf.rns.RNSIdentity;
import io.lxmf.rns.impl.IdentityAdapter;
import io.lxmf.rns.impl.ReticulumProvider;
import io.reticulum.Reticulum;
import io.reticulum.Transport;
import io.reticulum.identity.Identity;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * LXM Daemon — Java equivalent of the Python {@code lxmd.py} utility.
 *
 * <p>Provides an LXMF message router that can optionally run a Propagation Node,
 * receive inbound messages, and forward them to an external program.
 *
 * <p>Configuration is read from a Java {@code .properties} file at
 * {@code <configdir>/config.properties}. Run with {@code --exampleconfig} to
 * print a fully-commented default configuration to stdout.
 *
 * <pre>
 * Usage:
 *   LxmdDaemon [options]
 *
 * Options:
 *   --config DIR          path to alternative lxmd config directory
 *   --rnsconfig DIR       path to alternative Reticulum config directory
 *   -p, --propagation-node  run an LXMF Propagation Node
 *   -i PATH, --on-inbound PATH  executable to run when a message is received
 *   -v, --verbose         increase log verbosity (repeatable)
 *   -q, --quiet           decrease log verbosity (repeatable)
 *   -s, --service         log to file instead of stdout
 *   --exampleconfig       print example config to stdout and exit
 * </pre>
 */
public class LxmdDaemon {

    private static final int DEFERRED_JOBS_DELAY_S = 10;
    private static final int JOBS_INTERVAL_S       = 5;

    // ── Active configuration keys ─────────────────────────────────────────────
    private static final String CFG_DISPLAY_NAME                     = "displayName";
    private static final String CFG_PEER_ANNOUNCE_AT_START           = "peerAnnounceAtStart";
    private static final String CFG_PEER_ANNOUNCE_INTERVAL           = "peerAnnounceInterval";
    private static final String CFG_DELIVERY_TRANSFER_MAX_SIZE       = "deliveryTransferMaxSize";
    private static final String CFG_ON_INBOUND                       = "onInbound";
    private static final String CFG_ENABLE_PROPAGATION_NODE          = "enablePropagationNode";
    private static final String CFG_NODE_NAME                        = "nodeName";
    private static final String CFG_AUTH_REQUIRED                    = "authRequired";
    private static final String CFG_NODE_ANNOUNCE_AT_START           = "nodeAnnounceAtStart";
    private static final String CFG_AUTOPEER                         = "autopeer";
    private static final String CFG_AUTOPEER_MAXDEPTH                = "autopeerMaxdepth";
    private static final String CFG_NODE_ANNOUNCE_INTERVAL           = "nodeAnnounceInterval";
    private static final String CFG_MESSAGE_STORAGE_LIMIT            = "messageStorageLimit";
    private static final String CFG_PROPAGATION_TRANSFER_MAX_SIZE    = "propagationTransferMaxSize";
    private static final String CFG_PROPAGATION_SYNC_MAX_SIZE        = "propagationSyncMaxSize";
    private static final String CFG_PROPAGATION_STAMP_COST           = "propagationStampCost";
    private static final String CFG_PROPAGATION_STAMP_FLEXIBILITY    = "propagationStampFlexibility";
    private static final String CFG_PEERING_COST                     = "peeringCost";
    private static final String CFG_REMOTE_PEERING_COST_MAX          = "remotePeeringCostMax";
    private static final String CFG_PRIORITISED_DESTINATIONS         = "prioritisedDestinations";
    private static final String CFG_CONTROL_ALLOWED_IDENTITIES       = "controlAllowedIdentities";
    private static final String CFG_STATIC_PEERS                     = "staticPeers";
    private static final String CFG_MAX_PEERS                        = "maxPeers";
    private static final String CFG_FROM_STATIC_ONLY                 = "fromStaticOnly";
    private static final String CFG_IGNORED_DESTINATIONS             = "ignoredDestinations";
    private static final String CFG_ALLOWED_IDENTITIES               = "allowedIdentities";

    // ── Global state ──────────────────────────────────────────────────────────
    private static LXMRouter     messageRouter;
    private static RNSDestination lxmfDestination;
    private static Map<String, Object> config = new HashMap<>();

    private static String configFilePath;
    private static String ignoredPath;
    private static String allowedPath;
    private static String identityPath;
    private static String storageDir;
    private static String lxmDir;

    private static long lastPeerAnnounce = 0;
    private static long lastNodeAnnounce = 0;

    // ── Entry point ───────────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        String  configDir    = null;
        String  rnsConfigDir = null;
        boolean runPn        = false;
        String  onInbound    = null;
        int     verbosity    = 0;
        int     quietness    = 0;
        boolean service      = false;
        boolean exampleCfg   = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--config":       configDir    = args[++i]; break;
                case "--rnsconfig":    rnsConfigDir = args[++i]; break;
                case "-p":
                case "--propagation-node": runPn    = true;      break;
                case "-i":
                case "--on-inbound":   onInbound    = args[++i]; break;
                case "-v":
                case "--verbose":      verbosity++;              break;
                case "-q":
                case "--quiet":        quietness++;              break;
                case "-s":
                case "--service":      service      = true;      break;
                case "--exampleconfig": exampleCfg  = true;      break;
                default:
                    System.err.println("Unknown argument: " + args[i]);
                    printUsage();
                    System.exit(1);
            }
        }

        if (exampleCfg) {
            System.out.println(defaultConfig());
            return;
        }

        programSetup(configDir, rnsConfigDir, runPn, onInbound, verbosity, quietness, service);
    }

    // ── Daemon setup ──────────────────────────────────────────────────────────

    private static void programSetup(String configDir, String rnsConfigDir, boolean runPn,
                                      String onInbound, int verbosity, int quietness,
                                      boolean service) throws Exception {
        // Resolve config directory
        if (configDir == null) {
            String home = System.getProperty("user.home");
            Path etcLxmd  = Paths.get("/etc/lxmd");
            Path homeLxmd = Paths.get(home, ".lxmd");
            configDir = Files.isDirectory(etcLxmd) && Files.isRegularFile(etcLxmd.resolve("config.properties"))
                    ? etcLxmd.toString()
                    : homeLxmd.toString();
        }

        configFilePath = configDir + "/config.properties";
        ignoredPath    = configDir + "/ignored";
        allowedPath    = configDir + "/allowed";
        identityPath   = configDir + "/identity";
        storageDir     = configDir + "/storage";
        lxmDir         = storageDir + "/messages";

        Files.createDirectories(Paths.get(storageDir));
        Files.createDirectories(Paths.get(lxmDir));

        Properties rawConfig = new Properties();
        if (!Files.isRegularFile(Paths.get(configFilePath))) {
            System.out.println("Could not load config file, creating default configuration...");
            createDefaultConfig(Paths.get(configFilePath));
            System.out.println("Default config created at " + configFilePath + ". Edit if needed and restart.");
            Thread.sleep(1500);
        }
        try (InputStream in = new FileInputStream(configFilePath)) {
            rawConfig.load(in);
        } catch (IOException e) {
            System.err.println("Could not parse configuration at " + configFilePath);
            System.err.println("Check your configuration file for errors!");
            System.exit(3);
        }

        applyConfig(rawConfig);
        if (onInbound != null) config.put(CFG_ON_INBOUND, onInbound);

        // Start Reticulum
        System.out.println("Substantiating Reticulum...");
        Reticulum reticulum = new Reticulum(rnsConfigDir != null ? rnsConfigDir
                : System.getProperty("user.home") + "/.reticulum");
        Transport transport = Transport.start(reticulum);
        RNS.initialize(new ReticulumProvider(transport));

        // Load or create primary identity
        Path idPath = Paths.get(identityPath);
        Identity nativeIdentity;
        if (Files.isRegularFile(idPath)) {
            nativeIdentity = Identity.fromFile(idPath);
            if (nativeIdentity == null) {
                System.err.println("Could not load Primary Identity from " + identityPath);
                System.exit(4);
            }
            System.out.println("Loaded Primary Identity " + RNS.prettyhexrep(nativeIdentity.getHash()));
        } else {
            System.out.println("No Primary Identity file found, creating new...");
            nativeIdentity = new Identity();
            nativeIdentity.toFile(idPath);
            System.out.println("Created new Primary Identity " + RNS.prettyhexrep(nativeIdentity.getHash()));
        }
        RNSIdentity identity = new IdentityAdapter(nativeIdentity);

        // Build LXMRouter
        LXMRouter.Builder builder = new LXMRouter.Builder()
                .autopeer(       (Boolean) config.get(CFG_AUTOPEER))
                .autopeerMaxdepth((Integer) config.get(CFG_AUTOPEER_MAXDEPTH))
                .propagationLimit((Integer) config.get(CFG_PROPAGATION_TRANSFER_MAX_SIZE))
                .propagationCost((Integer) config.get(CFG_PROPAGATION_STAMP_COST))
                .propagationCostFlexibility((Integer) config.get(CFG_PROPAGATION_STAMP_FLEXIBILITY))
                .peeringCost(    (Integer) config.get(CFG_PEERING_COST))
                .maxPeeringCost( (Integer) config.get(CFG_REMOTE_PEERING_COST_MAX))
                .syncLimit(      (Integer) config.get(CFG_PROPAGATION_SYNC_MAX_SIZE))
                .deliveryLimit(  (Integer) config.get(CFG_DELIVERY_TRANSFER_MAX_SIZE))
                .fromStaticOnly( (Boolean) config.get(CFG_FROM_STATIC_ONLY))
                .name(           (String)  config.get(CFG_NODE_NAME));

        @SuppressWarnings("unchecked")
        List<byte[]> staticPeers = (List<byte[]>) config.get(CFG_STATIC_PEERS);
        if (staticPeers != null && !staticPeers.isEmpty()) builder.staticPeers(staticPeers);

        Integer maxPeers = (Integer) config.get(CFG_MAX_PEERS);
        if (maxPeers != null) builder.maxPeers(maxPeers);

        messageRouter = new LXMRouter(identity, storageDir, builder);
        messageRouter.registerDeliveryCallback(LxmdDaemon::lxmfDelivery);

        // Ignore configured destinations
        @SuppressWarnings("unchecked")
        List<byte[]> ignoredDests = (List<byte[]>) config.get(CFG_IGNORED_DESTINATIONS);
        if (ignoredDests != null) {
            for (byte[] h : ignoredDests) messageRouter.ignoreDestination(h);
        }

        // Register delivery identity
        lxmfDestination = messageRouter.registerDeliveryIdentity(
                identity,
                (String) config.get(CFG_DISPLAY_NAME),
                null);
        System.out.println("LXMF Router ready to receive on " + RNS.prettyhexrep(lxmfDestination.getHash()));

        // Configure authentication
        if ((Boolean) config.get(CFG_AUTH_REQUIRED)) {
            messageRouter.setAuthentication(true);
            @SuppressWarnings("unchecked")
            List<byte[]> allowed = (List<byte[]>) config.get(CFG_ALLOWED_IDENTITIES);
            if (allowed == null || allowed.isEmpty()) {
                System.out.println("[WARNING] Authentication is enabled but no allowed identity hashes were loaded from "
                        + allowedPath + ". Nobody will be able to sync messages from this node.");
            } else {
                for (byte[] h : allowed) messageRouter.allow(h);
            }
        }

        // Enable propagation node
        if (runPn || (Boolean) config.get(CFG_ENABLE_PROPAGATION_NODE)) {
            long storageLimitMb = (long) ((Double) config.get(CFG_MESSAGE_STORAGE_LIMIT) * 1024 * 1024);
            messageRouter.setMessageStorageLimit(storageLimitMb);

            @SuppressWarnings("unchecked")
            List<byte[]> prioritised = (List<byte[]>) config.get(CFG_PRIORITISED_DESTINATIONS);
            if (prioritised != null) {
                for (byte[] h : prioritised) messageRouter.prioritise(h);
            }

            messageRouter.enablePropagation();

            @SuppressWarnings("unchecked")
            List<byte[]> controlAllowed = (List<byte[]>) config.get(CFG_CONTROL_ALLOWED_IDENTITIES);
            if (controlAllowed != null) {
                for (byte[] h : controlAllowed) messageRouter.allowControl(h);
            }

            System.out.println("LXMF Propagation Node started");
        }

        System.out.println("lxmd (Java) started");

        Thread t = new Thread(LxmdDaemon::deferredStartJobs);
        t.setDaemon(true);
        t.start();

        // Block main thread
        Object lock = new Object();
        synchronized (lock) {
            lock.wait();
        }
    }

    // ── Delivery callback ─────────────────────────────────────────────────────

    private static void lxmfDelivery(LXMessage lxm) {
        try {
            String writtenPath = lxm.writeToDirectory(lxmDir);
            System.out.println("Received " + lxm + " written to " + writtenPath);

            String onInbound = (String) config.get(CFG_ON_INBOUND);
            if (onInbound != null && !onInbound.isEmpty()) {
                System.out.println("Calling external program to handle message");
                String[] command = (onInbound + " \"" + writtenPath + "\"").split("\\s+(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                new ProcessBuilder(command)
                        .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                        .redirectError(ProcessBuilder.Redirect.DISCARD)
                        .start()
                        .waitFor();
            }
        } catch (Exception e) {
            System.err.println("Error processing received message: " + e.getMessage());
        }
    }

    // ── Periodic jobs ─────────────────────────────────────────────────────────

    private static void deferredStartJobs() {
        try {
            Thread.sleep(DEFERRED_JOBS_DELAY_S * 1000L);
        } catch (InterruptedException e) {
            return;
        }

        if ((Boolean) config.get(CFG_PEER_ANNOUNCE_AT_START)) {
            System.out.println("Sending announce for LXMF delivery destination");
            messageRouter.announce(lxmfDestination.getHash(), null);
        }
        // Propagation node announces itself during enablePropagation(); no separate call needed here.
        // If node_announce_at_start is also desired for periodic jobs, the LXMRouter.announcePropagationNode()
        // method would need to be made public.

        lastPeerAnnounce = System.currentTimeMillis() / 1000L;
        lastNodeAnnounce = System.currentTimeMillis() / 1000L;

        Thread t = new Thread(LxmdDaemon::jobs);
        t.setDaemon(true);
        t.start();
    }

    private static void jobs() {
        while (true) {
            try {
                long now = System.currentTimeMillis() / 1000L;

                Long peerInterval = (Long) config.get(CFG_PEER_ANNOUNCE_INTERVAL);
                if (peerInterval != null && now > lastPeerAnnounce + peerInterval) {
                    System.out.println("Sending periodic announce for LXMF delivery destination");
                    messageRouter.announce(lxmfDestination.getHash(), null);
                    lastPeerAnnounce = now;
                }

                // Periodic propagation node announce: LXMRouter.announcePropagationNode() is currently
                // package-private. Expose it as a public method on LXMRouter if periodic re-announcement
                // is required beyond the automatic announce triggered by enablePropagation().

            } catch (Exception e) {
                System.err.println("Error in periodic jobs: " + e.getMessage());
            }

            try {
                Thread.sleep(JOBS_INTERVAL_S * 1000L);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    // ── Config loading ────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private static void applyConfig(Properties p) {
        // [lxmf] section
        config.put(CFG_DISPLAY_NAME,              p.getProperty("lxmf.display_name", "Anonymous Peer"));
        config.put(CFG_PEER_ANNOUNCE_AT_START,    Boolean.parseBoolean(p.getProperty("lxmf.announce_at_start", "false")));
        String peerIntervalStr = p.getProperty("lxmf.announce_interval");
        config.put(CFG_PEER_ANNOUNCE_INTERVAL,    peerIntervalStr != null ? Long.parseLong(peerIntervalStr) * 60 : null);
        config.put(CFG_DELIVERY_TRANSFER_MAX_SIZE, clampMinInt(
                parseIntOrDefault(p, "lxmf.delivery_transfer_max_accepted_size", LXMRouter.DELIVERY_LIMIT), 1));
        config.put(CFG_ON_INBOUND,                p.getProperty("lxmf.on_inbound"));

        // [propagation] section
        config.put(CFG_ENABLE_PROPAGATION_NODE,   Boolean.parseBoolean(p.getProperty("propagation.enable_node", "false")));
        config.put(CFG_NODE_NAME,                 p.getProperty("propagation.node_name"));
        config.put(CFG_AUTH_REQUIRED,             Boolean.parseBoolean(p.getProperty("propagation.auth_required", "false")));
        config.put(CFG_NODE_ANNOUNCE_AT_START,    Boolean.parseBoolean(p.getProperty("propagation.announce_at_start", "false")));
        config.put(CFG_AUTOPEER,                  Boolean.parseBoolean(p.getProperty("propagation.autopeer", "true")));
        config.put(CFG_AUTOPEER_MAXDEPTH,         parseIntOrDefault(p, "propagation.autopeer_maxdepth", LXMRouter.AUTOPEER_MAXDEPTH));
        String nodeIntervalStr = p.getProperty("propagation.announce_interval");
        config.put(CFG_NODE_ANNOUNCE_INTERVAL,    nodeIntervalStr != null ? Long.parseLong(nodeIntervalStr) * 60 : null);
        config.put(CFG_MESSAGE_STORAGE_LIMIT,     Math.max(
                parseDoubleOrDefault(p, "propagation.message_storage_limit", 500.0), 0.005));
        config.put(CFG_PROPAGATION_TRANSFER_MAX_SIZE, clampMinInt(
                parseIntOrDefault(p, "propagation.propagation_message_max_accepted_size", LXMRouter.PROPAGATION_LIMIT), 1));
        config.put(CFG_PROPAGATION_SYNC_MAX_SIZE, clampMinInt(
                parseIntOrDefault(p, "propagation.propagation_sync_max_accepted_size", LXMRouter.SYNC_LIMIT), 1));
        config.put(CFG_PROPAGATION_STAMP_COST,    Math.max(
                parseIntOrDefault(p, "propagation.propagation_stamp_cost_target", LXMRouter.PROPAGATION_COST),
                LXMRouter.PROPAGATION_COST_MIN));
        config.put(CFG_PROPAGATION_STAMP_FLEXIBILITY, Math.max(
                parseIntOrDefault(p, "propagation.propagation_stamp_cost_flexibility", LXMRouter.PROPAGATION_COST_FLEX), 0));
        config.put(CFG_PEERING_COST,              Math.max(
                parseIntOrDefault(p, "propagation.peering_cost", LXMRouter.PEERING_COST), 0));
        config.put(CFG_REMOTE_PEERING_COST_MAX,   Math.max(
                parseIntOrDefault(p, "propagation.remote_peering_cost_max", LXMRouter.MAX_PEERING_COST), 0));
        config.put(CFG_FROM_STATIC_ONLY,          Boolean.parseBoolean(p.getProperty("propagation.from_static_only", "false")));
        String maxPeersStr = p.getProperty("propagation.max_peers");
        config.put(CFG_MAX_PEERS,                 maxPeersStr != null ? Integer.parseInt(maxPeersStr) : LXMRouter.MAX_PEERS);

        // Prioritised destinations
        config.put(CFG_PRIORITISED_DESTINATIONS,  parseHexList(p.getProperty("propagation.prioritise_destinations")));

        // Control-allowed identity hashes
        config.put(CFG_CONTROL_ALLOWED_IDENTITIES, parseHexList(p.getProperty("propagation.control_allowed")));

        // Static peers
        config.put(CFG_STATIC_PEERS, parseHexList(p.getProperty("propagation.static_peers")));

        // Ignored destinations (from file)
        config.put(CFG_IGNORED_DESTINATIONS, loadHashesFromFile(ignoredPath));

        // Allowed identities for auth (from file)
        config.put(CFG_ALLOWED_IDENTITIES, loadHashesFromFile(allowedPath));
    }

    // ── Config helpers ────────────────────────────────────────────────────────

    private static int parseIntOrDefault(Properties p, String key, int def) {
        String v = p.getProperty(key);
        if (v == null) return def;
        try { return Integer.parseInt(v.trim()); } catch (NumberFormatException e) { return def; }
    }

    private static double parseDoubleOrDefault(Properties p, String key, double def) {
        String v = p.getProperty(key);
        if (v == null) return def;
        try { return Double.parseDouble(v.trim()); } catch (NumberFormatException e) { return def; }
    }

    private static int clampMinInt(int value, int min) {
        return Math.max(value, min);
    }

    private static List<byte[]> parseHexList(String csv) {
        List<byte[]> result = new ArrayList<>();
        if (csv == null || csv.trim().isEmpty()) return result;
        for (String part : csv.split(",")) {
            String h = part.trim();
            if (h.length() == RNS.TRUNCATED_HASHLENGTH / 8 * 2) {
                try { result.add(hexToBytes(h)); } catch (IllegalArgumentException ignored) {}
            }
        }
        return result;
    }

    private static List<byte[]> loadHashesFromFile(String filePath) {
        List<byte[]> result = new ArrayList<>();
        Path p = Paths.get(filePath);
        if (!Files.isRegularFile(p)) return result;
        try {
            for (String line : Files.readAllLines(p, StandardCharsets.UTF_8)) {
                String h = line.trim();
                if (h.length() == RNS.TRUNCATED_HASHLENGTH / 8 * 2) {
                    try { result.add(hexToBytes(h)); } catch (IllegalArgumentException ignored) {}
                }
            }
        } catch (IOException e) {
            System.err.println("Error loading hashes from " + filePath + ": " + e.getMessage());
        }
        return result;
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

    // ── Default config ────────────────────────────────────────────────────────

    private static void createDefaultConfig(Path path) throws IOException {
        Files.createDirectories(path.getParent());
        Files.writeString(path, defaultConfig(), StandardCharsets.UTF_8);
    }

    private static String defaultConfig() {
        return
            "# LXM Daemon configuration file.\n" +
            "# Edit to suit your intended usage.\n" +
            "#\n" +
            "# Keys use the format: section.key = value\n" +
            "\n" +
            "# ─── [propagation] ───────────────────────────────────────────────────────────\n" +
            "\n" +
            "# Whether to enable the propagation node\n" +
            "propagation.enable_node = false\n" +
            "\n" +
            "# Identity hashes allowed to control and query this propagation node\n" +
            "# propagation.control_allowed = 7d7e542829b40f32364499b27438dba8, 437229f8e29598b2282b88bad5e44698\n" +
            "\n" +
            "# Optional name for this node, included in announces\n" +
            "# propagation.node_name = Anonymous Propagation Node\n" +
            "\n" +
            "# Automatic announce interval in minutes (default: 360 = 6 hours)\n" +
            "propagation.announce_interval = 360\n" +
            "\n" +
            "# Announce when the node starts\n" +
            "propagation.announce_at_start = true\n" +
            "\n" +
            "# Automatically peer with other propagation nodes on the network\n" +
            "propagation.autopeer = true\n" +
            "\n" +
            "# Maximum peering depth (hops) for automatically discovered peers\n" +
            "propagation.autopeer_maxdepth = 6\n" +
            "\n" +
            "# Maximum message store size in megabytes (default: 500)\n" +
            "# propagation.message_storage_limit = 500\n" +
            "\n" +
            "# Maximum accepted size per incoming propagation message, in kilobytes\n" +
            "# propagation.propagation_message_max_accepted_size = 256\n" +
            "\n" +
            "# Maximum accepted size per propagation node sync, in kilobytes\n" +
            "# propagation.propagation_sync_max_accepted_size = 10240\n" +
            "\n" +
            "# Target stamp cost required to deliver messages via this node\n" +
            "# propagation.propagation_stamp_cost_target = 16\n" +
            "\n" +
            "# Stamp cost flexibility (accept lower cost from other nodes, not direct clients)\n" +
            "# propagation.propagation_stamp_cost_flexibility = 3\n" +
            "\n" +
            "# Peering cost required for a remote node to peer with this node\n" +
            "# propagation.peering_cost = 18\n" +
            "\n" +
            "# Maximum remote peering cost this node will accept when auto-peering\n" +
            "# propagation.remote_peering_cost_max = 26\n" +
            "\n" +
            "# Destinations to prioritise in the message store (comma-separated hashes)\n" +
            "# propagation.prioritise_destinations = 41d20c727598a3fbbdf9106133a3a0ed\n" +
            "\n" +
            "# Maximum number of propagation node peers (default: 20)\n" +
            "# propagation.max_peers = 20\n" +
            "\n" +
            "# Static propagation node peers (comma-separated destination hashes)\n" +
            "# propagation.static_peers = e17f833c4ddf8890dd3a79a6fea8161d\n" +
            "\n" +
            "# Only accept incoming propagation from configured static peers\n" +
            "# propagation.from_static_only = false\n" +
            "\n" +
            "# Require authentication for message sync.\n" +
            "# Allowed identity hashes are loaded from <configdir>/allowed (one hash per line).\n" +
            "propagation.auth_required = false\n" +
            "\n" +
            "# ─── [lxmf] ──────────────────────────────────────────────────────────────────\n" +
            "\n" +
            "# Display name for the LXMF delivery destination announced to the network\n" +
            "lxmf.display_name = Anonymous Peer\n" +
            "\n" +
            "# Announce the delivery destination when the daemon starts\n" +
            "lxmf.announce_at_start = false\n" +
            "\n" +
            "# Periodic announce interval for the delivery destination, in minutes\n" +
            "# lxmf.announce_interval = 360\n" +
            "\n" +
            "# Maximum accepted unpacked size for directly received messages, in kilobytes\n" +
            "lxmf.delivery_transfer_max_accepted_size = 1000\n" +
            "\n" +
            "# External program to run when a message is received.\n" +
            "# The program receives the full path to the saved message file as its argument.\n" +
            "# Example: the line below deletes the message immediately after receipt.\n" +
            "# lxmf.on_inbound = rm\n";
    }

    // ── Help ──────────────────────────────────────────────────────────────────

    private static void printUsage() {
        System.err.println("Usage: LxmdDaemon [--config DIR] [--rnsconfig DIR] [-p] [-i PATH] [-v] [-q] [-s] [--exampleconfig]");
    }
}
