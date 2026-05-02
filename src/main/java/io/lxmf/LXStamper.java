package io.lxmf;

import io.lxmf.rns.RNS;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Proof-of-work stamp generation and validation for LXMF messages.
 *
 * <p>A "stamp" is a 32-byte value such that
 * {@code SHA-256(workblock || stamp)} has at least {@code stamp_cost} leading zero bits.
 * The workblock is derived from the message ID via iterated HKDF expansion, making stamp
 * generation computationally expensive but validation cheap.
 *
 * <p>This class faithfully translates {@code LXStamper.py} from the Python reference
 * implementation, replacing Python's multiprocessing module with Java's thread pool.
 */
public final class LXStamper {

    /** HKDF expansion rounds for normal delivery stamps. */
    public static final int WORKBLOCK_EXPAND_ROUNDS         = 3000;
    /** HKDF expansion rounds for propagation-node stamps (cheaper). */
    public static final int WORKBLOCK_EXPAND_ROUNDS_PN      = 1000;
    /** HKDF expansion rounds for peering keys (very cheap). */
    public static final int WORKBLOCK_EXPAND_ROUNDS_PEERING = 25;
    /** Size of a stamp in bytes (SHA-256 output = 32 bytes). */
    public static final int STAMP_SIZE                      = RNS.HASHLENGTH / 8;
    /** Minimum transient-list size before multiprocessing is used for PN validation. */
    public static final int PN_VALIDATION_POOL_MIN_SIZE     = 256;

    /** Each active stamp-generation job; keyed by message ID bytes. */
    private static final Map<BytesKey, AtomicBoolean> activeJobs = new ConcurrentHashMap<>();

    private LXStamper() {}

    // ── Workblock ─────────────────────────────────────────────────────────────

    /**
     * Expand {@code material} into a large pseudo-random workblock via iterated HKDF.
     * Each round produces 256 bytes; the total workblock is {@code expandRounds * 256} bytes.
     *
     * <p>Wire-compatible with Python:
     * <pre>
     *   for n in range(expand_rounds):
     *       workblock += HKDF(length=256, IKM=material,
     *                         salt=SHA256(material + msgpack.packb(n)), info=None)
     * </pre>
     */
    public static byte[] stampWorkblock(byte[] material, int expandRounds) {
        byte[] workblock = new byte[expandRounds * 256];
        for (int n = 0; n < expandRounds; n++) {
            byte[] salt = RNS.fullHash(RNS.concat(material, msgpackInt(n)));
            byte[] block = RNS.hkdf(256, material, salt, null);
            System.arraycopy(block, 0, workblock, n * 256, 256);
        }
        return workblock;
    }

    /**
     * Count the number of leading zero bits in {@code SHA-256(workblock || stamp)}.
     */
    public static int stampValue(byte[] workblock, byte[] stamp) {
        byte[] material = RNS.fullHash(RNS.concat(workblock, stamp));
        BigInteger i = new BigInteger(1, material); // unsigned
        int bits = 256;
        int value = 0;
        BigInteger msb = BigInteger.ONE.shiftLeft(bits - 1);
        while (!i.testBit(bits - 1)) {
            i = i.shiftLeft(1);
            value++;
        }
        return value;
    }

    /**
     * Returns true when {@code SHA-256(workblock || stamp)} satisfies {@code targetCost}
     * leading zero bits.
     */
    public static boolean stampValid(byte[] stamp, int targetCost, byte[] workblock) {
        // target = 1 << (256 - targetCost);  result must be <= target to be valid
        byte[] result = RNS.fullHash(RNS.concat(workblock, stamp));
        BigInteger r = new BigInteger(1, result);
        BigInteger target = BigInteger.ONE.shiftLeft(256 - targetCost);
        return r.compareTo(target) <= 0;
    }

    // ── Peering key validation ─────────────────────────────────────────────────

    public static boolean validatePeeringKey(byte[] peeringId, byte[] peeringKey, int targetCost) {
        byte[] workblock = stampWorkblock(peeringId, WORKBLOCK_EXPAND_ROUNDS_PEERING);
        return stampValid(peeringKey, targetCost, workblock);
    }

    // ── Propagation-node stamp validation ─────────────────────────────────────

    /** Result of validating a single propagation-node stamp. */
    public static final class ValidatedPnStamp {
        public final byte[] transientId;
        public final byte[] lxmData;
        public final int    value;
        public final byte[] stamp;

        ValidatedPnStamp(byte[] transientId, byte[] lxmData, int value, byte[] stamp) {
            this.transientId = transientId;
            this.lxmData     = lxmData;
            this.value       = value;
            this.stamp       = stamp;
        }
    }

    /**
     * Validate a single propagation-node stamp from raw transient data.
     * The last {@link #STAMP_SIZE} bytes are treated as the stamp; the rest as lxm_data.
     * Returns null if invalid.
     */
    public static ValidatedPnStamp validatePnStamp(byte[] transientData, int targetCost) {
        int lxmfOverhead = LXMessage.LXMF_OVERHEAD;
        if (transientData.length <= lxmfOverhead + STAMP_SIZE) return null;

        byte[] lxmData    = Arrays.copyOf(transientData, transientData.length - STAMP_SIZE);
        byte[] stamp       = Arrays.copyOfRange(transientData, transientData.length - STAMP_SIZE, transientData.length);
        byte[] transientId = RNS.fullHash(lxmData);
        byte[] workblock   = stampWorkblock(transientId, WORKBLOCK_EXPAND_ROUNDS_PN);

        if (!stampValid(stamp, targetCost, workblock)) return null;
        int value = stampValue(workblock, stamp);
        return new ValidatedPnStamp(transientId, lxmData, value, stamp);
    }

    /**
     * Batch-validate a list of raw transient payloads.
     * Uses a thread pool when the list is large enough.
     */
    public static List<ValidatedPnStamp> validatePnStamps(List<byte[]> transientList, int targetCost) {
        if (transientList.size() <= PN_VALIDATION_POOL_MIN_SIZE) {
            return validatePnStampsSimple(transientList, targetCost);
        }
        return validatePnStampsThreaded(transientList, targetCost);
    }

    private static List<ValidatedPnStamp> validatePnStampsSimple(List<byte[]> list, int targetCost) {
        List<ValidatedPnStamp> results = new ArrayList<>();
        for (byte[] data : list) {
            ValidatedPnStamp v = validatePnStamp(data, targetCost);
            if (v != null) results.add(v);
        }
        return results;
    }

    private static List<ValidatedPnStamp> validatePnStampsThreaded(List<byte[]> list, int targetCost) {
        int cores   = Runtime.getRuntime().availableProcessors();
        int threads = Math.min(cores, (int) Math.ceil((double) list.size() / PN_VALIDATION_POOL_MIN_SIZE));
        RNS.log("Validating " + list.size() + " stamps using " + threads + " threads...", RNS.LOG_VERBOSE);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<ValidatedPnStamp>> futures = new ArrayList<>(list.size());

        for (byte[] data : list) {
            futures.add(pool.submit(() -> validatePnStamp(data, targetCost)));
        }
        pool.shutdown();

        List<ValidatedPnStamp> results = new ArrayList<>();
        for (Future<ValidatedPnStamp> f : futures) {
            try {
                ValidatedPnStamp v = f.get();
                if (v != null) results.add(v);
            } catch (Exception ignored) {}
        }
        RNS.log("Validation pool completed for " + list.size() + " stamps", RNS.LOG_VERBOSE);
        return results;
    }

    // ── Stamp generation ──────────────────────────────────────────────────────

    /** Result of stamp generation: the 32-byte stamp and its numeric value. */
    public static final class GeneratedStamp {
        public final byte[] stamp;
        public final int    value;

        GeneratedStamp(byte[] stamp, int value) {
            this.stamp = stamp;
            this.value = value;
        }
    }

    /**
     * Brute-force search for a valid stamp.
     *
     * <p>Uses all available CPU cores (via a thread pool) to find a 32-byte random value
     * that satisfies the cost requirement.  Can be cancelled with {@link #cancelWork(byte[])}.
     *
     * @param messageId     the message ID (or transient ID for PN stamps)
     * @param stampCost     required number of leading zero bits
     * @param expandRounds  workblock expansion rounds
     * @return the found stamp and its value, or null if cancelled
     */
    public static GeneratedStamp generateStamp(byte[] messageId, int stampCost, int expandRounds) {
        RNS.log("Generating stamp with cost " + stampCost + " for " + RNS.prettyhexrep(messageId) + "...",
                RNS.LOG_DEBUG);

        byte[] workblock  = stampWorkblock(messageId, expandRounds);
        AtomicBoolean cancelled = new AtomicBoolean(false);
        BytesKey key = new BytesKey(messageId);
        activeJobs.put(key, cancelled);

        long startTime = System.currentTimeMillis();
        GeneratedStamp result = findStamp(stampCost, workblock, cancelled);
        activeJobs.remove(key);

        double duration = (System.currentTimeMillis() - startTime) / 1000.0;
        if (result != null) {
            RNS.log("Stamp with value " + result.value + " generated in "
                    + RNS.prettytime(duration), RNS.LOG_DEBUG);
        }
        return result;
    }

    /** Overload using default expansion rounds for normal delivery stamps. */
    public static GeneratedStamp generateStamp(byte[] messageId, int stampCost) {
        return generateStamp(messageId, stampCost, WORKBLOCK_EXPAND_ROUNDS);
    }

    /** Signal the stamp-generation job for {@code messageId} to stop. */
    public static void cancelWork(byte[] messageId) {
        AtomicBoolean cancelled = activeJobs.get(new BytesKey(messageId));
        if (cancelled != null) cancelled.set(true);
    }

    private static GeneratedStamp findStamp(int stampCost, byte[] workblock, AtomicBoolean cancelled) {
        int cores = Runtime.getRuntime().availableProcessors();
        AtomicReference<byte[]> found   = new AtomicReference<>(null);
        AtomicLong totalRounds          = new AtomicLong(0);
        AtomicBoolean workerDone        = new AtomicBoolean(false);

        ExecutorService pool = Executors.newFixedThreadPool(cores);
        List<Future<?>> futures = new ArrayList<>(cores);

        for (int w = 0; w < cores; w++) {
            futures.add(pool.submit(() -> {
                byte[] pstamp = new byte[STAMP_SIZE];
                while (!cancelled.get() && !workerDone.get()) {
                    RNS.randomBytes(STAMP_SIZE);
                    pstamp = RNS.randomBytes(STAMP_SIZE);
                    totalRounds.incrementAndGet();
                    if (stampValid(pstamp, stampCost, workblock)) {
                        if (found.compareAndSet(null, pstamp)) {
                            workerDone.set(true);
                        }
                        return;
                    }
                }
            }));
        }

        // Wait for a result or cancellation
        for (Future<?> f : futures) {
            try { f.get(); } catch (Exception ignored) {}
        }
        pool.shutdownNow();

        byte[] stamp = found.get();
        if (stamp == null) return null;
        int value = stampValue(workblock, stamp);
        return new GeneratedStamp(stamp, value);
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /**
     * Encode an integer as a msgpack byte sequence, wire-compatible with Python's
     * {@code msgpack.packb(n)}.
     */
    static byte[] msgpackInt(int n) {
        try {
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
            packer.packInt(n);
            return packer.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Wrapper to use byte arrays as map keys with value-based equality.
     */
    private static final class BytesKey {
        private final byte[] data;

        BytesKey(byte[] data) { this.data = data; }

        @Override public boolean equals(Object o) {
            return o instanceof BytesKey && Arrays.equals(data, ((BytesKey) o).data);
        }

        @Override public int hashCode() { return Arrays.hashCode(data); }
    }
}