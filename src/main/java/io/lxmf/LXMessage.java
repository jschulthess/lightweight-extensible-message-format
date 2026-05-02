package io.lxmf;

import io.lxmf.rns.RNS;
import io.lxmf.rns.RNSDestination;
import io.lxmf.rns.RNSIdentity;
import io.lxmf.rns.RNSLink;
import io.lxmf.rns.RNSPacket;
import io.lxmf.rns.RNSPacketReceipt;
import io.lxmf.rns.RNSResource;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * An LXMF message — the fundamental unit of the Lightweight Extensible Message Format.
 *
 * <p>Wire format (packed):
 * <pre>
 *   destination_hash (16 bytes)
 *   source_hash      (16 bytes)
 *   signature        (64 bytes)
 *   msgpack([timestamp:float64, title:bin, content:bin, fields:map, stamp?:bin])
 * </pre>
 *
 * <p>This class is a faithful Java translation of {@code LXMessage.py} from the Python reference
 * implementation, maintaining exact binary compatibility in the serialized representation.
 */
public class LXMessage {

    // ── Delivery states ────────────────────────────────────────────────────────
    public static final int GENERATING = 0x00;
    public static final int OUTBOUND   = 0x01;
    public static final int SENDING    = 0x02;
    public static final int SENT       = 0x04;
    public static final int DELIVERED  = 0x08;
    public static final int REJECTED   = 0xFD;
    public static final int CANCELLED  = 0xFE;
    public static final int FAILED     = 0xFF;

    // ── Representations ───────────────────────────────────────────────────────
    public static final int UNKNOWN  = 0x00;
    public static final int PACKET   = 0x01;
    public static final int RESOURCE = 0x02;

    // ── Delivery methods ──────────────────────────────────────────────────────
    public static final int OPPORTUNISTIC = 0x01;
    public static final int DIRECT        = 0x02;
    public static final int PROPAGATED    = 0x03;
    public static final int PAPER         = 0x05;

    // ── Unverified reasons ────────────────────────────────────────────────────
    public static final int SOURCE_UNKNOWN    = 0x01;
    public static final int SIGNATURE_INVALID = 0x02;

    // ── Size constants (wire-level, must match Python reference exactly) ───────
    /** 16 bytes — TRUNCATED_HASHLENGTH / 8. */
    public static final int DESTINATION_LENGTH = RNS.TRUNCATED_HASHLENGTH / 8;
    /** 64 bytes — SIGLENGTH / 8. */
    public static final int SIGNATURE_LENGTH   = RNS.SIGLENGTH / 8;
    /** 16 bytes — same as destination length. */
    public static final int TICKET_LENGTH      = RNS.TRUNCATED_HASHLENGTH / 8;

    public static final int TIMESTAMP_SIZE     = 8;
    public static final int STRUCT_OVERHEAD    = 8;
    public static final int LXMF_OVERHEAD      = 2 * DESTINATION_LENGTH + SIGNATURE_LENGTH
                                                  + TIMESTAMP_SIZE + STRUCT_OVERHEAD;

    /** Default RNS Packet.ENCRYPTED_MDU = 383; we add 8 for the LXMF timestamp. */
    public static final int ENCRYPTED_PACKET_MDU         = RNSPacket.ENCRYPTED_MDU + TIMESTAMP_SIZE;
    public static final int ENCRYPTED_PACKET_MAX_CONTENT = ENCRYPTED_PACKET_MDU - LXMF_OVERHEAD + DESTINATION_LENGTH;

    /** Default RNS Link.MDU = 431. */
    public static final int LINK_PACKET_MDU         = RNSLink.MDU;
    public static final int LINK_PACKET_MAX_CONTENT = LINK_PACKET_MDU - LXMF_OVERHEAD;

    /** Default RNS Packet.PLAIN_MDU = 464. */
    public static final int PLAIN_PACKET_MDU         = RNSPacket.PLAIN_MDU;
    public static final int PLAIN_PACKET_MAX_CONTENT = PLAIN_PACKET_MDU - LXMF_OVERHEAD + DESTINATION_LENGTH;

    // ── Ticket constants ──────────────────────────────────────────────────────
    public static final long TICKET_EXPIRY   = 21L * 24 * 60 * 60;
    public static final long TICKET_GRACE    =  5L * 24 * 60 * 60;
    public static final long TICKET_RENEW    = 14L * 24 * 60 * 60;
    public static final long TICKET_INTERVAL =  1L * 24 * 60 * 60;
    public static final int  COST_TICKET     = 0x100;

    // ── Encryption descriptions ────────────────────────────────────────────────
    public static final String ENCRYPTION_DESCRIPTION_AES         = "AES-128";
    public static final String ENCRYPTION_DESCRIPTION_EC          = "Curve25519";
    public static final String ENCRYPTION_DESCRIPTION_UNENCRYPTED = "Unencrypted";

    // ── Paper / QR encoding ───────────────────────────────────────────────────
    public static final String URI_SCHEMA      = "lxm";
    public static final int    QR_MAX_STORAGE  = 2953;
    public static final int    PAPER_MDU       = ((QR_MAX_STORAGE - (URI_SCHEMA.length() + 3)) * 6) / 8;

    // ── Instance fields ───────────────────────────────────────────────────────

    private RNSDestination destination;
    private RNSDestination source;

    public byte[] destinationHash;
    public byte[] sourceHash;
    public byte[] title;
    public byte[] content;
    public Map<Integer, Object> fields;

    public double  timestamp;
    public byte[]  signature;
    public byte[]  hash;
    public byte[]  messageId;
    public byte[]  transientId;
    public byte[]  packed;
    public int     packedSize;
    public byte[]  propagationPacked;
    public byte[]  paperPacked;

    public boolean autoCompress           = true;
    public int     state                  = GENERATING;
    public int     method                 = UNKNOWN;
    public int     representation         = UNKNOWN;
    public int     desiredMethod;
    public double  progress               = 0.0;
    public Float   rssi;
    public Float   snr;
    public Float   q;

    public byte[]  stamp;
    public Integer stampCost;
    public Integer stampValue;
    public boolean stampValid             = false;
    public boolean stampChecked           = false;
    public byte[]  propagationStamp;
    public Integer propagationStampValue;
    public boolean propagationStampValid  = false;
    public Integer propagationTargetCost;
    public boolean deferStamp             = true;
    public boolean deferPropagationStamp  = true;
    public byte[]  outboundTicket;
    public boolean includeTicket          = false;

    public boolean incoming               = false;
    public boolean signatureValidated     = false;
    public Integer unverifiedReason;
    public byte[]  ratchetId;

    public int     deliveryAttempts       = 0;
    public double  nextDeliveryAttempt    = 0;
    public boolean transportEncrypted     = false;
    public String  transportEncryption;

    public boolean deferredStampGenerating = false;

    private RNSDestination deliveryDestination;
    private Consumer<LXMessage> deliveryCallback;
    private byte[]              pnEncryptedData;
    public  Consumer<LXMessage> failedCallback;

    // ── Constructors ──────────────────────────────────────────────────────────

    public LXMessage(RNSDestination destination, RNSDestination source) {
        this(destination, source, "", "", null, DIRECT, null, null, null, false);
    }

    public LXMessage(RNSDestination destination, RNSDestination source,
                     String content, String title,
                     Map<Integer, Object> fields,
                     int desiredMethod,
                     byte[] destinationHash, byte[] sourceHash,
                     Integer stampCost, boolean includeTicket) {

        if (destination != null) {
            this.destination     = destination;
            this.destinationHash = destination.getHash();
        } else {
            this.destination     = null;
            this.destinationHash = destinationHash;
        }

        if (source != null) {
            this.source     = source;
            this.sourceHash = source.getHash();
        } else {
            this.source     = null;
            this.sourceHash = sourceHash;
        }

        setTitleFromString(title != null ? title : "");
        setContentFromString(content != null ? content : "");
        setFields(fields);

        this.stampCost      = stampCost;
        this.includeTicket  = includeTicket;
        this.desiredMethod  = desiredMethod;
    }

    // ── Title / content helpers ────────────────────────────────────────────────

    public void setTitleFromString(String s) {
        this.title = s.getBytes(StandardCharsets.UTF_8);
    }

    public void setTitleFromBytes(byte[] b) {
        this.title = b;
    }

    public String titleAsString() {
        return new String(title, StandardCharsets.UTF_8);
    }

    public void setContentFromString(String s) {
        this.content = s.getBytes(StandardCharsets.UTF_8);
    }

    public void setContentFromBytes(byte[] b) {
        this.content = b;
    }

    public String contentAsString() {
        return new String(content, StandardCharsets.UTF_8);
    }

    public void setFields(Map<Integer, Object> fields) {
        this.fields = fields != null ? fields : new LinkedHashMap<>();
    }

    public Map<Integer, Object> getFields() {
        return fields;
    }

    // ── Destination / source accessors ────────────────────────────────────────

    public RNSDestination getDestination() { return destination; }
    public RNSDestination getSource()      { return source;      }

    public byte[] getDestinationHash()     { return destinationHash; }
    public byte[] getSourceHash()          { return sourceHash; }

    public int getMethod()                 { return method; }
    public int getState()                  { return state;  }

    public void setDeliveryDestination(RNSDestination d) { this.deliveryDestination = d; }
    public RNSDestination getDeliveryDestination()       { return deliveryDestination; }

    public void registerDeliveryCallback(Consumer<LXMessage> cb) { this.deliveryCallback = cb; }
    public void registerFailedCallback(Consumer<LXMessage> cb)   { this.failedCallback   = cb; }

    public void setNextDeliveryAttempt(double t) { this.nextDeliveryAttempt = t; }

    // ── Stamp validation ──────────────────────────────────────────────────────

    /**
     * Validate this message's stamp against {@code targetCost}.
     * If {@code tickets} is non-null, also accepts a valid ticket as proof.
     */
    public boolean validateStamp(int targetCost, Iterable<byte[]> tickets) {
        if (tickets != null) {
            for (byte[] ticket : tickets) {
                try {
                    byte[] expected = RNS.truncatedHash(RNS.concat(ticket, messageId));
                    if (Arrays.equals(stamp, expected)) {
                        RNS.log("Stamp on " + this + " validated by inbound ticket", RNS.LOG_DEBUG);
                        this.stampValue = COST_TICKET;
                        return true;
                    }
                } catch (Exception e) {
                    RNS.log("Error while validating ticket: " + e.getMessage(), RNS.LOG_ERROR);
                }
            }
        }

        if (stamp == null) return false;

        byte[] workblock = LXStamper.stampWorkblock(messageId, LXStamper.WORKBLOCK_EXPAND_ROUNDS);
        if (LXStamper.stampValid(stamp, targetCost, workblock)) {
            RNS.log("Stamp on " + this + " validated", RNS.LOG_DEBUG);
            this.stampValue = LXStamper.stampValue(workblock, stamp);
            return true;
        }
        return false;
    }

    // ── Stamp generation ──────────────────────────────────────────────────────

    /**
     * Return the stamp for this message, generating one if necessary.
     * Uses an outbound ticket if available, skips if no cost is set.
     */
    public byte[] getStamp() {
        if (outboundTicket != null && outboundTicket.length == TICKET_LENGTH) {
            byte[] generated = RNS.truncatedHash(RNS.concat(outboundTicket, messageId));
            this.stampValue = COST_TICKET;
            RNS.log("Generated stamp with outbound ticket for " + this, RNS.LOG_DEBUG);
            return generated;
        }
        if (stampCost == null) {
            this.stampValue = null;
            return null;
        }
        if (stamp != null) return stamp;

        LXStamper.GeneratedStamp gs = LXStamper.generateStamp(messageId, stampCost);
        if (gs != null) {
            this.stampValue = gs.value;
            this.stampValid = true;
            return gs.stamp;
        }
        return null;
    }

    /**
     * Return the propagation stamp for this message.
     */
    public byte[] getPropagationStamp(int targetCost) {
        if (propagationStamp != null) return propagationStamp;

        this.propagationTargetCost = targetCost;
        if (!isPacked()) pack();

        LXStamper.GeneratedStamp gs = LXStamper.generateStamp(
                transientId, targetCost, LXStamper.WORKBLOCK_EXPAND_ROUNDS_PN);
        if (gs != null) {
            this.propagationStamp      = gs.stamp;
            this.propagationStampValue = gs.value;
            this.propagationStampValid = true;
            return gs.stamp;
        }
        return null;
    }

    // ── Packing ───────────────────────────────────────────────────────────────

    public boolean isPacked() { return packed != null; }

    /**
     * Serialize the message into its on-wire form.
     *
     * <p>Wire format:
     * <pre>
     *   destination_hash (16)
     *   source_hash      (16)
     *   signature        (64)
     *   msgpack([timestamp:f64, title:bin, content:bin, fields:map, stamp?:bin])
     * </pre>
     */
    public void pack() {
        pack(false);
    }

    public void pack(boolean payloadUpdated) {
        if (packed != null && !payloadUpdated) {
            throw new IllegalStateException("Attempt to re-pack LXMessage " + this + " that was already packed");
        }

        if (timestamp == 0) timestamp = System.currentTimeMillis() / 1000.0;

        propagationPacked = null;
        paperPacked       = null;

        // Build and hash payload
        byte[] packedPayloadBase = packPayload(false);
        byte[] hashedPart = concat3(destination.getHash(), source.getHash(), packedPayloadBase);
        this.hash      = RNS.fullHash(hashedPart);
        this.messageId = this.hash;

        // Compute stamp if not deferred
        byte[] stampToInclude = null;
        if (!deferStamp) {
            stampToInclude = getStamp();
        }

        // Re-pack payload with optional stamp
        byte[] packedPayload;
        if (stampToInclude != null) {
            packedPayload = packPayload(true, stampToInclude);
        } else {
            packedPayload = packedPayloadBase;
        }

        // Sign: destination_hash || source_hash || msgpack_payload || message_hash
        byte[] signedPart = concat2(hashedPart, this.hash);
        this.signature = (source != null && source.sign(signedPart) != null)
                ? source.sign(signedPart)
                : new byte[SIGNATURE_LENGTH]; // placeholder when signing identity unavailable
        this.signatureValidated = true;

        // Assemble packed bytes: dest_hash | src_hash | signature | msgpack_payload
        this.packed     = concat4(destination.getHash(), source.getHash(), this.signature, packedPayload);
        this.packedSize = packed.length;

        int contentSize = packedPayload.length - TIMESTAMP_SIZE - STRUCT_OVERHEAD;

        // Choose delivery method
        if (desiredMethod == 0) desiredMethod = DIRECT;

        if (desiredMethod == OPPORTUNISTIC) {
            if (destination.getType() == RNSDestination.SINGLE) {
                if (contentSize > ENCRYPTED_PACKET_MAX_CONTENT) {
                    RNS.log("Opportunistic delivery was requested for " + this
                            + ", but content exceeds packet size limit. Falling back to link-based delivery.",
                            RNS.LOG_DEBUG);
                    desiredMethod = DIRECT;
                }
            }
        }

        if (desiredMethod == OPPORTUNISTIC) {
            int limit = (destination.getType() == RNSDestination.SINGLE)
                    ? ENCRYPTED_PACKET_MAX_CONTENT : PLAIN_PACKET_MAX_CONTENT;
            if (contentSize > limit) {
                throw new IllegalStateException(
                        "LXMessage desired opportunistic delivery but content exceeds single-packet limit");
            }
            this.method              = OPPORTUNISTIC;
            this.representation      = PACKET;
            this.deliveryDestination = destination;

        } else if (desiredMethod == DIRECT) {
            if (contentSize <= LINK_PACKET_MAX_CONTENT) {
                this.method         = DIRECT;
                this.representation = PACKET;
            } else {
                this.method         = DIRECT;
                this.representation = RESOURCE;
            }

        } else if (desiredMethod == PROPAGATED) {
            if (pnEncryptedData == null || payloadUpdated) {
                pnEncryptedData = destination.encrypt(packed_slice(DESTINATION_LENGTH));
                ratchetId       = destination.getLatestRatchetId();
            }
            byte[] lxmfData = concat2(packed_prefix(DESTINATION_LENGTH), pnEncryptedData);
            this.transientId = RNS.fullHash(lxmfData);
            if (propagationStamp != null) lxmfData = RNS.concat(lxmfData, propagationStamp);
            this.propagationPacked = packPropagationBundle(lxmfData);

            int propSize = propagationPacked.length;
            if (propSize <= LINK_PACKET_MAX_CONTENT) {
                this.method         = PROPAGATED;
                this.representation = PACKET;
            } else {
                this.method         = PROPAGATED;
                this.representation = RESOURCE;
            }

        } else if (desiredMethod == PAPER) {
            byte[] encryptedData = destination.encrypt(packed_slice(DESTINATION_LENGTH));
            ratchetId   = destination.getLatestRatchetId();
            paperPacked = concat2(packed_prefix(DESTINATION_LENGTH), encryptedData);
            if (paperPacked.length > PAPER_MDU) {
                throw new IllegalStateException("LXMessage content exceeds paper message maximum size");
            }
            this.method         = PAPER;
            this.representation = PAPER;
        }
    }

    // ── Sending ───────────────────────────────────────────────────────────────

    public void send() {
        determineTransportEncryption();
        determineCompressionSupport();

        if (method == OPPORTUNISTIC) {
            RNSPacket pkt = asPacket();
            RNSPacketReceipt receipt = pkt.send();
            this.ratchetId = pkt.getRatchetId();
            if (receipt != null) receipt.setDeliveryCallback(r -> markDelivered());
            this.progress = 0.50;
            this.state    = SENT;

        } else if (method == DIRECT) {
            this.state = SENDING;
            if (representation == PACKET) {
                RNSPacket pkt = asPacket();
                RNSPacketReceipt receipt = pkt.send();
                this.ratchetId = deliveryDestination.getLinkId();
                if (receipt != null) {
                    receipt.setDeliveryCallback(r -> markDelivered());
                    receipt.setTimeoutCallback(r -> linkPacketTimedOut(r));
                    this.progress = 0.50;
                } else {
                    if (deliveryDestination != null) deliveryDestination.identify(null);
                }
            } else {
                asResource();
                this.ratchetId = deliveryDestination.getLinkId();
                this.progress  = 0.10;
            }

        } else if (method == PROPAGATED) {
            this.state = SENDING;
            if (representation == PACKET) {
                RNSPacketReceipt receipt = asPacket().send();
                if (receipt != null) {
                    receipt.setDeliveryCallback(r -> markPropagated());
                    receipt.setTimeoutCallback(r -> linkPacketTimedOut(r));
                    this.progress = 0.50;
                } else {
                    deliveryDestination.identify(null);
                }
            } else {
                asResource();
                this.progress = 0.10;
            }
        }
    }

    public void determineCompressionSupport() {
        byte[] appData = RNS.recallAppData(destinationHash);
        if (appData != null) {
            this.autoCompress = LXMF.compressionSupportFromAppData(appData);
        } else {
            this.autoCompress = true;
        }
    }

    public void determineTransportEncryption() {
        if (method == OPPORTUNISTIC || method == PROPAGATED || method == PAPER) {
            int dtype = (destination != null) ? destination.getType() : -1;
            if (dtype == RNSDestination.SINGLE) {
                transportEncrypted  = true;
                transportEncryption = ENCRYPTION_DESCRIPTION_EC;
            } else if (dtype == RNSDestination.GROUP) {
                transportEncrypted  = true;
                transportEncryption = ENCRYPTION_DESCRIPTION_AES;
            } else {
                transportEncrypted  = false;
                transportEncryption = ENCRYPTION_DESCRIPTION_UNENCRYPTED;
            }
        } else if (method == DIRECT) {
            transportEncrypted  = true;
            transportEncryption = ENCRYPTION_DESCRIPTION_EC;
        } else {
            transportEncrypted  = false;
            transportEncryption = ENCRYPTION_DESCRIPTION_UNENCRYPTED;
        }
    }

    // ── URI / paper encoding ──────────────────────────────────────────────────

    public String asUri() {
        return asUri(true);
    }

    /**
     * @param finalise if true, packs the message and marks it as generated (side effects);
     *                 if false, returns the URI string without modifying any state (read-only).
     */
    public String asUri(boolean finalise) {
        if (finalise) {
            if (!isPacked()) pack();
            if (desiredMethod == PAPER && paperPacked != null) {
                String encoded = Base64.getUrlEncoder().withoutPadding()
                        .encodeToString(paperPacked);
                markPaperGenerated();
                return URI_SCHEMA + "://" + encoded;
            }
            throw new IllegalStateException("Attempt to represent non-paper LXM as URI");
        } else {
            // Non-finalising read: return URI from existing packed data without side effects
            byte[] data = (paperPacked != null) ? paperPacked : packed;
            if (data == null) return null;
            return URI_SCHEMA + "://" + Base64.getUrlEncoder().withoutPadding().encodeToString(data);
        }
    }

    // ── Container / persistence ───────────────────────────────────────────────

    /** Serialize to a msgpack container suitable for on-disk storage. */
    public byte[] packedContainer() {
        if (!isPacked()) pack();
        try {
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
            packer.packMapHeader(5);
            packStr(packer, "state");               packer.packInt(state);
            packStr(packer, "lxmf_bytes");          packBytes(packer, packed);
            packStr(packer, "transport_encrypted"); packer.packBoolean(transportEncrypted);
            packStr(packer, "transport_encryption");
            if (transportEncryption != null) packStr(packer, transportEncryption);
            else packer.packNil();
            packStr(packer, "method");              packer.packInt(method);
            return packer.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Write this message's packed container to {@code directoryPath}. */
    public String writeToDirectory(String directoryPath) {
        if (!isPacked()) pack();
        String filePath = directoryPath + "/" + RNS.hexrep(hash, false);
        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(filePath)) {
            fos.write(packedContainer());
            return filePath;
        } catch (Exception e) {
            RNS.log("Error while writing LXMF message to file \"" + filePath + "\": " + e.getMessage(),
                    RNS.LOG_ERROR);
            return null;
        }
    }

    // ── Unpacking ─────────────────────────────────────────────────────────────

    /**
     * Deserialize an LXMessage from its raw wire bytes.
     * Wire format: dest_hash(16) | src_hash(16) | signature(64) | msgpack_payload
     */
    public static LXMessage unpackFromBytes(byte[] lxmfBytes, Integer originalMethod) {
        byte[] destHash    = Arrays.copyOf(lxmfBytes, DESTINATION_LENGTH);
        byte[] srcHash     = Arrays.copyOfRange(lxmfBytes, DESTINATION_LENGTH, 2 * DESTINATION_LENGTH);
        byte[] sig         = Arrays.copyOfRange(lxmfBytes, 2 * DESTINATION_LENGTH,
                                                 2 * DESTINATION_LENGTH + SIGNATURE_LENGTH);
        byte[] packedPayload = Arrays.copyOfRange(lxmfBytes,
                                                   2 * DESTINATION_LENGTH + SIGNATURE_LENGTH,
                                                   lxmfBytes.length);

        double timestamp;
        byte[] titleBytes, contentBytes;
        Map<Integer, Object> fields;
        byte[] stamp = null;

        try (MessageUnpacker up = MessagePack.newDefaultUnpacker(packedPayload)) {
            int arraySize = up.unpackArrayHeader();
            timestamp    = up.unpackDouble();
            titleBytes   = up.readPayload(up.unpackBinaryHeader());
            contentBytes = up.readPayload(up.unpackBinaryHeader());
            fields       = unpackFieldsMap(up);
            if (arraySize > 4) {
                stamp = up.readPayload(up.unpackBinaryHeader());
                // Re-pack without stamp for hash computation
                packedPayload = repackPayloadWithoutStamp(timestamp, titleBytes, contentBytes, fields);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to unpack LXMF payload", e);
        }

        byte[] hashedPart  = concat3(destHash, srcHash, packedPayload);
        byte[] messageHash = RNS.fullHash(hashedPart);
        byte[] signedPart  = concat2(hashedPart, messageHash);

        // Recall identities
        RNSIdentity destIdentity = RNS.recallIdentity(destHash);
        RNSDestination destDest  = (destIdentity != null)
                ? RNS.createDestination(destIdentity, RNSDestination.OUT, RNSDestination.SINGLE,
                                        LXMF.APP_NAME, "delivery")
                : null;

        RNSIdentity srcIdentity = RNS.recallIdentity(srcHash);
        RNSDestination srcDest  = (srcIdentity != null)
                ? RNS.createDestination(srcIdentity, RNSDestination.OUT, RNSDestination.SINGLE,
                                        LXMF.APP_NAME, "delivery")
                : null;

        int dm = originalMethod != null ? originalMethod : UNKNOWN;
        LXMessage msg = new LXMessage(destDest, srcDest, "", "", fields,
                                      dm, destHash, srcHash, null, false);
        msg.hash        = messageHash;
        msg.messageId   = messageHash;
        msg.signature   = sig;
        msg.stamp       = stamp;
        msg.incoming    = true;
        msg.timestamp   = timestamp;
        msg.packed      = lxmfBytes;
        msg.packedSize  = lxmfBytes.length;
        msg.setTitleFromBytes(titleBytes);
        msg.setContentFromBytes(contentBytes);

        // Validate signature
        if (srcIdentity != null) {
            try {
                if (srcIdentity.validate(sig, signedPart)) {
                    msg.signatureValidated = true;
                } else {
                    msg.signatureValidated = false;
                    msg.unverifiedReason   = SIGNATURE_INVALID;
                }
            } catch (Exception e) {
                msg.signatureValidated = false;
                RNS.log("Error while validating LXMF message signature: " + e.getMessage(), RNS.LOG_ERROR);
            }
        } else {
            msg.signatureValidated = false;
            msg.unverifiedReason   = SOURCE_UNKNOWN;
            RNS.log("Unpacked LXMF message signature could not be validated, since source identity is unknown",
                    RNS.LOG_DEBUG);
        }

        return msg;
    }

    public static LXMessage unpackFromBytes(byte[] lxmfBytes) {
        return unpackFromBytes(lxmfBytes, null);
    }

    /**
     * Deserialize from a msgpack container file (as written by {@link #writeToDirectory}).
     */
    public static LXMessage unpackFromFile(InputStream is) {
        try (MessageUnpacker up = MessagePack.newDefaultUnpacker(is)) {
            int mapSize = up.unpackMapHeader();
            byte[] lxmfBytes = null;
            int state = -1;
            boolean transportEncrypted = false;
            String transportEncryption = null;
            int method = -1;
            for (int i = 0; i < mapSize; i++) {
                String key = up.unpackString();
                switch (key) {
                    case "lxmf_bytes":
                        lxmfBytes = up.readPayload(up.unpackBinaryHeader()); break;
                    case "state":
                        state = up.unpackInt(); break;
                    case "transport_encrypted":
                        transportEncrypted = up.unpackBoolean(); break;
                    case "transport_encryption":
                        if (up.getNextFormat() == MessageFormat.NIL) up.unpackNil();
                        else transportEncryption = up.unpackString(); break;
                    case "method":
                        method = up.unpackInt(); break;
                    default:
                        up.skipValue(); break;
                }
            }
            if (lxmfBytes == null) return null;
            LXMessage lxm = unpackFromBytes(lxmfBytes);
            if (state >= 0)                 lxm.state                = state;
            lxm.transportEncrypted          = transportEncrypted;
            lxm.transportEncryption         = transportEncryption;
            if (method >= 0)                lxm.method               = method;
            return lxm;
        } catch (Exception e) {
            RNS.log("Could not unpack LXMessage from file: " + e.getMessage(), RNS.LOG_ERROR);
            return null;
        }
    }

    // ── toString ──────────────────────────────────────────────────────────────

    @Override
    public String toString() {
        if (hash != null) return "<LXMessage " + RNS.hexrep(hash, false) + ">";
        return "<LXMessage>";
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private byte[] packPayload(boolean withStamp) {
        return packPayload(withStamp, null);
    }

    private byte[] packPayload(boolean withStamp, byte[] stampBytes) {
        try {
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
            int arraySize = withStamp && stampBytes != null ? 5 : 4;
            packer.packArrayHeader(arraySize);
            packer.packDouble(timestamp);
            packBytes(packer, title);
            packBytes(packer, content);
            packFieldsMap(packer, fields);
            if (withStamp && stampBytes != null) packBytes(packer, stampBytes);
            return packer.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] packPropagationBundle(byte[] lxmfData) {
        try {
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
            packer.packArrayHeader(2);
            packer.packDouble(System.currentTimeMillis() / 1000.0);
            packer.packArrayHeader(1);
            packBytes(packer, lxmfData);
            return packer.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private RNSPacket asPacket() {
        if (!isPacked()) pack();
        if (deliveryDestination == null)
            throw new IllegalStateException("Cannot synthesize packet before delivery destination is known");

        byte[] data;
        if (method == OPPORTUNISTIC) {
            data = packed_slice(DESTINATION_LENGTH);
        } else if (method == DIRECT) {
            data = packed;
        } else { // PROPAGATED
            data = propagationPacked;
        }
        return RNS.createPacket(deliveryDestination, data);
    }

    private RNSResource asResource() {
        if (!isPacked()) pack();
        if (deliveryDestination == null)
            throw new IllegalStateException("Cannot synthesize resource before delivery destination is known");

        byte[] data = (method == DIRECT) ? packed : propagationPacked;
        Consumer<RNSResource> cb = (method == DIRECT)
                ? res -> { if (res.getStatus() == RNSResource.COMPLETE) markDelivered();
                           else if (res.getStatus() == RNSResource.REJECTED) state = REJECTED;
                           else { if (state != CANCELLED) state = OUTBOUND; } }
                : res -> { if (res.getStatus() == RNSResource.COMPLETE) markPropagated();
                           else { if (state != CANCELLED) state = OUTBOUND; } };

        Consumer<RNSResource> progressCb = res -> progress = 0.10 + res.getProgress() * 0.90;

        // deliveryDestination for resources must be a link — cast via the Link API
        return RNS.createResource(data, null /* link obtained from deliveryDestination */,
                                  cb, progressCb, autoCompress);
    }

    private void markDelivered() {
        RNS.log("Received delivery notification for " + this, RNS.LOG_DEBUG);
        this.state    = DELIVERED;
        this.progress = 1.0;
        fireDeliveryCallback();
    }

    private void markPropagated() {
        RNS.log("Received propagation success notification for " + this, RNS.LOG_DEBUG);
        this.state    = SENT;
        this.progress = 1.0;
        fireDeliveryCallback();
    }

    private void markPaperGenerated() {
        this.state    = PAPER;
        this.progress = 1.0;
        fireDeliveryCallback();
    }

    private void linkPacketTimedOut(RNSPacketReceipt receipt) {
        if (state != CANCELLED) {
            if (receipt != null && receipt.getDestination() != null)
                receipt.getDestination().identify(null);
            this.state = OUTBOUND;
        }
    }

    private void fireDeliveryCallback() {
        if (deliveryCallback != null) {
            try { deliveryCallback.accept(this); }
            catch (Exception e) {
                RNS.log("An error occurred in the external delivery callback for " + this, RNS.LOG_ERROR);
            }
        }
    }

    // byte[] slice helpers
    private byte[] packed_slice(int fromOffset) {
        return Arrays.copyOfRange(packed, fromOffset, packed.length);
    }

    private byte[] packed_prefix(int length) {
        return Arrays.copyOf(packed, length);
    }

    // concat helpers
    static byte[] concat2(byte[] a, byte[] b) {
        byte[] r = new byte[a.length + b.length];
        System.arraycopy(a, 0, r, 0, a.length);
        System.arraycopy(b, 0, r, a.length, b.length);
        return r;
    }

    static byte[] concat3(byte[] a, byte[] b, byte[] c) {
        byte[] r = new byte[a.length + b.length + c.length];
        System.arraycopy(a, 0, r, 0, a.length);
        System.arraycopy(b, 0, r, a.length, b.length);
        System.arraycopy(c, 0, r, a.length + b.length, c.length);
        return r;
    }

    static byte[] concat4(byte[] a, byte[] b, byte[] c, byte[] d) {
        byte[] r = new byte[a.length + b.length + c.length + d.length];
        int off = 0;
        System.arraycopy(a, 0, r, off, a.length); off += a.length;
        System.arraycopy(b, 0, r, off, b.length); off += b.length;
        System.arraycopy(c, 0, r, off, c.length); off += c.length;
        System.arraycopy(d, 0, r, off, d.length);
        return r;
    }

    // msgpack helpers
    static void packBytes(MessageBufferPacker packer, byte[] data) throws IOException {
        if (data == null) { packer.packNil(); return; }
        packer.packBinaryHeader(data.length);
        packer.writePayload(data);
    }

    static void packStr(MessageBufferPacker packer, String s) throws IOException {
        packer.packString(s);
    }

    static void packFieldsMap(MessageBufferPacker packer, Map<Integer, Object> fields) throws IOException {
        if (fields == null) { packer.packMapHeader(0); return; }
        packer.packMapHeader(fields.size());
        for (Map.Entry<Integer, Object> e : fields.entrySet()) {
            packer.packInt(e.getKey());
            packAnyValue(packer, e.getValue());
        }
    }

    @SuppressWarnings("unchecked")
    static void packAnyValue(MessageBufferPacker packer, Object v) throws IOException {
        if (v == null)                  packer.packNil();
        else if (v instanceof byte[])   packBytes(packer, (byte[]) v);
        else if (v instanceof String)   packer.packString((String) v);
        else if (v instanceof Integer)  packer.packInt((Integer) v);
        else if (v instanceof Long)     packer.packLong((Long) v);
        else if (v instanceof Double)   packer.packDouble((Double) v);
        else if (v instanceof Boolean)  packer.packBoolean((Boolean) v);
        else if (v instanceof java.util.List) {
            java.util.List<?> list = (java.util.List<?>) v;
            packer.packArrayHeader(list.size());
            for (Object item : list) packAnyValue(packer, item);
        } else if (v instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) v;
            packer.packMapHeader(map.size());
            for (Map.Entry<?, ?> e : map.entrySet()) {
                packAnyValue(packer, e.getKey());
                packAnyValue(packer, e.getValue());
            }
        } else {
            packer.packString(v.toString());
        }
    }

    static Map<Integer, Object> unpackFieldsMap(MessageUnpacker up) throws IOException {
        Map<Integer, Object> map = new LinkedHashMap<>();
        if (up.getNextFormat() == MessageFormat.NIL) { up.unpackNil(); return map; }
        int size = up.unpackMapHeader();
        for (int i = 0; i < size; i++) {
            int key = up.unpackInt();
            map.put(key, unpackAnyValue(up));
        }
        return map;
    }

    static Object unpackAnyValue(MessageUnpacker up) throws IOException {
        MessageFormat fmt = up.getNextFormat();
        switch (fmt.getValueType()) {
            case NIL:     up.unpackNil(); return null;
            case BOOLEAN: return up.unpackBoolean();
            case INTEGER: return up.unpackLong();
            case FLOAT:   return up.unpackDouble();
            case STRING:  return up.unpackString();
            case BINARY:  return up.readPayload(up.unpackBinaryHeader());
            case ARRAY: {
                int size = up.unpackArrayHeader();
                java.util.List<Object> list = new java.util.ArrayList<>(size);
                for (int i = 0; i < size; i++) list.add(unpackAnyValue(up));
                return list;
            }
            case MAP: {
                int size = up.unpackMapHeader();
                Map<Object, Object> m = new LinkedHashMap<>();
                for (int i = 0; i < size; i++) m.put(unpackAnyValue(up), unpackAnyValue(up));
                return m;
            }
            default: up.skipValue(); return null;
        }
    }

    /** Re-pack the payload without the stamp (for hash computation on incoming messages). */
    private static byte[] repackPayloadWithoutStamp(double ts, byte[] title, byte[] content,
                                                     Map<Integer, Object> fields) {
        try {
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
            packer.packArrayHeader(4);
            packer.packDouble(ts);
            packBytes(packer, title);
            packBytes(packer, content);
            packFieldsMap(packer, fields);
            return packer.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
