package io.lxmf;

import io.lxmf.rns.RNS;
import io.lxmf.support.StubDestination;
import io.lxmf.support.StubIdentity;
import io.lxmf.support.TestProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileInputStream;
import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link LXMessage} packing, unpacking, and signature validation.
 */
@DisplayName("LXMessage")
class LXMessageTest {

    // Shared provider that persists across all tests — identities are registered here.
    private static TestProvider provider;

    // Two stub identity/destination pairs used for the round-trip tests.
    private static StubIdentity sourceIdentity;
    private static StubIdentity destIdentity;
    private static StubDestination sourceDest;
    private static StubDestination destDest;

    @BeforeAll
    static void setup() {
        provider = TestProvider.install();

        sourceIdentity = new StubIdentity();
        destIdentity   = new StubIdentity();

        sourceDest = new StubDestination(sourceIdentity);
        destDest   = new StubDestination(destIdentity);

        // Register both so unpackFromBytes can recall them
        provider.remember(sourceIdentity);
        provider.remember(destIdentity);
    }

    // ── Construction ──────────────────────────────────────────────────────────

    @Nested
    @DisplayName("Construction")
    class ConstructionTests {

        @Test
        @DisplayName("title and content are stored as UTF-8")
        void titleAndContent() {
            LXMessage msg = new LXMessage(destDest, sourceDest,
                    "World", "Hello",
                    null, LXMessage.DIRECT,
                    null, null, null, false);

            assertAll(
                    () -> assertEquals("Hello", msg.titleAsString()),
                    () -> assertEquals("World", msg.contentAsString())
            );
        }

        @Test
        @DisplayName("fields map is non-null even when null is passed")
        void fieldsNotNull() {
            LXMessage msg = new LXMessage(destDest, sourceDest,
                    "", "", null, LXMessage.DIRECT,
                    null, null, null, false);
            assertNotNull(msg.getFields());
        }

        @Test
        @DisplayName("destinationHash comes from destination.getHash()")
        void destinationHash() {
            LXMessage msg = new LXMessage(destDest, sourceDest,
                    "", "", null, LXMessage.DIRECT,
                    null, null, null, false);
            assertArrayEquals(destIdentity.getHash(), msg.destinationHash);
        }

        @Test
        @DisplayName("sourceHash comes from source.getHash()")
        void sourceHash() {
            LXMessage msg = new LXMessage(destDest, sourceDest,
                    "", "", null, LXMessage.DIRECT,
                    null, null, null, false);
            assertArrayEquals(sourceIdentity.getHash(), msg.sourceHash);
        }
    }

    // ── Pack round-trip (DIRECT method) ───────────────────────────────────────

    @Nested
    @DisplayName("pack() round-trip")
    class PackRoundTripTests {

        private LXMessage buildMessage() {
            Map<Integer, Object> fields = new LinkedHashMap<>();
            fields.put(LXMF.FIELD_THREAD, "tid".getBytes());
            return new LXMessage(destDest, sourceDest,
                    "World", "Hello",
                    fields, LXMessage.DIRECT,
                    null, null, null, false);
        }

        @Test
        @DisplayName("isPacked() true after pack()")
        void isPackedAfterPack() {
            LXMessage msg = buildMessage();
            msg.pack();
            assertTrue(msg.isPacked());
        }

        @Test
        @DisplayName("packed bytes have correct structure")
        void packedStructure() {
            LXMessage msg = buildMessage();
            msg.pack();

            int D = LXMessage.DESTINATION_LENGTH;
            int S = LXMessage.SIGNATURE_LENGTH;

            assertAll(
                    () -> assertTrue(msg.packed.length > D * 2 + S,
                            "packed must contain at least header + some payload"),
                    () -> assertArrayEquals(destIdentity.getHash(),
                            Arrays.copyOf(msg.packed, D),
                            "first 16 bytes = destination hash"),
                    () -> assertArrayEquals(sourceIdentity.getHash(),
                            Arrays.copyOfRange(msg.packed, D, 2 * D),
                            "bytes 16..32 = source hash")
            );
        }

        @Test
        @DisplayName("message hash is 32 bytes and non-null after pack()")
        void messageHash() {
            LXMessage msg = buildMessage();
            msg.pack();
            assertNotNull(msg.hash);
            assertEquals(32, msg.hash.length);
        }

        @Test
        @DisplayName("unpackFromBytes round-trip: title and content survive")
        void unpackTitleAndContent() {
            LXMessage msg = buildMessage();
            msg.pack();

            LXMessage unpacked = LXMessage.unpackFromBytes(msg.packed);
            assertAll(
                    () -> assertEquals("Hello", unpacked.titleAsString()),
                    () -> assertEquals("World", unpacked.contentAsString())
            );
        }

        @Test
        @DisplayName("unpackFromBytes: timestamp > 0")
        void unpackTimestamp() {
            LXMessage msg = buildMessage();
            msg.pack();

            LXMessage unpacked = LXMessage.unpackFromBytes(msg.packed);
            assertTrue(unpacked.timestamp > 0);
        }

        @Test
        @DisplayName("unpackFromBytes: hashes match")
        void unpackHashes() {
            LXMessage msg = buildMessage();
            msg.pack();

            LXMessage unpacked = LXMessage.unpackFromBytes(msg.packed);
            assertAll(
                    () -> assertArrayEquals(destIdentity.getHash(), unpacked.destinationHash),
                    () -> assertArrayEquals(sourceIdentity.getHash(), unpacked.sourceHash)
            );
        }

        @Test
        @DisplayName("unpackFromBytes: FIELD_THREAD present in fields")
        void unpackFields() {
            LXMessage msg = buildMessage();
            msg.pack();

            LXMessage unpacked = LXMessage.unpackFromBytes(msg.packed);
            assertTrue(unpacked.fields.containsKey(LXMF.FIELD_THREAD),
                    "FIELD_THREAD should be preserved through pack/unpack");
        }

        @Test
        @DisplayName("unpackFromBytes: signature validated (known source)")
        void unpackSignatureValidated() {
            LXMessage msg = buildMessage();
            msg.pack();

            LXMessage unpacked = LXMessage.unpackFromBytes(msg.packed);
            assertTrue(unpacked.signatureValidated,
                    "Signature should be validated when source identity is registered");
        }
    }

    // ── Signature validation with unknown source ──────────────────────────────

    @Nested
    @DisplayName("Signature validation with unknown source")
    class UnknownSourceTests {

        @Test
        @DisplayName("signatureValidated=false and unverifiedReason=SOURCE_UNKNOWN")
        void unknownSource() {
            // Use a brand-new identity that is NOT registered in the provider
            StubIdentity unknownSrc = new StubIdentity();
            StubDestination unknownSrcDest = new StubDestination(unknownSrc);

            LXMessage msg = new LXMessage(destDest, unknownSrcDest,
                    "World", "Hello", null, LXMessage.DIRECT,
                    null, null, null, false);
            msg.pack();

            LXMessage unpacked = LXMessage.unpackFromBytes(msg.packed);
            assertAll(
                    () -> assertFalse(unpacked.signatureValidated),
                    () -> assertEquals(LXMessage.SOURCE_UNKNOWN, unpacked.unverifiedReason)
            );
        }
    }

    // ── writeToDirectory / unpackFromFile round-trip ──────────────────────────

    @Nested
    @DisplayName("writeToDirectory / unpackFromFile round-trip")
    class FileRoundTripTests {

        @Test
        @DisplayName("title and content survive a file round-trip")
        void fileRoundTrip(@TempDir Path tmpDir) throws Exception {
            LXMessage msg = new LXMessage(destDest, sourceDest,
                    "FileContent", "FileTitle", null, LXMessage.DIRECT,
                    null, null, null, false);
            msg.pack();

            String filePath = msg.writeToDirectory(tmpDir.toString());
            assertNotNull(filePath, "writeToDirectory should return the file path");

            try (FileInputStream fis = new FileInputStream(new File(filePath))) {
                LXMessage loaded = LXMessage.unpackFromFile(fis);
                assertNotNull(loaded);
                assertAll(
                        () -> assertEquals("FileTitle", loaded.titleAsString()),
                        () -> assertEquals("FileContent", loaded.contentAsString())
                );
            }
        }
    }
}
