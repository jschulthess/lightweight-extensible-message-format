# Java implementation of LXMF — Lightweight eXtensible Message Format for Reticulum

A feature-complete, wire-compatible Java translation of the Python [LXMF](https://github.com/markqvist/LXMF) library, built on top of the Java [reticulum-network-stack](https://github.com/jschulthess/reticulum-network-stack).

Seamless interoperability with the Python reference implementation is the primary goal: messages packed by this library can be unpacked by Python LXMF and vice versa.

## Features

- Full `LXMessage` pack/unpack with Ed25519 signing, encryption, and all delivery methods (OPPORTUNISTIC, DIRECT, PROPAGATED, PAPER)
- `LXMRouter` with outbound processing, direct and propagation-node delivery, inbound delivery callbacks, and full store-and-forward propagation node support
- Peer synchronisation protocol (offer/accept/resource transfer) via `LXMPeer`
- Proof-of-work stamping via `LXStamper`
- Client-side propagation node sync: path discovery, link establishment, message list negotiation, download, and acknowledgement
- LXM URI ingestion (`lxm://…`)
- Stamp and ticket handling for access control

## Requirements

- Java 11+
- Maven

## Dependency

Add via [JitPack](https://jitpack.io):

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>com.github.jschulthess</groupId>
        <artifactId>lightweight-extensible-message-format</artifactId>
        <version>0.9.6</version>
    </dependency>
</dependencies>
```

## Quick start

### 1. Initialise Reticulum and wire the provider

```java
import io.reticulum.Reticulum;
import io.reticulum.Transport;
import io.lxmf.rns.RNS;
import io.lxmf.rns.impl.ReticulumProvider;

Reticulum reticulum = new Reticulum("/path/to/reticulum/config");
Transport transport = Transport.start(reticulum);
RNS.initialize(new ReticulumProvider(transport));
```

This single call wires all of LXMF's network operations — identity management, destinations, links, packets, resources, and transport — to the live Reticulum stack.

### 2. Create a router and a delivery destination

```java
import io.lxmf.LXMRouter;
import io.lxmf.rns.RNSIdentity;

RNSIdentity identity = RNS.createIdentity(); // or load from file
LXMRouter router = new LXMRouter.Builder()
        .storagePath("/var/lxmf")
        .build(identity);

router.setDeliveryCallback(lxm -> {
    System.out.println("Received: " + new String(lxm.getContent()));
});
router.registerDeliveryIdentity(identity);
```

### 3. Send a message

```java
import io.lxmf.LXMessage;
import io.lxmf.rns.RNSDestination;

byte[] recipientHash = ...; // 16-byte truncated hash of recipient destination
RNSIdentity recipientIdentity = RNS.recallIdentity(recipientHash);
RNSDestination destination = RNS.createDestination(
        recipientIdentity, RNSDestination.OUT, RNSDestination.SINGLE, "lxmf", "delivery");

LXMessage msg = new LXMessage(destination, identity);
msg.setTitle("Hello");
msg.setContent("Hello from Java LXMF!".getBytes());
router.sendMessage(msg);
```

### 4. Sync from a propagation node

```java
byte[] propagationNodeHash = ...; // hash of a known PN
router.setOutboundPropagationNode(propagationNodeHash);
router.requestMessagesFromPropagationNode(identity, LXMRouter.PR_ALL_MESSAGES);
```

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    io.lxmf.*                             │
│  LXMRouter · LXMessage · LXMPeer · LXStamper · LXMF     │
└────────────────────────┬─────────────────────────────────┘
                         │ calls
┌────────────────────────▼─────────────────────────────────┐
│               io.lxmf.rns.*  (interfaces)                │
│  RNS · RNSProvider · RNSIdentity · RNSDestination        │
│  RNSLink · RNSPacket · RNSResource · RNSAnnounceHandler  │
└────────────────────────┬─────────────────────────────────┘
                         │ implemented by
┌────────────────────────▼─────────────────────────────────┐
│           io.lxmf.rns.impl.*  (adapters)                 │
│  ReticulumProvider · IdentityAdapter · DestinationAdapter│
│  LinkAdapter · LinkAsDestinationAdapter · PacketAdapter  │
│  ResourceAdapter · RequestReceiptAdapter · MsgPackHelper │
└────────────────────────┬─────────────────────────────────┘
                         │ wraps
┌────────────────────────▼─────────────────────────────────┐
│          io.reticulum.*  (native Java Reticulum)         │
│  Transport · Identity · Destination · Link · Packet …    │
└──────────────────────────────────────────────────────────┘
```

The `io.lxmf.rns` layer is a thin interface contract. `ReticulumProvider` is the only concrete implementation; it delegates everything to the native Reticulum stack.

### Key design notes

**Msgpack boundary** — The native `Link.request()` and request handlers exchange raw `byte[]`, while LXMF's abstraction exchanges `Object` (any msgpack-serialisable value). `MsgPackHelper` transparently encodes outgoing objects and decodes incoming bytes at this boundary.

**Encrypt/decrypt convention** — Python RNS uses `destination.encrypt()` for both directions: it encrypts on OUT destinations and decrypts on IN destinations (which hold the private key). `DestinationAdapter` preserves this convention by delegating to native `decrypt()` when the direction is IN.

**Dynamic app data** — `RNSDestination.setDefaultAppData(Supplier<byte[]>)` accepts a lambda so the router can generate fresh announce payloads reflecting current node state. The adapter evaluates the supplier on each `announce()` call and also keeps the native destination's static default in sync for path-response announces.

**Link status mapping**

| Native `LinkStatus` | LXMF constant |
|---|---|
| `PENDING`, `HANDSHAKE` | `RNSLink.PENDING` |
| `ACTIVE`, `STALE` | `RNSLink.ACTIVE` |
| `CLOSED` | `RNSLink.CLOSED` |

## Related projects

- [Reticulum](https://reticulum.network) — the underlying network stack
- [reticulum-network-stack](https://github.com/jschulthess/reticulum-network-stack) — native Java Reticulum implementation
- [LXMF (Python reference)](https://github.com/markqvist/LXMF) — the reference implementation this library is translated from
- [Sideband](https://github.com/markqvist/Sideband) — Python LXMF client application
