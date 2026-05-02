# Module layout

```
src/main/java/io/lxmf/
├── LXMF.java                        protocol constants and wire-format helpers
├── LXMessage.java                   message model — pack, unpack, sign, stamp, send
├── LXMRouter.java                   central router and propagation node
├── LXMPeer.java                     peer relationship and sync state machine
├── LXStamper.java                   proof-of-work stamp generation and validation
├── handlers/
│   ├── LXMFDeliveryAnnounceHandler.java
│   └── LXMFPropagationAnnounceHandler.java
└── rns/
    ├── RNS.java                     static facade (call RNS.initialize() once)
    ├── RNSProvider.java             interface for the network implementation
    ├── RNSIdentity.java
    ├── RNSDestination.java
    ├── RNSLink.java
    ├── RNSPacket.java
    ├── RNSPacketReceipt.java
    ├── RNSResource.java
    ├── RNSAnnounceHandler.java
    ├── RNSRequestHandler.java
    ├── RNSLinkRequestReceipt.java
    └── impl/
        ├── ReticulumProvider.java   wire RNS.initialize() to the native stack
        ├── IdentityAdapter.java
        ├── DestinationAdapter.java
        ├── LinkAdapter.java
        ├── LinkAsDestinationAdapter.java
        ├── PacketAdapter.java
        ├── PacketReceiptAdapter.java
        ├── ResourceAdapter.java
        ├── RequestReceiptAdapter.java
        └── MsgPackHelper.java       Object ↔ byte[] msgpack codec
```


