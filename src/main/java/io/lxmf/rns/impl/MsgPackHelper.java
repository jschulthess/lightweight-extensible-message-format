package io.lxmf.rns.impl;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Bridges the LXMF Object-typed request/response API to the native Reticulum byte[]-typed API.
 *
 * <p>The native Link.request(path, byte[] data, ...) / responseGenerator: Function&lt;Request, byte[]&gt;
 * work with raw bytes.  LXMF's RNSLink.request(path, Object data, ...) and RNSRequestHandler
 * work with msgpack-serialisable Java objects.  This class encodes and decodes the boundary.
 */
final class MsgPackHelper {

    private MsgPackHelper() {}

    /** Msgpack-encode an arbitrary Java object to bytes. */
    static byte[] pack(Object obj) throws IOException {
        try (MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
            packValue(packer, obj);
            return packer.toByteArray();
        }
    }

    /** Msgpack-decode bytes to a Java object (null/Boolean/Integer/Long/Double/byte[]/List/Map). */
    static Object unpack(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) return null;
        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes)) {
            return unpackValue(unpacker);
        }
    }

    private static void packValue(MessageBufferPacker p, Object obj) throws IOException {
        if (obj == null) {
            p.packNil();
        } else if (obj instanceof Boolean) {
            p.packBoolean((Boolean) obj);
        } else if (obj instanceof Integer) {
            p.packInt((Integer) obj);
        } else if (obj instanceof Long) {
            p.packLong((Long) obj);
        } else if (obj instanceof Double) {
            p.packDouble((Double) obj);
        } else if (obj instanceof Float) {
            p.packFloat((Float) obj);
        } else if (obj instanceof byte[]) {
            byte[] b = (byte[]) obj;
            p.packBinaryHeader(b.length);
            p.writePayload(b);
        } else if (obj instanceof String) {
            p.packString((String) obj);
        } else if (obj instanceof Object[]) {
            Object[] arr = (Object[]) obj;
            p.packArrayHeader(arr.length);
            for (Object o : arr) packValue(p, o);
        } else if (obj instanceof List) {
            List<?> list = (List<?>) obj;
            p.packArrayHeader(list.size());
            for (Object o : list) packValue(p, o);
        } else if (obj instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) obj;
            p.packMapHeader(map.size());
            for (Map.Entry<?, ?> e : map.entrySet()) {
                packValue(p, e.getKey());
                packValue(p, e.getValue());
            }
        } else {
            throw new IllegalArgumentException("Cannot msgpack-encode " + obj.getClass().getName());
        }
    }

    private static Object unpackValue(MessageUnpacker u) throws IOException {
        MessageFormat fmt = u.getNextFormat();
        switch (fmt.getValueType()) {
            case NIL:
                u.unpackNil();
                return null;
            case BOOLEAN:
                return u.unpackBoolean();
            case INTEGER: {
                long l = u.unpackLong();
                return (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) ? (int) l : l;
            }
            case FLOAT:
                return u.unpackDouble();
            case BINARY: {
                int len = u.unpackBinaryHeader();
                return u.readPayload(len);
            }
            case STRING:
                return u.unpackString();
            case ARRAY: {
                int size = u.unpackArrayHeader();
                List<Object> list = new ArrayList<>(size);
                for (int i = 0; i < size; i++) list.add(unpackValue(u));
                return list;
            }
            case MAP: {
                int size = u.unpackMapHeader();
                Map<Object, Object> map = new LinkedHashMap<>(size * 2);
                for (int i = 0; i < size; i++) {
                    Object k = unpackValue(u);
                    Object v = unpackValue(u);
                    map.put(k, v);
                }
                return map;
            }
            default:
                throw new IOException("Unsupported msgpack value type: " + fmt);
        }
    }
}
