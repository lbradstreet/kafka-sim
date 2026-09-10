// The fixture capture oracle uses Apache Kafka's generated Java Message classes.
// Run through kafka-java-fixtures.py; this file has no serialization implementation.
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.record.internal.MemoryRecords;

class KafkaJavaFixtures {
    static Map<String, Object> map(Object... entries) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < entries.length; i += 2) {
            result.put((String) entries[i], entries[i + 1]);
        }
        return result;
    }

    static List<Object> list(Object... entries) {
        return java.util.Arrays.asList(entries);
    }

    @SuppressWarnings("unchecked")
    static Object convert(Object value, Class<?> type, Type genericType) throws Exception {
        if (value == null) return null;
        if (type == byte.class) {
            long number = ((Number) value).longValue();
            if (number < Byte.MIN_VALUE || number > Byte.MAX_VALUE) throw new IllegalArgumentException("byte field range");
            return (byte) number;
        }
        if (type == short.class) return ((Number) value).shortValue();
        if (type == int.class) return ((Number) value).intValue();
        if (type == long.class) return ((Number) value).longValue();
        if (type == Uuid.class) {
            UUID uuid = UUID.fromString((String) value);
            return new Uuid(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        }
        if (type == byte[].class || type == ByteBuffer.class ||
                org.apache.kafka.common.record.internal.BaseRecords.class.isAssignableFrom(type)) {
            byte[] bytes = HexFormat.of().parseHex((String) ((Map<?, ?>) value).get("hex"));
            if (type == byte[].class) return bytes;
            if (type == ByteBuffer.class) return ByteBuffer.wrap(bytes);
            return MemoryRecords.readableRecords(ByteBuffer.wrap(bytes));
        }
        if (Collection.class.isAssignableFrom(type)) {
            Collection<Object> result;
            Type element;
            if (type == List.class) {
                result = new ArrayList<>();
                element = ((ParameterizedType) genericType).getActualTypeArguments()[0];
            } else {
                result = (Collection<Object>) type.getConstructor().newInstance();
                element = ((ParameterizedType) type.getGenericSuperclass()).getActualTypeArguments()[0];
            }
            for (Object item : (List<?>) value) result.add(convert(item, (Class<?>) element, element));
            return result;
        }
        if (Message.class.isAssignableFrom(type)) {
            return populate((Message) type.getConstructor().newInstance(), (Map<String, Object>) value);
        }
        if (type == Integer.class) return ((Number) value).intValue();
        return value;
    }

    static Message populate(Message message, Map<String, Object> fields) throws Exception {
        for (var entry : fields.entrySet()) {
            if (entry.getKey().equals("_unknownTaggedFields")) {
                for (Object value : (List<?>) entry.getValue()) {
                    Map<?, ?> tag = (Map<?, ?>) value;
                    message.unknownTaggedFields().add(new RawTaggedField(
                        ((Number) tag.get("tag")).intValue(),
                        HexFormat.of().parseHex((String) tag.get("dataHex"))));
                }
                continue;
            }
            Method setter = null;
            for (Method method : message.getClass().getMethods()) {
                if (method.getName().equals("set" + entry.getKey()) && method.getParameterCount() == 1) {
                    setter = method;
                    break;
                }
            }
            if (setter == null) throw new IllegalArgumentException("No setter for " + entry.getKey());
            setter.invoke(message, convert(entry.getValue(), setter.getParameterTypes()[0],
                setter.getGenericParameterTypes()[0]));
        }
        return message;
    }

    static byte[] encode(Message message, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        int expectedSize = message.size(cache, version);
        ByteBuffer buffer = ByteBuffer.allocate(expectedSize);
        message.write(new ByteBufferAccessor(buffer), cache, version);
        if (buffer.position() != expectedSize) throw new AssertionError("Java size/write disagreement");
        return buffer.array();
    }

    static void capture(String name, String messageName, int version, Map<String, Object> fields)
            throws Exception {
        Class<?> type = Class.forName("org.apache.kafka.common.message." + messageName + "Data");
        Message original = populate((Message) type.getConstructor().newInstance(), fields);
        byte[] bytes = encode(original, (short) version);
        Message decoded = (Message) type.getConstructor().newInstance();
        ByteBuffer input = ByteBuffer.wrap(bytes);
        decoded.read(new ByteBufferAccessor(input), (short) version);
        if (input.hasRemaining()) throw new AssertionError("Java read left trailing bytes: " + name);
        if (!java.util.Arrays.equals(bytes, encode(decoded, (short) version))) {
            throw new AssertionError("Java decode/write disagreement: " + name);
        }
        System.out.println(name + "\t" + HexFormat.of().formatHex(bytes));
    }
}
