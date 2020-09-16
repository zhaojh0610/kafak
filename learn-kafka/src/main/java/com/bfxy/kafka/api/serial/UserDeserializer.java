package com.bfxy.kafka.api.serial;

import com.bfxy.kafka.api.User;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @Author: zhaojh
 * @Data: 2020/9/16 15:25
 * @Desc:
 */
public class UserDeserializer implements Deserializer<User> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public User deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        if (data.length < 8) {
            throw new SerializationException("size is wrong, data.length must more than 8 ");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int idLength = buffer.getInt();
        byte[] idByte = new byte[idLength];
        buffer.get(idByte);
        int nameLength = buffer.getInt();
        byte[] nameByte = new byte[nameLength];
        buffer.get(nameByte);
        String id, name;
        try {
            id = new String(idByte, "utf-8");
            name = new String(nameByte, "utf-8");
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("deserializer error", e);
        }
        return new User(id, name);
    }

    @Override
    public void close() {

    }
}
