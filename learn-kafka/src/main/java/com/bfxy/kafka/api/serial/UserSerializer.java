package com.bfxy.kafka.api.serial;

import com.bfxy.kafka.api.User;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @Author: zhaojh
 * @Data: 2020/9/16 14:56
 * @Desc:
 */
public class UserSerializer implements Serializer<User> {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @SneakyThrows
    @Override
    public byte[] serialize(String topic, User user) {
        if (user == null) {
            return  null;
        }
        byte[] idBytes, nameBytes;

        try {
            String id = user.getId();
            if (id != null) {
                idBytes = id.getBytes("utf-8");
            }else {
                idBytes = new byte[0];
            }
            String name = user.getName();
            if (name != null) {
                nameBytes = name.getBytes("utf-8");
            }else {
                nameBytes = new byte[0];
            }
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + idBytes.length + nameBytes.length);
            buffer.putInt(idBytes.length);
            buffer.put(idBytes);
            buffer.putInt(nameBytes.length);
            buffer.put(nameBytes);
            return buffer.array();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
