package com.yankee.example;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/11/25 13:27
 */
public class UserSchema implements KafkaDeserializationSchema<User> {
    private final static Logger LOG = LoggerFactory.getLogger(UserSchema.class);

    @Override
    public boolean isEndOfStream(User user) {
        return false;
    }

    @Override
    public User deserialize(ConsumerRecord<byte[], byte[]> record) {
        User user = null;
        if (record != null) {
            try {
                String value = new String(record.value(), "UTF-8");
                user = JSON.parseObject(value, User.class);
            } catch (Exception e) {
                e.printStackTrace();
                LOG.error("Deserialize failed : " + e.getMessage());
            }
        }
        return user;
    }

    @Override
    public TypeInformation<User> getProducedType() {
        return TypeInformation.of(new TypeHint<User>() {
            @Override
            public TypeInformation<User> getTypeInfo() {
                return super.getTypeInfo();
            }
        });
    }
}
