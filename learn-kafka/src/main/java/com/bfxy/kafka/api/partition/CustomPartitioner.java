package com.bfxy.kafka.api.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: zhaojh
 * @Data: 2020/9/16 16:16
 * @Desc:
 */
public class CustomPartitioner implements Partitioner {

    private AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitionList = cluster.partitionsForTopic(topic);
        int numPartitions = partitionList.size();
        System.out.println("---进入自定义分区器，当前分区数量为：" + numPartitions);
        if (keyBytes == null) {
            return counter.getAndIncrement() % numPartitions;
        } else {
            return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
