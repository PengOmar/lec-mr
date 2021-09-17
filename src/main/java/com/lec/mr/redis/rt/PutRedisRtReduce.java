package com.lec.mr.redis.rt;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.IOException;

public class PutRedisRtReduce extends Reducer<NullWritable, Text, NullWritable, NullWritable> {
    private Jedis jedis;

    public PutRedisRtReduce() {
        jedis = new Jedis("9.1.10.228", 6379, 1000);
        jedis.auth("Primeton@123");
        jedis.select(8);
        jedis.flushDB();
    }

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Pipeline pipe = jedis.pipelined();

        for (Text value : values) {
            String[] strings = value.toString().split("\\$");
            String ecif_cust_no = strings[0];
            String name = strings[1];
            pipe.set("lec:" + ecif_cust_no, name);
        }
        pipe.sync();
        jedis.close();
    }
}
