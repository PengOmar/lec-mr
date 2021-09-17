package com.lec.mr.redis.cp;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.IOException;

public class PutRedisCpReduce extends Reducer<NullWritable, Text, NullWritable, NullWritable> {
    private Jedis jedis;

    public PutRedisCpReduce() {
        jedis = new Jedis("9.1.10.228", 6379, 1000);
        jedis.auth("Primeton@123");
        jedis.select(7);
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
