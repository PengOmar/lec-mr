package com.lec.mr.profile;


import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 用户画像Mapper
 */
public class ProfileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

    @Override
    protected void map(LongWritable key, Text valueIn, Context context)
            throws IOException, InterruptedException {
        String mapkey = valueIn.toString().split("\t")[0];
        String value = valueIn.toString().split("\t")[1];
        String lbl_no = mapkey.split("\\$")[0];
        String org_ecd = mapkey.split("\\$")[1];
        byte[] family = "c".getBytes();

        ImmutableBytesWritable keyout = new ImmutableBytesWritable(Bytes.toBytes(lbl_no));
        KeyValue valueOut = new KeyValue(lbl_no.getBytes(), family, org_ecd.getBytes(), value.getBytes());
        context.write(keyout, valueOut);

    }
}