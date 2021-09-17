package com.lec.mr.redis.rt;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;

public class ReadHRtMapper extends Mapper<NullWritable, OrcStruct, NullWritable, Text> {
    @Override
    protected void map(NullWritable keyIn, OrcStruct valueIn, Context context) throws IOException, InterruptedException {
        Text text = new Text();
        String ecif_cust_no = valueIn.getFieldValue(3).toString();
        String name = valueIn.getFieldValue(4).toString();
        text.set(ecif_cust_no + "$" + name);
        context.write(keyIn, text);
    }
}
