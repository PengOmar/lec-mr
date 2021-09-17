package com.lec.mr.index;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.mapred.OrcStruct;
import java.io.IOException;

public class IndexMapper extends Mapper<NullWritable, OrcStruct, ImmutableBytesWritable, KeyValue> {

    private static Configuration conf = null;

    @Override
    protected void setup(Context context) {
        conf = context.getConfiguration();
    }

    @Override
    protected void map(NullWritable keyIn, OrcStruct valueIn, Context context)
            throws IOException, InterruptedException {

        Text idx_ecd = (Text) valueIn.getFieldValue(1);
        Text ecif_cust_no = (Text) valueIn.getFieldValue(2);
        Text org_ecd = (Text) valueIn.getFieldValue(3);
        Text idx_val = (Text) valueIn.getFieldValue(4);

        String rowkey = StringUtils.reverse(ecif_cust_no.toString()) + StringUtils.reverse(org_ecd.toString());
        byte[] family = "c".getBytes();
        byte[] rowkeyBytes = new StringBuilder(rowkey.substring(0, 10)).reverse().toString().getBytes();
        ImmutableBytesWritable keyout = new ImmutableBytesWritable(rowkeyBytes);
        KeyValue kv = new KeyValue(rowkeyBytes, family, idx_ecd.getBytes(), idx_val.getBytes());
        context.write(keyout, kv);
    }
}