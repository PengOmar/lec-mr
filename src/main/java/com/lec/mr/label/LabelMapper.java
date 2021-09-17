package com.lec.mr.label;



import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;

/**
 * 标签Mapper
 */
public class LabelMapper extends Mapper<NullWritable, OrcStruct, ImmutableBytesWritable, KeyValue> {

    private static Configuration conf = null;

    @Override
    protected void setup(Context context) {
        conf = context.getConfiguration();
    }

    @Override
    protected void map(NullWritable keyIn, OrcStruct valueIn, Context context)
            throws IOException, InterruptedException {

        Text lbl_no = (Text) valueIn.getFieldValue(1);
        Text lbl_val = (Text) valueIn.getFieldValue(2);
        Text org_ecd = (Text) valueIn.getFieldValue(3);
        Text ecif_cust_no = (Text) valueIn.getFieldValue(4);

        String rowkey = StringUtils.reverse(ecif_cust_no.toString()) + StringUtils.reverse(org_ecd.toString());
        byte[] family = "c".getBytes();
        byte[] rowkeyBytes = new StringBuilder(rowkey.substring(0, 10)).reverse().toString().getBytes();
        ImmutableBytesWritable keyout = new ImmutableBytesWritable(rowkeyBytes);
        KeyValue kv = new KeyValue(rowkeyBytes, family, lbl_no.getBytes(), lbl_val.getBytes());
        context.write(keyout, kv);
    }
}
