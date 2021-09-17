package com.lec.mr.es.cp;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;

public class ReadHiveCpMapper extends Mapper<NullWritable, OrcStruct, NullWritable, Text> {
    @Override
    protected void map(NullWritable keyIn, OrcStruct valueIn, Context context) throws IOException, InterruptedException {
        Text text = new Text();
        Text uscc = (Text) valueIn.getFieldValue(0);
        Text org_no = (Text) valueIn.getFieldValue(1);
        Text ecif_cust_no = (Text) valueIn.getFieldValue(2);
        Text cust_name = (Text) valueIn.getFieldValue(3);

        text.set(uscc.toString() + "$" + org_no.toString() + "$" + ecif_cust_no.toString() + "$" + cust_name.toString());

        context.write(keyIn, text);
    }
}
