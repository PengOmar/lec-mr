package com.lec.mr.es.rt;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;

public class ReadHivePsMapper extends Mapper<NullWritable, OrcStruct, NullWritable, Text> {
    @Override
    protected void map(NullWritable keyIn, OrcStruct valueIn, Context context) throws IOException, InterruptedException {
        Text text = new Text();
        Text ctftp_cd = (Text) valueIn.getFieldValue(0);
        Text crtf_nbr = (Text) valueIn.getFieldValue(1);
        Text org_no = (Text) valueIn.getFieldValue(2);
        Text ecif_cust_no = (Text) valueIn.getFieldValue(3);
        Text cust_name = (Text) valueIn.getFieldValue(4);


        text.set(ctftp_cd.toString() + "$" + crtf_nbr.toString() + "$" + org_no.toString() + "$" + ecif_cust_no.toString() + "$" + cust_name.toString());

        context.write(keyIn, text);
    }
}
