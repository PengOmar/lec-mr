package com.lec.mr.profile;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;

/**
 * 用户画像前置Mapper
 */
public class PreMapper extends Mapper<NullWritable, OrcStruct, Text, LongWritable> {

    @Override
    protected void map(NullWritable keyIn, OrcStruct valueIn, Context context) throws IOException, InterruptedException {
        Text keyOut = new Text();
        Text lbl_no = (Text) valueIn.getFieldValue(1);
        Text org_ecd = (Text) valueIn.getFieldValue(3);
        Text ecif_cust_no = (Text) valueIn.getFieldValue(4);
        keyOut.set(lbl_no.toString() + "$" + org_ecd.toString());
        try {
            LongWritable vauleOut = new LongWritable();
            vauleOut.set(Long.parseLong(ecif_cust_no.toString()));
            context.write(keyOut, vauleOut);
        } catch (NumberFormatException e) {

        }


    }
}