package com.lec.mr.profile;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Base64;

/**
 * 用户画像前置Reducer
 */
public class PreReducer extends Reducer<Text, LongWritable, Text, Text> {

//    private TypeDescription schema = TypeDescription.fromString("struct<value:string>");
//    private OrcStruct pair = (OrcStruct) OrcStruct.createValue(schema);

//    private final NullWritable nw = NullWritable.get();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Roaring64NavigableMap r1 = new Roaring64NavigableMap();

        for (LongWritable value : values) {
            r1.add(value.get());
        }

        Text valueOut = new Text();
        valueOut.set(this.serializeToStr(r1));
        context.write(key, valueOut);

    }


    public String serializeToStr(Roaring64NavigableMap bitmap) throws IOException {
        bitmap.runOptimize();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        bitmap.serialize(new DataOutputStream(outputStream));
        return Base64.getEncoder().encodeToString(outputStream.toByteArray());
    }


}
