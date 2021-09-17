package com.lec.mr.es.cp;

import com.lec.RestHighLevelClient.BulkProcessorUtil;
import com.lec.RestHighLevelClient.MyRestHighLevelClient;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PutEsCpReduce extends Reducer<NullWritable, Text, NullWritable, NullWritable> {


    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        RestHighLevelClient restHighLevelClient = new MyRestHighLevelClient();
        Map<String, Object> jsonmap = new HashMap();
        BulkProcessorUtil bulkProcessorUtil = new BulkProcessorUtil(restHighLevelClient, "h_por_cpcst_id_rtrvl");
        BulkProcessor request = bulkProcessorUtil.getBulkProcessor();
        for (Text value : values) {
            String[] split = value.toString().split("\\$");
            String uscc = split[0];
            String org_no = split[1];
            String ecif_cust_no = split[2];
            String cust_name = split[3];

            String id = StringUtils.reverse(ecif_cust_no) + StringUtils.reverse(org_no);

            jsonmap.clear();
            jsonmap.put("uscc", uscc);
            jsonmap.put("org_no", org_no);
            jsonmap.put("ecif_cust_no", ecif_cust_no);
            jsonmap.put("cust_name", cust_name);


            request.add(new IndexRequest("h_por_cpcst_id_rtrvl", "_doc", id).source(jsonmap));
        }
        bulkProcessorUtil.destroy();
        restHighLevelClient.close();
    }
}
