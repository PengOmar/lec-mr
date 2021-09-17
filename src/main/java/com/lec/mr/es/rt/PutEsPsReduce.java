package com.lec.mr.es.rt;

import com.lec.RestHighLevelClient.BulkProcessorUtil;
import com.lec.RestHighLevelClient.MyRestHighLevelClient;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.orc.mapred.OrcStruct;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PutEsPsReduce extends Reducer<NullWritable, Text, NullWritable, NullWritable> {



    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        RestHighLevelClient restHighLevelClient = new MyRestHighLevelClient();
        Map<String, Object> jsonmap = new HashMap();
        BulkProcessorUtil bulkProcessorUtil = new BulkProcessorUtil(restHighLevelClient, "h_por_pscst_id_rtrvl");
        BulkProcessor request = bulkProcessorUtil.getBulkProcessor();
        for (Text value : values) {
            String[] split = value.toString().split("\\$");
            String ctftp_cd = split[0];
            String crtf_nbr = split[1];
            String org_no = split[2];
            String ecif_cust_no = split[3];
            String cust_name = split[4];


            String id = StringUtils.reverse(ecif_cust_no) + StringUtils.reverse(org_no);

            jsonmap.clear();
            jsonmap.put("ctftp_cd", ctftp_cd);
            jsonmap.put("crtf_nbr", crtf_nbr);
            jsonmap.put("org_no", org_no);
            jsonmap.put("ecif_cust_no", ecif_cust_no);
            jsonmap.put("cust_name", cust_name);

            request.add(new IndexRequest("h_por_pscst_id_rtrvl", "_doc", id).source(jsonmap));
        }
        bulkProcessorUtil.destroy();
        restHighLevelClient.close();
    }
}

