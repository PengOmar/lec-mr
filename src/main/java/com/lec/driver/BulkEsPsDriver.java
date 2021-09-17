package com.lec.driver;

import com.lec.RestHighLevelClient.BulkProcessorUtil;
import com.lec.RestHighLevelClient.MyRestHighLevelClient;
import com.lec.common.utils.KerberosLoginUtil;
import com.lec.job.JobSotre;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class BulkEsPsDriver {
    private static final Logger LOG = LoggerFactory.getLogger(BulkEsPsDriver.class);

    public static void main(String[] args) {
        MyRestHighLevelClient highLevelClient = new MyRestHighLevelClient();


        Configuration conf = HBaseConfiguration.create();
        conf.addResource("core-site.xml");
        conf.addResource("hbase-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.addResource("hivemetastore-site.xml");
        conf.addResource("hive-site.xml");
        conf.addResource("mapred-site.xml");
        conf.addResource("yarn-site.xml");
        try {
            KerberosLoginUtil.login("etl", "etl.keytab", "krb5.conf", conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Path path = new Path(args[0]);
        String outputEsIndex = "h_por_pscst_id_rtrvl";
        try {
            FileSystem.get(conf).delete(new Path("/user/lec/tmp" + outputEsIndex), true);
            BulkProcessorUtil bulkProcessorUtil = new BulkProcessorUtil(highLevelClient, outputEsIndex);
            if (bulkProcessorUtil.isExistIndexForHighLevel()) {
                bulkProcessorUtil.deleteIndex();
                bulkProcessorUtil.createPsIndex();
            } else {
                bulkProcessorUtil.createPsIndex();
            }

            // 构建job
            LOG.info("\n---------------start MapReduceJob----------------\n");
            long startTime = System.currentTimeMillis();
            Job job = new JobSotre().creatBulkEsPsJob(conf, path, outputEsIndex);
            boolean res = job.waitForCompletion(true);
            LOG.info("\n---------------MapReduceJobEnd---------------\n");
            long endTime = System.currentTimeMillis();
            LOG.info("=================spendTime==========>>>>>>" + ((endTime - startTime) / 1000) + "s");
            System.exit(res ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}