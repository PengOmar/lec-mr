package com.lec.driver;

import com.lec.common.utils.HbaseUtils;
import com.lec.common.utils.KerberosLoginUtil;
import com.lec.job.JobSotre;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LabelDriver {
    private static final Logger LOG = LoggerFactory.getLogger(LabelDriver.class);

    public static void main(String[] args) throws Exception {
        // 输入路径
        String inPath = args[0];
        // Hfile存储路径
        Path outPath = new Path(args[1]);

//        HbaseUtils.createHBaseTableByPartition("nsHBaseTableName", "columnName", 1, "flag");
        // 初始化配置
        Configuration conf = HBaseConfiguration.create();
        conf.addResource("core-site.xml");
        conf.addResource("hbase-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.addResource("hivemetastore-site.xml");
        conf.addResource("hive-site.xml");
        conf.addResource("mapred-site.xml");
        conf.addResource("yarn-site.xml");
        KerberosLoginUtil.login("etl", "etl.keytab", "krb5.conf", conf);
        FileSystem.get(conf).delete(outPath, true);
        Connection connection = ConnectionFactory.createConnection(conf);
        HbaseUtils hbaseUtils = new HbaseUtils(connection);
        hbaseUtils.createHBaseTableByPartition(args[2], "c", 1000, "true");
        TableName tableNameObj = TableName.valueOf(args[2]);
        Table table = connection.getTable(tableNameObj);
        RegionLocator regionLocator = connection.getRegionLocator(tableNameObj);
        Admin admin = connection.getAdmin();


        // 构建job
        LOG.info("\n---------------start MapReduceJob----------------\n");
        long startTime = System.currentTimeMillis();
        Job job = new JobSotre().creatLabelJob(conf, inPath, outPath, table, regionLocator);


        //构建子任务
        ControlledJob firstJob = new ControlledJob(conf);
        firstJob.setJob(job);


        //主的控制容器，控制上面的总的两个子作业
        JobControl jobCtrl = new JobControl("lab");
        jobCtrl.addJob(firstJob);


        //在线程启动
        Thread t = new Thread(jobCtrl);
        t.start();
        while (true) {
            if (jobCtrl.allFinished()) {
                jobCtrl.stop();
                break;
            }
        }
        LOG.info("\n---------------End MapReduceJob----------------\n");
        long endTime = System.currentTimeMillis();
        LOG.info("-----------------spendTime==========>>>>>>" + ((endTime - startTime) / 1000) + "s");


        //BulkLoad任务
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        long bulkTime = System.currentTimeMillis();
        LOG.info("\n---------------start BulkLoad---------------\n");

        loader.doBulkLoad(outPath, admin, table, regionLocator);
        LOG.info("\n---------------End BulkLoad---------------\n");
        long bulkEndTime = System.currentTimeMillis();
        LOG.info("-----------------spendTime==========>>>>>>" + ((bulkEndTime - bulkTime) / 1000) + "s");


        //收尾工作
        FileSystem.get(conf).delete(outPath, true);
        System.exit(0);
    }
}
