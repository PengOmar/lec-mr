package com.lec.driver;

import com.lec.common.utils.HbaseUtils;
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
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProfileDriver {
    private static final Logger LOG = LoggerFactory.getLogger(ProfileDriver.class);

    public static void main(String[] args) throws Exception {
        // 输入路径
        Path inPath = new Path(args[0]);

        //中间输出路径
        Path midPath = new Path(args[1]);

        // Hfile存储路径
        Path outPath = new Path(args[2]);


        // 初始化配置
        Configuration conf = HBaseConfiguration.create();
        conf.addResource("core-site.xml");
        conf.addResource("hbase-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.addResource("hivemetastore-site.xml");
        conf.addResource("hive-site.xml");
        conf.addResource("mapred-site.xml");
        conf.addResource("yarn-site.xml");
        System.setProperty("java.security.krb5.conf", "krb5.conf");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("etl", "etl.keytab");
        FileSystem.get(conf).delete(midPath, true);
        FileSystem.get(conf).delete(outPath, true);
        Connection connection = ConnectionFactory.createConnection(conf);
        HbaseUtils hbaseUtils = new HbaseUtils(connection);
        hbaseUtils.createHBaseTableByPartition(args[3], "c", 1000, "true");


        TableName tableNameObj = TableName.valueOf(args[3]);
        Table table = connection.getTable(tableNameObj);
        RegionLocator regionLocator = connection.getRegionLocator(tableNameObj);
        Admin admin = connection.getAdmin();
        JobSotre jobSotre = new JobSotre();

        //开始Pre
        LOG.info("\n---------------start PreJob----------------\n");
        long startTime = System.currentTimeMillis();
        Job preJob = jobSotre.creatPreJob(conf, table, inPath, midPath);
        preJob.waitForCompletion(true);
        LOG.info("\n---------------End PreJob----------------\n");
        long endTime = System.currentTimeMillis();
        LOG.info("-----------------spendTime==========>>>>>>" + ((endTime - startTime) / 1000) + "s");


        //开始Profile
        LOG.info("\n---------------start ProfileJob----------------\n");
        long startProfile = System.currentTimeMillis();
        Job profileJob = jobSotre.creatProfileJob(conf, midPath, outPath, table, regionLocator);
        profileJob.waitForCompletion(true);
        LOG.info("\n---------------End ProfileJob----------------\n");
        long endProfile = System.currentTimeMillis();
        LOG.info("-----------------spendTime==========>>>>>>" + ((endProfile - startProfile) / 1000) + "s");


        //BulkLoad任务
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        long bulkTime = System.currentTimeMillis();
        LOG.info("\n---------------start BulkLoad---------------\n");
        loader.doBulkLoad(outPath, admin, table, regionLocator);
        LOG.info("\n---------------End BulkLoad---------------\n");
        long bulkEndTime = System.currentTimeMillis();
        LOG.info("-----------------spendTime==========>>>>>>" + ((bulkEndTime - bulkTime) / 1000) + "s");


        //收尾工作
        FileSystem.get(conf).delete(midPath, true);
        FileSystem.get(conf).delete(outPath, true);
        System.exit(0);
    }
}

// hadoop jar Hive2Hbase-1.0-SNAPSHOT.jar com.lec.driver.ProfileDriver /user/hive/warehouse/qry.db/demo3 /user/etl/midTemp/Profile /user/etl/HFile/Profile lec:LBL_RSLT