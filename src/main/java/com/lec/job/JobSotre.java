package com.lec.job;


import com.lec.mr.es.cp.PutEsCpReduce;
import com.lec.mr.es.cp.ReadHiveCpMapper;
import com.lec.mr.es.rt.PutEsPsReduce;
import com.lec.mr.es.rt.ReadHivePsMapper;
import com.lec.mr.index.IndexMapper;
import com.lec.mr.label.LabelMapper;
import com.lec.mr.profile.PreMapper;
import com.lec.mr.profile.PreReducer;
import com.lec.mr.profile.ProfileMapper;
import com.lec.mr.redis.cp.PutRedisCpReduce;
import com.lec.mr.redis.cp.ReadHCpMapper;
import com.lec.mr.redis.rt.PutRedisRtReduce;
import com.lec.mr.redis.rt.ReadHRtMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.orc.mapreduce.OrcInputFormat;

import java.io.IOException;

public class JobSotre {

    public Job creatIndexJob(Configuration conf, String inPath, Path outPath, Table table, RegionLocator regionLocator) throws IOException {
//        HbaseUtils.createHBaseTableByPartition("nsHBaseTableName", "columnName", 1, "flag");

        //准备工作
        FileSystem.get(conf).delete(outPath, true);

        // 构建job
        Job job = Job.getInstance(conf, "lec_index_cp_job".toLowerCase());
        job.setJarByClass(JobSotre.class);
        job.setMapperClass(IndexMapper.class);
        HFileOutputFormat2.setOutputPath(job, outPath);
        OrcInputFormat.addInputPaths(job, inPath);
        OrcInputFormat.setMinInputSplitSize(job, 64000000);
        OrcInputFormat.setMaxInputSplitSize(job, 64000000);
        job.setInputFormatClass(OrcInputFormat.class);
        HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
        return job;
    }


    public Job creatPreJob(Configuration conf, Table table, Path inPath, Path outPath) throws IOException {
        //准备工作
        FileSystem.get(conf).delete(outPath, true);

        Job job = Job.getInstance(conf, table.getName().toString() + "_job".toLowerCase());
        job.setJarByClass(JobSotre.class);
        job.setMapperClass(PreMapper.class);
        job.setReducerClass(PreReducer.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(Text.class);
        OrcInputFormat.setInputPaths(job, inPath);
        OrcInputFormat.setMinInputSplitSize(job, 64000000 / 2);
        OrcInputFormat.setMaxInputSplitSize(job, 64000000);
        TextOutputFormat.setOutputPath(job, outPath);
        TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(OrcInputFormat.class);

        return job;
    }
//

    public Job creatProfileJob(Configuration conf, Path inPath, Path outPath, Table table, RegionLocator regionLocator) throws IOException {
        //准备工作
        FileSystem.get(conf).delete(outPath, true);
        Job job = Job.getInstance(conf, table.getName().toString() + "_job".toLowerCase());
        TextInputFormat.setInputPaths(job, inPath);
        job.setInputFormatClass(TextInputFormat.class);
        job.setJarByClass(JobSotre.class);
        job.setMapperClass(ProfileMapper.class);
        HFileOutputFormat2.setOutputPath(job, outPath);
        HFileOutputFormat2.setOutputCompressorClass(job, GzipCodec.class);
        HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
        return job;
    }


    public Job creatLabelJob(Configuration conf, String inPath, Path outPath, Table table, RegionLocator regionLocator) throws IOException {

        //准备工作
        FileSystem.get(conf).delete(outPath, true);

        // 构建job
        Job job = Job.getInstance(conf, "lec_index_rt_job".toLowerCase());
        job.setJarByClass(JobSotre.class);
        job.setMapperClass(LabelMapper.class);
        HFileOutputFormat2.setOutputPath(job, outPath);
        OrcInputFormat.addInputPaths(job, inPath);
        OrcInputFormat.setMinInputSplitSize(job, 64000000);
        OrcInputFormat.setMaxInputSplitSize(job, 64000000);
        job.setInputFormatClass(OrcInputFormat.class);
        HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
        return job;
    }

    public Job creatBulkEsCpJob(Configuration conf, Path inPath, String outputEsIndex) throws IOException {
        Job job = Job.getInstance(conf, outputEsIndex);
        OrcInputFormat.setInputPaths(job, inPath);
        OrcInputFormat.setMinInputSplitSize(job, 64000000 / 2);
        OrcInputFormat.setMaxInputSplitSize(job, 64000000);
        job.setInputFormatClass(OrcInputFormat.class);
        job.setJarByClass(JobSotre.class);
        job.setMapperClass(ReadHiveCpMapper.class);
        job.setReducerClass(PutEsCpReduce.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        TextOutputFormat.setOutputPath(job, new Path("/user/lec/tmp" + outputEsIndex));
        job.setOutputFormatClass(TextOutputFormat.class);
        return job;
    }

    public Job creatBulkEsPsJob(Configuration conf, Path inPath, String outputEsIndex) throws IOException {
        Job job = Job.getInstance(conf, outputEsIndex);
        OrcInputFormat.setInputPaths(job, inPath);
        OrcInputFormat.setMinInputSplitSize(job, 64000000 / 2);
        OrcInputFormat.setMaxInputSplitSize(job, 64000000);
        job.setInputFormatClass(OrcInputFormat.class);
        job.setJarByClass(JobSotre.class);
        job.setMapperClass(ReadHivePsMapper.class);
        job.setReducerClass(PutEsPsReduce.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        TextOutputFormat.setOutputPath(job, new Path("/user/lec/tmp" + outputEsIndex));
        job.setOutputFormatClass(TextOutputFormat.class);
        return job;
    }

    public Job creatRedisCpJob(Configuration conf, Path inPath,String outputRedis) throws IOException {

        Job job = Job.getInstance(conf, outputRedis);
        OrcInputFormat.setInputPaths(job, inPath);
        OrcInputFormat.setMinInputSplitSize(job, 64000000 / 2);
        OrcInputFormat.setMaxInputSplitSize(job, 64000000);
        job.setInputFormatClass(OrcInputFormat.class);
        job.setJarByClass(JobSotre.class);
        job.setMapperClass(ReadHCpMapper.class);
        job.setReducerClass(PutRedisCpReduce.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        TextOutputFormat.setOutputPath(job, new Path("/user/lec/tmp" + outputRedis));
        job.setOutputFormatClass(TextOutputFormat.class);
        return job;
    }

    public Job creatRedisRtJob(Configuration conf, Path inPath,String outputRedis) throws IOException {

        Job job = Job.getInstance(conf, outputRedis);
        OrcInputFormat.setInputPaths(job, inPath);
        OrcInputFormat.setMinInputSplitSize(job, 64000000 / 2);
        OrcInputFormat.setMaxInputSplitSize(job, 64000000);
        job.setInputFormatClass(OrcInputFormat.class);
        job.setJarByClass(JobSotre.class);
        job.setMapperClass(ReadHRtMapper.class);
        job.setReducerClass(PutRedisRtReduce.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        TextOutputFormat.setOutputPath(job, new Path("/user/lec/tmp" + outputRedis));
        job.setOutputFormatClass(TextOutputFormat.class);
        return job;
    }

//    public Job creatHbaseCpJob(Configuration conf, String inPath, Path outPath, Table table, RegionLocator regionLocator) throws IOException {
//
//        //准备工作
//        FileSystem.get(conf).delete(outPath, true);
//
//        // 构建job
//        Job job = Job.getInstance(conf, "lec_index_cp_job".toLowerCase());
//        job.setJarByClass(JobSotre.class);
//        job.setMapperClass(IndexCpMapper.class);
//        HFileOutputFormat2.setOutputPath(job, outPath);
//        OrcInputFormat.addInputPaths(job, inPath);
//        OrcInputFormat.setMinInputSplitSize(job, 64000000);
//        OrcInputFormat.setMaxInputSplitSize(job, 64000000);
//        job.setInputFormatClass(OrcInputFormat.class);
//        HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
//        return job;
//    }
//    public Job creatHbaseRtJob(Configuration conf, String inPath, Path outPath, Table table, RegionLocator regionLocator) throws IOException {
//
//        //准备工作
//        FileSystem.get(conf).delete(outPath, true);
//
//        // 构建job
//        Job job = Job.getInstance(conf, "lec_index_cp_job".toLowerCase());
//        job.setJarByClass(JobSotre.class);
//        job.setMapperClass(IndexRtMapper.class);
//        HFileOutputFormat2.setOutputPath(job, outPath);
//        OrcInputFormat.addInputPaths(job, inPath);
//        OrcInputFormat.setMinInputSplitSize(job, 64000000);
//        OrcInputFormat.setMaxInputSplitSize(job, 64000000);
//        job.setInputFormatClass(OrcInputFormat.class);
//        HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
//        return job;
//    }
}
