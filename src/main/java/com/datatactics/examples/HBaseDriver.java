package com.datatactics.examples;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class HBaseDriver extends Configured implements Tool
{
    private static final Logger log = Logger.getLogger(HBaseDriver.class);

    @Override
    public int run(String[] args) throws Exception
    {
        // Calendar/Time to be used in a few places
        final Calendar cal = Calendar.getInstance();
        final SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MMM-yyyy HH:mm");

        // Construct the Job name, using Date to help easily find in JT
        StringBuilder jobName = new StringBuilder("Image Size Update: ");
        jobName.append(dateFormat.format(cal.getTime()));

        Configuration conf = getConf();
        log.info("zookeepers = " + conf.get("hbase.zookeeper.quorum"));
        log.info("rootdir = " + conf.get("hbase.rootdir"));
        
        Job job = Job.getInstance(conf, jobName.toString());
        job.setJarByClass(this.getClass());

        Scan scan = new Scan();
        scan.addFamily("image".getBytes());
        scan.setCacheBlocks(false);

        // Init the input/output formats
        TableMapReduceUtil.initTableMapperJob("aglahe-roxyimages", scan, 
        		HBaseMapper.class, ImmutableBytesWritable.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob("aglahe-roxyimages", HBaseReducer.class, job);
        
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception
    {
        int exitCode = ToolRunner.run(HBaseConfiguration.create(new Configuration()), new HBaseDriver(), args);
        System.exit(exitCode);
    }
}
