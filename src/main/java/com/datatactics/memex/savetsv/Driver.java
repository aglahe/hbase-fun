package com.datatactics.memex.savetsv;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class Driver extends Configured implements Tool {
	private static final Logger log = Logger.getLogger(Driver.class);

	public static final byte[] META_CF = "meta".getBytes();
	
	@Override
	public int run(String[] args) throws Exception {
		// Calendar/Time to be used in a few places
		final Calendar cal = Calendar.getInstance();
		final SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MMM-yyyy HH:mm");

		// Construct the Job name, using Date to help easily find in JT
		StringBuilder jobName = new StringBuilder("Image Importer: ");
		jobName.append(dateFormat.format(cal.getTime()));

		// Path to save the S3 Images
		Path dataPath = new Path(args[0]);

		Configuration conf = getConf();
		log.info("zookeepers = " + conf.get("hbase.zookeeper.quorum"));
		log.info("rootdir = " + conf.get("hbase.rootdir"));

		Job job = Job.getInstance(conf, jobName.toString());
		job.setJarByClass(this.getClass());

		// Setup the Mapper
		Scan scan = new Scan();
		scan.addFamily(META_CF);
		scan.setCacheBlocks(false);
		
		// Limit our scan for testing purposes
		if (StringUtils.isNotBlank(conf.get("hbase.startRow"))) {
			log.info("Setting start limit on the scan:" + conf.get("hbase.startRow"));
			scan.setStartRow(conf.get("hbase.startRow").getBytes());
		}
		if (StringUtils.isNotBlank(conf.get("hbase.stopRow"))) {
			log.info("Setting end limit on the scan:" + conf.get("hbase.stopRow"));
			scan.setStopRow(conf.get("hbase.stopRow").getBytes());
		}
		
		// Input
		TableMapReduceUtil.initTableMapperJob(conf.get("hbase.table.name"), scan, HtImageHBaseLocationMapper.class,
				Text.class, Text.class, job);

		// Output
        TextOutputFormat.setOutputPath(job, dataPath);
		job.setReducerClass(HtImageTSVReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set the standard reduce tasks
		job.setNumReduceTasks(Integer.parseInt(conf.get("mapred.reduce.tasks", "20")));
		
		// / Submit the Job
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(new Configuration()),
				new Driver(), args);
		System.exit(exitCode);
	}
}
