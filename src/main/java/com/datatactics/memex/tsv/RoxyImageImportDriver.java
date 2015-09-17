package com.datatactics.memex.tsv;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class RoxyImageImportDriver extends Configured implements Tool {
	private static final Logger log = Logger.getLogger(RoxyImageImportDriver.class);

	public static final byte[] META_CF = "meta".getBytes();
	
	@Override
	public int run(String[] args) throws Exception {
		// Calendar/Time to be used in a few places
		final Calendar cal = Calendar.getInstance();
		final SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MMM-yyyy HH:mm");

		// Construct the Job name, using Date to help easily find in JT
		StringBuilder jobName = new StringBuilder("Image Importer: ");
		jobName.append(dateFormat.format(cal.getTime()));

		// Path to the Image S3 locations
		Path dataPath = new Path(args[0]);

		Configuration conf = getConf();
		log.info("zookeepers = " + conf.get("hbase.zookeeper.quorum"));
		log.info("rootdir = " + conf.get("hbase.rootdir"));

		Job job = Job.getInstance(conf, jobName.toString());
		job.setJarByClass(this.getClass());

		// Setup the InputFormat
//		TextInputFormat.addInputPath(job, dataPath);
//		job.setInputFormatClass(TextInputFormat.class);

		// Setup the Mapper
//		MultithreadedMapper.setMapperClass(job, HtImageFileMapper.class);
//		MultithreadedMapper.setNumberOfThreads(job,
//				Integer.parseInt(conf.get("mapred.map.multithreadedrunner.threads", "10")));
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(HtImageWritable.class);
//		job.setMapperClass(MultithreadedMapper.class);

		// Setup the Mapper
		Scan scan = new Scan();
		scan.addFamily(META_CF);
		scan.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob(conf.get("hbase.table.name"), scan, HtImageHBaseMapper.class,
				ImmutableBytesWritable.class, BytesWritable.class, job);

		// Setup the Reducer
		TableMapReduceUtil.initTableReducerJob(conf.get("hbase.table.name"), HtImageHBaseReducer.class, job);

		// Set the standard reduce tasks
		job.setNumReduceTasks(Integer.parseInt(conf.get("mapred.reduce.tasks", "20")));

		// / Submit the Job
		job.submit();
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(new Configuration()),
				new RoxyImageImportDriver(), args);
		System.exit(exitCode);
	}
}
