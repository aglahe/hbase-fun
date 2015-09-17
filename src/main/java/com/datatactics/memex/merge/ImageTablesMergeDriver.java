package com.datatactics.memex.merge;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.datatactics.memex.util.ImagesColumnNames;

public class ImageTablesMergeDriver extends Configured implements Tool {
	private static final Logger log = Logger.getLogger(ImageTablesMergeDriver.class);

	
	@Override
	public int run(String[] args) throws Exception {
		// Calendar/Time to be used in a few places
		final Calendar cal = Calendar.getInstance();
		final SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MMM-yyyy HH:mm");

		// Construct the Job name, using Date to help easily find in JT
		StringBuilder jobName = new StringBuilder("Image Tables Merge: ");
		jobName.append(dateFormat.format(cal.getTime()));

		Configuration conf = getConf();
		log.info("zookeepers = " + conf.get("hbase.zookeeper.quorum"));
		log.info("rootdir = " + conf.get("hbase.rootdir"));

		Job job = Job.getInstance(conf, jobName.toString());
		job.setJarByClass(this.getClass());

		// Setup the Mapper
		Scan scan = new Scan();
		scan.addFamily(ImagesColumnNames.META_FAMILY);
//		scan.addFamily(ImagesColumnNames.IMAGE_FAMILY);
		scan.setCaching(100);
		scan.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob(conf.get("orig.hbase.table.name"), scan, ImagesTablesMergeMapper.class,
				ImmutableBytesWritable.class, BytesWritable.class, job);
		TableMapReduceUtil.initTableReducerJob(conf.get("final.hbase.table.name"), ImageTablesMergeReducer.class, job);
		
		// Set the standard reduce tasks
		job.setNumReduceTasks(Integer.parseInt(conf.get("mapred.reduce.tasks", "20")));

		// / Submit the Job
		job.submit();
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(new Configuration()),
				new ImageTablesMergeDriver(), args);
		System.exit(exitCode);
	}
}
