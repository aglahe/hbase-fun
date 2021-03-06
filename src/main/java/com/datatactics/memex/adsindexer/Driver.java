package com.datatactics.memex.adsindexer;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.datatactics.memex.util.ImagesColumnNames;

public class Driver extends Configured implements Tool {
	private static final Logger log = Logger.getLogger(Driver.class);
	
	@Override
	public int run(String[] args) throws Exception {
		// Calendar/Time to be used in a few places
		final Calendar cal = Calendar.getInstance();
		final SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MMM-yyyy HH:mm");

		// Construct the Job name, using Date to help easily find in JT
		StringBuilder jobName = new StringBuilder("Ad Id Indexer: ");
		jobName.append(dateFormat.format(cal.getTime()));

		Configuration conf = getConf();
		log.info("zookeepers = " + conf.get("hbase.zookeeper.quorum"));
		log.info("rootdir = " + conf.get("hbase.rootdir"));

		Job job = Job.getInstance(conf, jobName.toString());
		job.setJarByClass(this.getClass());

		// Set the standard reduce tasks
		job.setNumReduceTasks(Integer.parseInt(conf.get("mapred.reduce.tasks", "20")));
		
		// Setup the Mapper
		Scan scan = new Scan();
		scan.addFamily(ImagesColumnNames.META_FAMILY);
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
		TableMapReduceUtil.initTableMapperJob(conf.get("hbase.table.name"), scan, AdsIdMapper.class,
				ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);

		// See if we need to make the index table
		String indexTableName = conf.get("hbase.index.table.name");
		
		// Output
		TableMapReduceUtil.initTableReducerJob(indexTableName, AdsIdReducer.class, job);
		
		// / Submit the Job
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(new Configuration()),
				new Driver(), args);
		System.exit(exitCode);
	}
}
