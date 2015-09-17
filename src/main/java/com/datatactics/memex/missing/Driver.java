package com.datatactics.memex.missing;

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
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.datatactics.memex.savetsv.HtImageTSVReducer;
import com.datatactics.memex.util.ImagesColumnNames;

public class Driver extends Configured implements Tool {
	private static final Logger log = Logger.getLogger(Driver.class);

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		log.info("zookeepers = " + conf.get("hbase.zookeeper.quorum"));
		log.info("rootdir = " + conf.get("hbase.rootdir"));

		// Setup the Mapper
		Scan scan = new Scan();
		scan.addFamily(ImagesColumnNames.META_FAMILY);
		scan.addFamily(ImagesColumnNames.IMAGE_FAMILY);
		scan.setCacheBlocks(false);

		// Construct the Job name, using Date to help easily find in JT
		StringBuilder jobName = new StringBuilder("Missing Images Check");

		// Limit our scan
		String startrow = conf.get("hbase.startrow");
		if (StringUtils.isNotBlank(startrow)) {
			log.info("Setting start limit on the scan:" + startrow);
			scan.setStartRow(startrow.getBytes());
			jobName.append(" start at: " + startrow);
		}

		String endrow = conf.get("hbase.endrow");
		if (StringUtils.isNotBlank(endrow)) {
			log.info("Setting end limit on the scan:" + endrow);
			scan.setStopRow(endrow.getBytes());
			jobName.append(" to " + endrow);
		}

		// Create Job
		Job job = Job.getInstance(conf, jobName.toString());
		job.setJarByClass(this.getClass());

		// Input
		TableMapReduceUtil.initTableMapperJob(conf.get("hbase.table.name"), scan,
				DiscoverMissingImagesMapper.class, Text.class, Text.class, job);


		// Set it up so we can do a check, and count kinda job
		boolean checkonly = conf.getBoolean("checkonly", false);
		if (!checkonly) {
			log.info("Get any missing images");

			// Set the retriver class
			job.setReducerClass(HtImageTSVReducer.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			// Path to save the S3 Images
			Path dataPath = new Path(args[0]);
			
			// Output
			TextOutputFormat.setOutputPath(job, dataPath);
			
			// Set the standard reduce tasks
			job.setNumReduceTasks(Integer.parseInt(conf.get("mapred.reduce.tasks", "40")));
		} else {
			log.info("Doing a checkonly");
			job.setOutputFormatClass(NullOutputFormat.class);
			job.setNumReduceTasks(0);
		}

		// / Submit the Job
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(new Configuration()), new Driver(), args);
		System.exit(exitCode);
	}
}
