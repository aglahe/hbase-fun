package com.datatactics.memex.tsv;

import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3.S3Exception;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.datatactics.memex.io.HtImageWritable;
import com.datatactics.memex.util.ConfigCredentials;
import com.datatactics.memex.util.RecordCounter;

public class HtImageFileMapper extends Mapper<LongWritable, Text, Text, HtImageWritable> {
	private static final Logger log = Logger.getLogger(HtImageFileMapper.class);

//	private AmazonS3Client s3Client;
	private ConfigCredentials credentials;
	private String bucketName;

	@Override
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		this.bucketName = conf.get("s3.bucketName");
		try {
			this.credentials = new ConfigCredentials(conf);
		} catch (S3Exception e) {
			log.error("Something evil with AWS Credentials..couldn't create", e);
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) {
		StrTokenizer tokenizer = new StrTokenizer(value.toString(), ',');
		List<String> tokens = tokenizer.getTokenList();

		// Get the Object Key
		String locationURI = tokens.get(0);
		String dateTime = tokens.get(1);
		log.debug("locationURI: " + locationURI);
		String rowId = StringUtils.substringAfterLast(locationURI, "/");

		if (StringUtils.isNoneEmpty(rowId)) {
			log.debug("Parsed rowId: " + rowId);

			// Create client
			if (this.credentials != null)
			{
				AmazonS3Client s3Client = new AmazonS3Client(this.credentials);
				log.debug("S3 Client created");
				
				// Get the Object
				try {

					S3Object object = s3Client.getObject(this.bucketName, rowId);
					S3ObjectInputStream s3Is = object.getObjectContent();

					try {
						byte[] imageBytes = IOUtils.toByteArray(s3Is);

						// Write out the Map
						try {
							HtImageWritable imageValue = new HtImageWritable(rowId, imageBytes);
							
							context.write(new Text(dateTime), imageValue);
							context.getCounter(RecordCounter.IMAGE_SUCCESS).increment(1);

						} catch (IOException | InterruptedException e) {
							log.error("Issues writing rowid and/or mapwritable", e);
							context.getCounter(RecordCounter.IMAGE_ERROR).increment(1);
						}
					} catch (IOException e) {
						log.error("Could not read Oject from input Stream", e);
					}

				} catch (AmazonServiceException e1) {
					log.error("Service issue", e1);
					context.getCounter(RecordCounter.AWS_SERVICE_ERRORS).increment(1);
				} catch (AmazonClientException e1) {
					log.error("Client issue", e1);
					context.getCounter(RecordCounter.AWS_CLIENT_ERRORS).increment(1);
				}

				context.getCounter(RecordCounter.ATTEMPTS).increment(1);
			}
			else
			{
				log.warn("AWS Credentials was null..couldn't create AWS Client");
			}
		} else {
			log.debug("There was no rowID to be parsed..probably null");
		}
	}
}
