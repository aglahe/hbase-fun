package com.datatactics.memex.tsv;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3.S3Exception;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.datatactics.memex.util.ConfigCredentials;
import com.datatactics.memex.util.RecordCounter;

public class HtImageHBaseMapper extends TableMapper<ImmutableBytesWritable, BytesWritable> {
	private static final Logger log = Logger.getLogger(HtImageHBaseMapper.class);

	public static final byte[] META_CF = "meta".getBytes();
	public static final byte[] META_CQ = "location".getBytes();
	
	private AmazonS3Client s3Client;
	private ConfigCredentials credentials;
	private String bucketName;

	@Override
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		this.bucketName = conf.get("s3.bucketName");
		
		try {
			this.credentials = new ConfigCredentials(conf);
			this.s3Client = new AmazonS3Client(this.credentials);
			log.debug("S3 Client created");

		} catch (S3Exception e) {
			log.error("Something evil with AWS Credentials..couldn't create", e);
		}
	}

	public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException {
		
		String locationURI = Bytes.toString(value.getValue(META_CF, META_CQ));
		String s3objId = StringUtils.substringAfterLast(locationURI, "/");

		// Number of Attempts, or Keys read
		context.getCounter(RecordCounter.ATTEMPTS).increment(1);

		if (StringUtils.isNoneEmpty(s3objId)) {

			// Get the Object
			try {

				S3Object object = s3Client.getObject(this.bucketName, s3objId);
				S3ObjectInputStream s3Is = object.getObjectContent();

				try {
					BytesWritable imageBytes = new BytesWritable(IOUtils.toByteArray(s3Is));
					log.debug("imagesize = "+ imageBytes.getLength());
					// Write out the Map
					try {
						context.write(key, imageBytes);
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
		} else {
			log.warn(Bytes.toString(key.get()) + " had no s3objId to be parsed..probably null, locationURI: " + locationURI);
		}
	}
}
