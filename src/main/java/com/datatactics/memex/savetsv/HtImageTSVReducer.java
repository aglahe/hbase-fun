package com.datatactics.memex.savetsv;

import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.text.StrBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3.S3Exception;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.datatactics.memex.util.ConfigCredentials;
import com.datatactics.memex.util.RecordCounter;

public class HtImageTSVReducer extends Reducer<Text, Text, NullWritable, Text> {
	private static final Logger log = Logger.getLogger(HtImageTSVReducer.class);

	private AmazonS3Client s3Client;
	private ConfigCredentials credentials;
	private String bucketName;

	@Override
	protected void setup(Context context) {
		log.info("Setup HtImageTSVReducer");
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

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		if (this.s3Client != null) {
			log.debug("S3 Client shuttingdown");
			this.s3Client.shutdown();
		} else {
			log.error("No S3 Client to Shutdown");
		}
	}

	@Override
	protected void reduce(Text rowId, Iterable<Text> values, Context context) throws InterruptedException {
		for (Text val : values) {
			context.getCounter(RecordCounter.ATTEMPTS).increment(1);

			// Get the Object
			try {

				String s3objId = val.toString();
				S3Object object = s3Client.getObject(this.bucketName, s3objId);
				S3ObjectInputStream s3Is = object.getObjectContent();
				
				try {
					byte[] imageBytes = Base64.encodeBase64(IOUtils.toByteArray(s3Is));

					log.debug("imagesize = " + imageBytes.length);
					if (imageBytes.length > 0) {
						context.getCounter(RecordCounter.IMAGE_SUCCESS).increment(1);

						StrBuilder builder = new StrBuilder(rowId.toString());
						builder.appendSeparator('\t');
						builder.append(new String(imageBytes));

						Text output = new Text(builder.build());
						context.write(null, output);
					} else {
						context.getCounter(RecordCounter.IMAGE_ZERO_BYTES).increment(1);
					}

				} catch (IOException e) {
					context.getCounter(RecordCounter.IMAGE_ERROR).increment(1);
					log.error("Could not read Oject from input Stream", e);
				}

			} catch (AmazonServiceException e1) {
				log.error("Service issue", e1);
				context.getCounter(RecordCounter.AWS_SERVICE_ERRORS).increment(1);
			} catch (AmazonClientException e1) {
				log.error("Client issue", e1);
				context.getCounter(RecordCounter.AWS_CLIENT_ERRORS).increment(1);
			}
		}
	}
}
