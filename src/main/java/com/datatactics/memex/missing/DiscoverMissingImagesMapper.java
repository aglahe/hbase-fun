package com.datatactics.memex.missing;

import static com.datatactics.memex.util.ImagesColumnNames.IMAGE_FAMILY;
import static com.datatactics.memex.util.ImagesColumnNames.IMAGE_QUALIFIER;
import static com.datatactics.memex.util.ImagesColumnNames.LOCATION_QUALIFIER;
import static com.datatactics.memex.util.ImagesColumnNames.META_FAMILY;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.datatactics.memex.util.RecordCounter;

public class DiscoverMissingImagesMapper extends TableMapper<Text, Text> {
	private static final Logger log = Logger.getLogger(DiscoverMissingImagesMapper.class);

	@Override
	protected void setup(Context context) {
	}

	public void map(ImmutableBytesWritable rowId, Result value, Context context) throws IOException {
		context.getCounter(RecordCounter.ATTEMPTS).increment(1);

		byte[] image = value.getValue(IMAGE_FAMILY, IMAGE_QUALIFIER);
		if ((image != null) && (image.length > 0))	{
			context.getCounter(RecordCounter.IMAGE_EXISTS).increment(1);
		}
		else
		{
			String locationURI = Bytes.toString(value.getValue(META_FAMILY, LOCATION_QUALIFIER));
			String s3objId = StringUtils.substringAfterLast(locationURI, "/");

			if (StringUtils.isNotBlank(s3objId)) {

				Text s3objIdText = new Text(s3objId);
				Text keyText = new Text(rowId.get());
				try {
					context.write(keyText, s3objIdText);
					context.getCounter(RecordCounter.S3_LOCATION_SUCCESS).increment(1);
				} catch (InterruptedException e) {
					log.error("Couldn't write from mapper", e);
					context.getCounter(RecordCounter.HADOOP_WRITE_ERROR).increment(1);
				}

			} else {
				log.warn(Bytes.toString(rowId.get()) + " had no s3objId to be parsed..probably null, locationURI: " + locationURI);
				context.getCounter(RecordCounter.S3_LOCATION_MISSING).increment(1);
			}
		}
	}
}
