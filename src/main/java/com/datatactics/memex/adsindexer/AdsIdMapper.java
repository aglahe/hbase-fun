package com.datatactics.memex.adsindexer;

import static com.datatactics.memex.util.ImagesColumnNames.ADS_ID_QUALIFIER;
import static com.datatactics.memex.util.ImagesColumnNames.META_FAMILY;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.log4j.Logger;

import com.datatactics.memex.util.RecordCounter;

public class AdsIdMapper extends TableMapper<BytesWritable, ImmutableBytesWritable> {
	private static final Logger log = Logger.getLogger(AdsIdMapper.class);

	@Override
	protected void setup(Context context) {
	}

	public void map(ImmutableBytesWritable rowId, Result value, Context context) throws IOException {
		context.getCounter(RecordCounter.ATTEMPTS).increment(1);

		byte[] adsIdBytes = value.getValue(META_FAMILY, ADS_ID_QUALIFIER);

		if ((adsIdBytes != null) && (adsIdBytes.length > 0))	{
			context.getCounter(RecordCounter.ADS_ID_EXISTS).increment(1);
			BytesWritable adId = new BytesWritable(adsIdBytes);
			try {
				context.write(adId, rowId);
			} catch (InterruptedException e) {
				log.error("Couldn't write from mapper", e);
				context.getCounter(RecordCounter.HADOOP_WRITE_ERROR).increment(1);
			}
		}
		else
		{
			context.getCounter(RecordCounter.ADS_ID_MISSING).increment(1);
			log.warn(Bytes.toString(rowId.get()) + " had no ads Id to be parsed..probably null, locationURI: " + Bytes.toString(adsIdBytes));
		}
	}
}
