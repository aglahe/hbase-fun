package com.datatactics.memex.merge;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.log4j.Logger;

import com.datatactics.memex.util.ImagesColumnNames;
import com.datatactics.memex.util.RecordCounter;

public class ImagesTablesMergeMapper extends TableMapper<ImmutableBytesWritable, BytesWritable> {
	private static final Logger log = Logger.getLogger(ImagesTablesMergeMapper.class);

	@Override
	protected void setup(Context context) {
	}

	public void map(ImmutableBytesWritable rowId, Result value, Context context) throws IOException {

		// Number of Attempts, or Keys read
		context.getCounter(RecordCounter.ATTEMPTS).increment(1);

		byte[] mimeTypeBytes = value.getValue(ImagesColumnNames.META_FAMILY, ImagesColumnNames.MIMETYPE_QUALIFIER);
		if ((mimeTypeBytes!=null) && (mimeTypeBytes.length>0))
		{
			BytesWritable mimeTypeWritable = new BytesWritable(mimeTypeBytes);
 			try {
				context.write(rowId, mimeTypeWritable);
				context.getCounter(RecordCounter.IMAGE_SUCCESS).increment(1);
			} catch (InterruptedException e) {
				log.error("Issues writing rowid and/or bytes writable", e);
				context.getCounter(RecordCounter.IMAGE_ERROR).increment(1);
			} catch (ArrayIndexOutOfBoundsException e)
 			{
				log.error("rowId: " + Bytes.toString(rowId.get()) + "mimeTypeWritable size:" + mimeTypeWritable.getLength());
				throw e;
 			}
		}
		else
		{
			context.getCounter(RecordCounter.MIMETYPE_MISSING).increment(1);
		}
	}
}
