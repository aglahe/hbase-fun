package com.datatactics.memex.merge;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.log4j.Logger;

import com.datatactics.memex.util.ImagesColumnNames;
import com.datatactics.memex.util.RecordCounter;

public class ImageTablesMergeReducer extends TableReducer<ImmutableBytesWritable, BytesWritable, ImmutableBytesWritable> 
{
    private static final Logger log = Logger.getLogger(ImageTablesMergeReducer.class);

	
    @Override
    protected void setup(Context context)
    {
        log.info("Setup HtImageHBaseReducer");
    }

    @Override
    protected void reduce(ImmutableBytesWritable rowId, Iterable<BytesWritable> values, Context context) throws IOException
    {
		for (BytesWritable val : values)
		{
			Put put = new Put(rowId.get());
			put.addColumn(ImagesColumnNames.META_FAMILY, ImagesColumnNames.MIMETYPE_QUALIFIER, val.getBytes());
//			put.addColumn(ImagesColumnNames.META_FAMILY, ImagesColumnNames.SIZE_QUALIFIER, Bytes.toBytes(val.getBytes().length));

			try 
			{
				context.write(null, put);
				context.getCounter(RecordCounter.HBASE_PUT).increment(1);
			}
			catch (InterruptedException e) 
			{
				log.error("Record - " + new String(rowId.get()) + " - was bad, issue Writing to main table in Reducer: ", e);
				context.getCounter(RecordCounter.HBASE_ERROR).increment(1);
			}
		}
    }
}

