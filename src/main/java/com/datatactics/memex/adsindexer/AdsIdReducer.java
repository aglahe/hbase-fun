package com.datatactics.memex.adsindexer;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.log4j.Logger;

import com.datatactics.memex.util.ImagesColumnNames;
import com.datatactics.memex.util.RecordCounter;

public class AdsIdReducer extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> 
{
    private static final Logger log = Logger.getLogger(AdsIdReducer.class);

	
    @Override
    protected void setup(Context context)
    {
        log.info("Setup AdsIdReducer");
    }

    @Override
    protected void reduce(ImmutableBytesWritable adId, Iterable<ImmutableBytesWritable> values, Context context) throws IOException
    {
		Put put = new Put(adId.copyBytes());
    	for (ImmutableBytesWritable val : values)
		{
			put.addColumn(ImagesColumnNames.IMAGE_ID_FAMILY, val.copyBytes(), new byte[0]);
			try 
			{
				context.write(null, put);
				context.getCounter(RecordCounter.HBASE_PUT).increment(1);
			}
			catch (InterruptedException e) 
			{
				log.error("Record - " + new String(adId.get()) + " - was bad, issue Writing to main table in Reducer: ", e);
				context.getCounter(RecordCounter.HBASE_ERROR).increment(1);
			}
		}
    }
}

