package com.datatactics.memex.adsindexer;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.log4j.Logger;

import com.datatactics.memex.util.ImagesColumnNames;
import com.datatactics.memex.util.RecordCounter;

public class AdsIdReducer extends TableReducer<BytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> 
{
    private static final Logger log = Logger.getLogger(AdsIdReducer.class);

	
    @Override
    protected void setup(Context context)
    {
        log.info("Setup AdsIdReducer");
    }

    @Override
    protected void reduce(BytesWritable adId, Iterable<ImmutableBytesWritable> values, Context context) throws IOException
    {
		for (ImmutableBytesWritable val : values)
		{
			Put put = new Put(adId.getBytes());
			put.addColumn(ImagesColumnNames.ADS_ID_FAMILY, val.copyBytes(), new byte[0]);
			try 
			{
				context.write(null, put);
				context.getCounter(RecordCounter.HBASE_PUT).increment(1);
			}
			catch (InterruptedException e) 
			{
				log.error("Record - " + new String(adId.getBytes()) + " - was bad, issue Writing to main table in Reducer: ", e);
				context.getCounter(RecordCounter.HBASE_ERROR).increment(1);
			}
		}
    }
}

