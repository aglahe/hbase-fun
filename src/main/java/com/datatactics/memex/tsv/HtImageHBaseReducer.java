package com.datatactics.memex.tsv;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.log4j.Logger;

import com.datatactics.memex.util.RecordCounter;

public class HtImageHBaseReducer extends TableReducer<ImmutableBytesWritable, BytesWritable, ImmutableBytesWritable> 
{
    private static final Logger log = Logger.getLogger(HtImageHBaseReducer.class);

	public static final byte[] IMAGE_CF = "image".getBytes();
	public static final byte[] IMAGE_CQ = "orig".getBytes();
	
    @Override
    protected void setup(Context context)
    {
        log.info("Setup HtImageHBaseReducer");
    }

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<BytesWritable> values, Context context) throws IOException
    {
		for (BytesWritable val : values)
		{
			Put put = new Put(key.get());
			put.addColumn(IMAGE_CF, IMAGE_CQ, val.getBytes());

			try 
			{
				context.write(null, put);
				context.getCounter(RecordCounter.HBASE_PUT).increment(1);
			}
			catch (InterruptedException e) 
			{
				log.error("Record - " + new String(key.get()) + " - was bad, issue Writing to main table in Reducer: ", e);
				context.getCounter(RecordCounter.HBASE_ERROR).increment(1);
			}
		}
    }
}

