package com.datatactics.examples;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class HBaseReducer extends TableReducer<ImmutableBytesWritable, Text, Text> 
{
    private static final Logger log = Logger.getLogger(HBaseReducer.class);

	public static final byte[] CF = "meta".getBytes();
	public static final byte[] CQ = "size".getBytes();

    
    @Override
    protected void setup(Context context)
    {
        log.info("Setup HBaseReducer");
    }

    @Override
    protected void reduce(ImmutableBytesWritable rowId, Iterable<Text> values, Context context) throws IOException
    {
		Put put = new Put(rowId.get());
		for (Text val : values) 
		{
			put.add(CF, CQ, Bytes.toBytes(val.toString()));
		}

		try 
		{
			context.write(null, put);
		}
		catch (InterruptedException e) 
		{
			log.error("Error writing to HBase", e);
		}
    }
}
