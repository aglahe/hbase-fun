package com.datatactics.memex.mimetype;

import static com.datatactics.memex.util.ImagesColumnNames.META_FAMILY;
import static com.datatactics.memex.util.ImagesColumnNames.MIMETYPE_QUALIFIER;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.datatactics.examples.HBaseReducer;

public class MimeTypeReducer extends TableReducer<ImmutableBytesWritable, Text, Text> 
{
    private static final Logger log = Logger.getLogger(HBaseReducer.class);

    
    @Override
    protected void setup(Context context)
    {
        log.info("Setup MimeTypeReducer");
    }

    @Override
    protected void reduce(ImmutableBytesWritable rowId, Iterable<Text> values, Context context) throws IOException
    {
		Put put = new Put(rowId.get());
		for (Text val : values) 
		{
			put.addColumn(META_FAMILY, MIMETYPE_QUALIFIER, Bytes.toBytes(val.toString()));
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
