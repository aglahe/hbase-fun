package com.datatactics.examples;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class HBaseMapper extends TableMapper<ImmutableBytesWritable, Text> 
{
    private static final Logger log = Logger.getLogger(HBaseMapper.class);

    private Text imageSize;
    
    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException
    {
        log.info("Setup HBaseMapper");
        
        this.imageSize = new Text();
        
    }

    public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException
    {
    	int size = value.getValue("meta".getBytes(), "location".getBytes()).length;

    	// Set the size
    	log.info("Image size = " + size);
    	imageSize.set(Integer.toString(size));;
    	
    	try 
    	{
			context.write(key, imageSize);
		} 
    	catch (InterruptedException e) 
    	{
			log.error("Error writing Mapper output", e);
		}
    }
}
