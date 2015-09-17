package com.datatactics.memex.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class HtImageWritable implements Writable {

	private Text key;
	private BytesWritable image;
	
	public HtImageWritable(Text key, BytesWritable bytes)
	{
		this.key = key;
		this.image = bytes;
	}

	public HtImageWritable(String key, byte[] bytes)
	{
		this.key = new Text(key);
		this.image = new BytesWritable(bytes);
	}
	
	public HtImageWritable() 
	{
		this.key = new Text();
		this.image = new BytesWritable();
	} 
	
	@Override
	public void readFields(DataInput in) throws IOException {
		key.readFields(in);
		image.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		key.write(out);
		image.write(out);
	}

	public Text getKey() {
		return key;
	}

	public void setKey(Text key) {
		this.key = key;
	}

	public BytesWritable getImageBytes() {
		return image;
	}

	public void setBytes(BytesWritable bytes) {
		this.image = bytes;
	}

	@Override
    public boolean equals(Object o) 
	{
      if (o instanceof HtImageWritable)
      {
    	  HtImageWritable value = (HtImageWritable) o;
    	  return key.equals(value.key) && image.equals(value.image);
      }
      
      return false;
    }
	
	@Override
	public String toString()
	{
		return key + ", [image size]: " + image.getLength(); 
	}

	@Override
	public int hashCode()
	{
		return key.hashCode();
	}
}
