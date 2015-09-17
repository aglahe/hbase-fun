package com.datatactics.memex.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class PairWritable implements Writable {

	private Text colName;
	private BytesWritable data;
	
	public PairWritable(Text colName, BytesWritable bytes)
	{
		this.colName = colName;
		this.data = bytes;
	}

	public PairWritable(String colName, byte[] bytes)
	{
		this.colName = new Text(colName);
		this.data = new BytesWritable(bytes);
	}
	
	public PairWritable() 
	{
		this.colName = new Text();
		this.data = new BytesWritable();
	} 
	
	@Override
	public void readFields(DataInput in) throws IOException {
		colName.readFields(in);
		data.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		colName.write(out);
		data.write(out);
	}

	public Text getKey() {
		return colName;
	}

	public void setKey(Text key) {
		this.colName = key;
	}

	public BytesWritable getImageBytes() {
		return data;
	}

	public void setBytes(BytesWritable bytes) {
		this.data = bytes;
	}

	@Override
    public boolean equals(Object o) 
	{
      if (o instanceof PairWritable)
      {
    	  PairWritable value = (PairWritable) o;
    	  return colName.equals(value.colName) && data.equals(value.data);
      }
      
      return false;
    }
	
	@Override
	public String toString()
	{
		return colName + ", [image size]: " + data.getLength(); 
	}

	@Override
	public int hashCode()
	{
		return colName.hashCode();
	}
	
}
