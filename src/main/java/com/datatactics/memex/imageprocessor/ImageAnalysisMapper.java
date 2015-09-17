package com.datatactics.memex.imageprocessor;

import static com.datatactics.memex.util.ImagesColumnNames.IMAGE_FAMILY;
import static com.datatactics.memex.util.ImagesColumnNames.IMAGE_QUALIFIER;
import static com.datatactics.memex.util.ImagesColumnNames.META_FAMILY;
import static com.datatactics.memex.util.ImagesColumnNames.MIMETYPE_QUALIFIER;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.datatactics.memex.io.PairWritable;
import com.datatactics.memex.util.RecordCounter;

public class ImageAnalysisMapper extends TableMapper<Text, PairWritable> {
	private static final Logger log = Logger.getLogger(ImageAnalysisMapper.class);

	private ImageProcessor analyizer;
	
	@Override
	protected void setup(Context context) 
	{
		this.analyizer = new ImageProcessor();
	}

	public void map(ImmutableBytesWritable rowId, Result value, Context context) throws IOException {
		
		String mimeType = Bytes.toString(value.getValue(META_FAMILY, MIMETYPE_QUALIFIER));
		context.getCounter(RecordCounter.ATTEMPTS).increment(1);

		if (StringUtils.isBlank(mimeType)) {
			context.getCounter(RecordCounter.MIMETYPE_MISSING).increment(1);

			// Attempt to get a 
			byte[] image = value.getValue(IMAGE_FAMILY, IMAGE_QUALIFIER);
			if ((image != null) && (image.length > 0))	{
				context.getCounter(RecordCounter.IMAGE_SUCCESS).increment(1);

				// Now let's see if we can analyize this image
				PairWritable analysisBtyes = analyizer.process(image);
				
				// Save it out
				Text keyText = new Text(rowId.get());
				try {
					context.write(keyText, analysisBtyes);
					context.getCounter(RecordCounter.IMAGE_ANALYIZED).increment(1);
				} catch (InterruptedException e) {
					log.error("Couldn't write from mapper", e);
					context.getCounter(RecordCounter.HADOOP_WRITE_ERROR).increment(1);
				}
			}
			else {
				log.warn(Bytes.toString(rowId.get()) + " missing image");
				context.getCounter(RecordCounter.IMAGE_MISSING).increment(1);
			}
		} else {
			log.warn(Bytes.toString(rowId.get()) + " had MIMETYPE: " + mimeType);
			context.getCounter(RecordCounter.MIMETYPE_EXISTS).increment(1);
		}
	}
}
