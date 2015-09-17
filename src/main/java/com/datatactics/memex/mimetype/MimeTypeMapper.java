package com.datatactics.memex.mimetype;

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

import com.datatactics.memex.util.RecordCounter;

public class MimeTypeMapper extends TableMapper<Text, Text> {
	private static final Logger log = Logger.getLogger(MimeTypeMapper.class);

	@Override
	protected void setup(Context context) {
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

				// Now let's see if we can determine the mimetype
				
				
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
