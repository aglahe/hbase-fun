package com.datatactics.memex.util;

// Used for counting the records ingested
public enum RecordCounter 
{
	ATTEMPTS, S3_LOCATION_MISSING, S3_LOCATION_SUCCESS,
	AWS_CLIENT_ERRORS, AWS_SERVICE_ERRORS, 
	IMAGE_ATTEMPTS, IMAGE_SUCCESS, IMAGE_ERROR, IMAGE_EXISTS, IMAGE_MISSING, IMAGE_ZERO_BYTES, 
	MIMETYPE_MISSING, MIMETYPE_EXISTS, 
	HBASE_PUT, HBASE_ERROR, HADOOP_WRITE_ERROR
}
