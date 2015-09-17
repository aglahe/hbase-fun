register /usr/lib/zookeeper/zookeeper-3.4.5-cdh5.4.5.jar
register /usr/lib/hbase/hbase-client-1.0.0-cdh5.4.5.jar

raw = LOAD 'hbase://aglahe-roxyimages' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('image:orig meta:type', '-loadKey=true') AS (id:bytearray, image:bytearray, meta:chararray);

# Create rowid projection
rowids = FOREACH raw GENERATE id;

dump rowids;
