Hbase-Examples
==============

yarn jar target/hbase-test-0.0.1-SNAPSHOT.jar com.datatactics.examples.HBaseDriver -conf /etc/hbase/conf/hbase-site.xml

yarn jar target/hbase-test-0.0.1-SNAPSHOT.jar com.datatactics.examples.HBaseDriver -conf /etc/hbase/conf/hbase-site.xml -Dhbase.startrow=1 -Dhbase.endrow=10

yarn jar target/hbase-test-0.0.1-SNAPSHOT.jar com.datatactics.memex.missing.Driver -conf ./src/main/resources/amazon.xml -conf ./src/main/resources/hbase-extras.xml -conf /etc/hbase/conf/hbase-site.xml /user/aarong/ht_images
