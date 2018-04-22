package com.avlesi.hbaselearn;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class HbaseClientExample {

    public void testConnection() throws Exception {
        Configuration config = HBaseConfiguration.create();

//        String path = this.getClass()
//                .getClassLoader()
//                .getResource("hbase-site.xml")
//                .getPath();
//        config.addResource(new Path(path));

        config.set(HConstants.ZOOKEEPER_QUORUM, "localhost");
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");

        try {
            HBaseAdmin.checkHBaseAvailable(config);
        } catch (MasterNotRunningException e) {
            System.out.println("HBase is not running." + e.getMessage());
            return;
        }

    }

    public static void main(String [] args) throws IOException, ServiceException {

        System.out.println("hello!");
        try {
            HbaseClientExample hbaseClientExample = new HbaseClientExample();
            hbaseClientExample.testConnection();
        }
        catch(Exception e) {
            System.out.println(e.getMessage());
        }

    }
}
