package com.avlesi.hbaselearn;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class HbaseClientExample {

    public void testConnection() throws Exception {
        org.apache.hadoop.hbase.MasterNotRunningException ex;
        Configuration config = HBaseConfiguration.create();

        String path = this.getClass()
                .getClassLoader()
                .getResource("hbase-site.xml")
                .getPath();
        config.addResource(new Path(path));

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
