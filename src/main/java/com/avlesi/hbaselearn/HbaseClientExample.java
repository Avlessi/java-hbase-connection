package com.avlesi.hbaselearn;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class HbaseClientExample {
    Configuration config;
    HbaseOperations hbaseOperations = new HbaseOperations();

    public HbaseClientExample() {
        config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, "localhost");
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2182");
    }

    public void testConnection() throws Exception {
        try {
            HBaseAdmin.checkHBaseAvailable(config);
            System.out.println("HBase is available!!!");
        } catch (MasterNotRunningException e) {
            System.out.println("HBase is not running." + e.getMessage());
        }
    }


    public Connection getConnection() throws IOException {
        return ConnectionFactory.createConnection(config);
    }

    public void createTable() {
        Connection connection = null;
        try {
            connection = getConnection();
            Admin admin = connection.getAdmin();
            hbaseOperations.createTable(admin,"titi");
        }
        catch (IOException e) {
            System.out.println(e.getMessage());
        }
        finally {
            try {
                if(connection != null) {
                    connection.close();
                }
            }
            catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    public static void main(String [] args) throws IOException, ServiceException {
        System.out.println("start");
        try {
            HbaseClientExample hbaseClientExample = new HbaseClientExample();
            hbaseClientExample.testConnection();
            hbaseClientExample.createTable();
        }
        catch(Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
