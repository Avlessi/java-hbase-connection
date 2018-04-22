package com.avlesi.hbaselearn;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;

import java.io.IOException;

public class HbaseOperations {

    public void createTable(Admin admin, String tableName) throws IOException {
        TableName tname = TableName.valueOf(tableName);
        if (!admin.tableExists(tname)) {
            HTableDescriptor desc = new HTableDescriptor(tname);
            desc.addFamily(new HColumnDescriptor("fam1"));
            desc.addFamily(new HColumnDescriptor("fam2"));
            admin.createTable(desc);
        }
    }
}
