package com.geekbang;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;


public class HBaseTest {
    public static void main(String[] args) throws IOException {
        // 建立链接
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "127.0.0.1");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "127.0.0.1:60000");
        
        Connection connection = ConnectionFactory.createConnection(conf);

        Admin admin = connection.getAdmin();

        TableName tableName = TableName.valueOf("user_t");
        String columnFamily = "User";
        int rowKey = 1;

        // 建表
        if(admin.tableExists(tableName)) {
            System.out.println("Table already exists.");
        } else {
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
            ColumnFamilyDescriptorBuilder cfDescriptorBuilder =ColumnFamilyDescriptorBuilder.newBuilder(columnFamily.getBytes());
            tableDescriptorBuilder.setColumnFamily(cfDescriptorBuilder.build());
            admin.createTable(tableDescriptorBuilder.build());
            System.out.println("Table create successful.");
        }

        // 插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("uid"), Bytes.toBytes("001"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("name"), Bytes.toBytes("Tim"));
        connection.getTable(tableName).put(put);
        System.out.println("Data insert success");

        // 查看数据
        Get get = new Get(Bytes.toBytes(rowKey));
        if(!get.isCheckExistenceOnly()) {
            Result result = connection.getTable(tableName).get(get);
            for(Cell cell: result.rawCells()) {
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String colValue = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("Data get success, column name:" + colName +", value:"+colValue+".");
            }
        }

        // 删除数据
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        connection.getTable(tableName).delete(delete);
        System.out.println("Delete success");

        // 删除表
        if(admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table delete successful");
        } else {
            System.out.println("Table does not exists!");
        }
    }
}
