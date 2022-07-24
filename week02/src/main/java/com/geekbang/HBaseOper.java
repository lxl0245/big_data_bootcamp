package com.geekbang;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
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

public class HBaseOper {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        // 建立链接
        // 本地 Docker HBase 服务的配置
        // String ip = "114.55.52.33";
        // String ip = "127.0.0.1";
        // String port = "60000";

        // conf.set("hbase.zookeeper.quorum", ip);
        // conf.set("hbase.zookeeper.property.clientPort", "2181");
        // conf.set("hbase.master", ip+port);
        // 极客时间的HBase服务配置
        conf.set("hbase.zookeeper.quorum", "emr-worker-2:2181,emr-worker-1:2181,emr-header-1:2181"); 
        
        Connection connection = ConnectionFactory.createConnection(conf);

        Admin admin = connection.getAdmin();

        String NSname = "lixinli";
        String TbName = "student";
        String CFInfo = "info";
        String CFScore = "score";

        NamespaceDescriptor NSLxl = NamespaceDescriptor.create(NSname).build();
        TableName tabName = TableName.valueOf(NSname, TbName);

        // 创建表空间
        try {
            admin.createNamespace(NSLxl);
            System.out.println("命名空间 "+NSname+" 创建成功");
        } catch (NamespaceExistException e) {
            System.out.println("命名空间 "+NSname+" 已经存在!");
            /*
            System.out.println(admin.listNamespaces());
            for(String ns : admin.listNamespaces()) {
                System.out.println(ns);
            } */
        }

        // 创建表
        if(admin.tableExists(tabName)) {
            System.out.println("表 "+TbName+" 已经存在");
        } else {
            TableDescriptorBuilder student = TableDescriptorBuilder.newBuilder(tabName);
            // ColumnFamilyDescriptorBuilder CFDBName = ColumnFamilyDescriptorBuilder.newBuilder(CFName.getBytes());
            ColumnFamilyDescriptorBuilder CFDBInfo = ColumnFamilyDescriptorBuilder.newBuilder(CFInfo.getBytes());
            ColumnFamilyDescriptorBuilder CFDBScore = ColumnFamilyDescriptorBuilder.newBuilder(CFScore.getBytes());
            
            // student.setColumnFamily(CFDBName.build());
            student.setColumnFamily(CFDBInfo.build());
            student.setColumnFamily(CFDBScore.build());

            admin.createTable(student.build());
            System.out.println("成功创建表 "+NSname+":"+TbName);
        }

        // 插入数据
        // 方法1：一行一行的插入； 方法2：一次插入多行 Bluk Insert
        Put put_tom = new Put(Bytes.toBytes("Tom"));
        put_tom.addColumn(Bytes.toBytes(CFInfo), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000001"));
        put_tom.addColumn(Bytes.toBytes(CFInfo), Bytes.toBytes("class"), Bytes.toBytes("1"));
        put_tom.addColumn(Bytes.toBytes(CFScore), Bytes.toBytes("understanding"), Bytes.toBytes("75"));
        put_tom.addColumn(Bytes.toBytes(CFScore), Bytes.toBytes("programming"), Bytes.toBytes("82"));
        // connection.getTable(tabName).put(put_tom);

        Put put_jerry = new Put(Bytes.toBytes("Jerry"));
        put_jerry.addColumn(Bytes.toBytes(CFInfo), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000002"));
        put_jerry.addColumn(Bytes.toBytes(CFInfo), Bytes.toBytes("class"), Bytes.toBytes("1"));
        put_jerry.addColumn(Bytes.toBytes(CFScore), Bytes.toBytes("understanding"), Bytes.toBytes("85"));
        put_jerry.addColumn(Bytes.toBytes(CFScore), Bytes.toBytes("programming"), Bytes.toBytes("76"));
        // connection.getTable(tabName).put(put_jerry);

        Put put_jack = new Put(Bytes.toBytes("Jack"));
        put_jack.addColumn(Bytes.toBytes(CFInfo), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000003"));
        put_jack.addColumn(Bytes.toBytes(CFInfo), Bytes.toBytes("class"), Bytes.toBytes("2"));
        put_jack.addColumn(Bytes.toBytes(CFScore), Bytes.toBytes("understanding"), Bytes.toBytes("80"));
        put_jack.addColumn(Bytes.toBytes(CFScore), Bytes.toBytes("programming"), Bytes.toBytes("80"));
        // connection.getTable(tabName).put(put_jack);

        Put put_rose = new Put(Bytes.toBytes("Rose"));
        put_rose.addColumn(Bytes.toBytes(CFInfo), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000004"));
        put_rose.addColumn(Bytes.toBytes(CFInfo), Bytes.toBytes("class"), Bytes.toBytes("2"));
        put_rose.addColumn(Bytes.toBytes(CFScore), Bytes.toBytes("understanding"), Bytes.toBytes("61"));
        put_rose.addColumn(Bytes.toBytes(CFScore), Bytes.toBytes("programming"), Bytes.toBytes("61"));
        // connection.getTable(tabName).put(put_rose);

        Put put_lxl = new Put(Bytes.toBytes("lixinli"));
        put_lxl.addColumn(Bytes.toBytes(CFInfo), Bytes.toBytes("student_id"), Bytes.toBytes("G20220735040045"));
        put_lxl.addColumn(Bytes.toBytes(CFInfo), Bytes.toBytes("class"), Bytes.toBytes("3"));
        put_lxl.addColumn(Bytes.toBytes(CFScore), Bytes.toBytes("understanding"), Bytes.toBytes("99"));
        put_lxl.addColumn(Bytes.toBytes(CFScore), Bytes.toBytes("programming"), Bytes.toBytes("99"));
        // connection.getTable(tabName).put(put_lxl);

        List<Put> new_rows = new ArrayList<Put>();
        new_rows.add(put_jack);
        new_rows.add(put_jerry);
        new_rows.add(put_lxl);
        new_rows.add(put_rose);
        new_rows.add(put_tom);
        connection.getTable(tabName).put(new_rows);

        
        // 查询数据
        // 方式1： 通过 rowkey 查询一整行
        Get get = new Get(Bytes.toBytes("lixinli"));
        if(!get.isCheckExistenceOnly()) {
            Result result = connection.getTable(tabName).get(get);
            for(Cell cell: result.rawCells()) {
                // 方法1： 获取qualifier 和 value
                // String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                // String colValue = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                // System.out.println("Data get success, column name:" + colName +", value:"+colValue+".");
                // 方法2： 获取 qualifier 和 value
                byte[] rowKey = CellUtil.cloneRow(cell);
                byte[] columnFamilyName = CellUtil.cloneFamily(cell);
                byte[] qualifierName = CellUtil.cloneQualifier(cell);
                byte[] cellValue = CellUtil.cloneValue(cell);
                System.out.println("Data get success, "+ "rowKey:"+Bytes.toString(rowKey) + ", column family: "+Bytes.toString(columnFamilyName) + ", column name:" + Bytes.toString(qualifierName) + ", cell value:"+Bytes.toString(cellValue)+".");
            }
        }
        // 方式2： 通过 scan 查询整表
        /**
         * 不知道rowkey的具体值，我想查询rowkey范围值是 0003 到0006
         * select * from myuser where age > 30 and id < 8 and name like 'zhangsan'
         **/

        // 删除数据
        Delete delete = new Delete(Bytes.toBytes("Jack"));
        connection.getTable(tabName).delete(delete);
        System.out.println("Delete success");

        // 禁用+删除表
        if(admin.isTableDisabled(tabName)) {
            // admin.enableTable(tabName);
        } else {
            admin.disableTable(tabName);
        }
        admin.deleteTable(tabName);
    }
}
