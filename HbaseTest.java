import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTest {
    private static Configuration config=null;
    static{
        config = HBaseConfiguration.create();
        config.addResource("core-site.xml");
        config.addResource("hbase-site.xml");
        config.addResource("hdfs-site.xml");
        config.set("hbase.zookeeper.quorum", "localhost");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        }


    public static void createTable(String tableName)
            throws Exception{
        try{
        HBaseAdmin hBaseAdmin = new HBaseAdmin(config);
        if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
            System.out.println(tableName + " is exist,detele....");
        }
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        tableDescriptor.addFamily(new HColumnDescriptor("ID"));
        tableDescriptor.addFamily(new HColumnDescriptor("Description"));
        tableDescriptor.addFamily(new HColumnDescriptor("Courses"));
        tableDescriptor.addFamily(new HColumnDescriptor("Home"));
        hBaseAdmin.createTable(tableDescriptor);
        System.out.println("create table"+tableName+"success!");

    } catch (IOException e) {
        e.printStackTrace();
    }
        System.out.println("end create table ......");
}

    public static void addData(String tableName, String rowKey, String family,
                               String qualifier, String value) throws Exception {
        try {
            /*Connection conn= ConnectionFactory.createConnection(config);*/
            HTable table =new HTable(config,tableName);
            Put put= new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),Bytes.toBytes(value));
            table.put(put);
            System.out.println("insert record success!");


        }catch(Exception e){e.printStackTrace();}
    }


        public static void main(String[] args) {
        try{
            String tablename="students";
            HbaseTest.createTable(tablename);
            HbaseTest.addData("students","001","Description","Name","Li Lei");
            HbaseTest.addData("students","001","Description","Height","176");
            HbaseTest.addData("students","001","Courses","Chinese","80");
            HbaseTest.addData("students","001","Courses","Maths","90");
            HbaseTest.addData("students","001","Courses","Physics","95");
            HbaseTest.addData("students","001","Home","Province","Zhejiang");
            HbaseTest.addData("students","002","Description","Name","Han Meimei");
            HbaseTest.addData("students","002","Description","Height","183");
            HbaseTest.addData("students","002","Courses","Chinese","88");
            HbaseTest.addData("students","002","Courses","Maths","77");
            HbaseTest.addData("students","002","Courses","Physics","66");
            HbaseTest.addData("students","002","Home","Province","Beijing");
            HbaseTest.addData("students","003","Description","Name","Xiao Ming");
            HbaseTest.addData("students","003","Description","Height","162");
            HbaseTest.addData("students","003","Courses","Chinese","90");
            HbaseTest.addData("students","003","Courses","Maths","90");
            HbaseTest.addData("students","003","Courses","Physics","90");
            HbaseTest.addData("students","003","Home","Province","Shanghai");



        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
