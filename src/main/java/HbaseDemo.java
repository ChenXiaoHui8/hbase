import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

public class HbaseDemo {

    private Connection conn;//DML
    private Admin admin;//DDL

    @Before
    public void before() throws IOException {
        Configuration conf= HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","CentOs");
        conn= ConnectionFactory.createConnection(conf);
        admin=conn.getAdmin();
    }


    @Test
    public void testCreateNamespace() throws IOException {
        NamespaceDescriptor nd=NamespaceDescriptor.create("zpark").addConfiguration("author","zs").build();
        admin.createNamespace(nd);

    }


    @Test
    public void testDropNamespace() throws IOException {
        admin.deleteNamespace("baizhi");
    }


    @Test
    public void testListTables() throws IOException {

        TableName[] tablesName=admin.listTableNames("baizhi:t_.*");

        for (TableName tableName : tablesName) {
            System.out.println(tableName);
        }
    }


    @Test
    public void testNamespaceTables() throws IOException {
        TableName[] tbs=admin.listTableNamesByNamespace("baizhi");
        for (TableName tb : tbs) {
            System.out.println(tb.getNameAsString());
        }

    }


    @Test
    public void testCreateTables() throws IOException {
        TableName tname=TableName.valueOf("zpark:t_user");
        HTableDescriptor table=new HTableDescriptor(tname);

        HColumnDescriptor cf1=new HColumnDescriptor("cf1");
        cf1.setMaxVersions(3);

        HColumnDescriptor cf2=new HColumnDescriptor("cf2");
        cf2.setTimeToLive(120);
        cf2.setInMemory(true);
        cf2.setBloomFilterType(BloomType.ROWCOL);
        table.addFamily(cf1);
        table.addFamily(cf2);
        admin.createTable(table);

    }


    @Test
    public void testPut01() throws IOException {
        Table table=conn.getTable(TableName.valueOf("zpark:t_user"));
        String[] company={"www.baidu.com","www.sina.com"};
        for(int i=0;i<1000;i++){
            String com=company[new Random().nextInt(2)];
            String rowKey=com+":";
            if(i<10){
                rowKey+="00"+i;
            }else if (i<100){
                rowKey+="0"+i;
            }else if(i<1000){
                rowKey+=""+i;
            }
            Put put=new Put(rowKey.getBytes());
            put.addColumn("cf1".getBytes(),"name".getBytes(),("user"+i).getBytes());
            put.addColumn("cf1".getBytes(),"age".getBytes(), Bytes.toBytes(i));
            put.addColumn("cf1".getBytes(),"salary".getBytes(),Bytes.toBytes(5000.0+1000*i));
            put.addColumn("cf1".getBytes(),"company".getBytes(),com.getBytes());

            table.put(put);

        }
        table.close();
    }


}
