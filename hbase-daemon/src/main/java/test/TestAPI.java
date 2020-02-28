package test;
import com.google.inject.internal.util.$Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * DDL:
 * 1.判断表是否存在
 * 2.创建表
 * 3.创建命名空间
 * 4.删除表
 * DML：
 * 5.插入数据
 * 6.查询数据(get)
 * 7.查询数据(scan)
 * 8.删除数据
 */

public class TestAPI {

    private static Connection connection = null ;
    private static Admin admin = null ;
    static {
        try {
            //创建配置信息对象
            //HBaseConfiguration configuration = new HBaseConfiguration();
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","hadoop101,hadoop102,hadoop103");

            //创建客户端对象
            connection = ConnectionFactory.createConnection();

            //创建admin对象
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //1)判断表存是否在
    public static boolean isTableExist(String tableName) throws IOException {

        boolean exists = admin.tableExists(TableName.valueOf(tableName));

        return exists;
    };

    //2)创建表
    public static void createTable(String tableName, String ... cfs) throws IOException {
        //1.判断是否输入列族信息
        if (cfs.length <= 0){
            System.out.println("请输入至少一个列族信息");
            return;
        }
        //2.判断表是否存在
        if (isTableExist(tableName)){
            System.out.println(tableName+"表已存在");
            return;
        }
        //3.创建表描述器
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //4.循环添加列族信息
        for (String cf : cfs) {
            //5.创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            descriptor.addFamily(hColumnDescriptor);
        }
        //7.创建表
        admin.createTable(descriptor);

    }

    //3)删除表
    public static void dropTbale(String tableName) throws IOException {
        //1.判断表是否存在
        if (!isTableExist(tableName)){
            System.out.println(tableName+"表不存在");
        }

        //2.disable表
        admin.disableTable(TableName.valueOf(tableName));

        //3.删除表
        admin.deleteTable(TableName.valueOf(tableName));
    }

    //4)创建命名空间
    public static void createNameSpace(String ns){
        //1.创建命名空间描述器
        NamespaceDescriptor namespacedescriptor = NamespaceDescriptor.create(ns).build();
        //2.创建命名空间
        try {
            admin.createNamespace(namespacedescriptor);
        } catch (NamespaceExistException e){
            System.out.println(ns + "命名空间已存在");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    //5)向表内插入数据
    public static void putData(String tableName,String rowKey,String cf,String cn,String value) throws IOException {
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2.创建put对象（如果需要创建多个rowkey数据则需要创建多个put对象构成List<Put>）
        Put put = new Put(Bytes.toBytes(rowKey));

        //3.给put对象赋值 （如果需要给某个rowkey创建多个不同数据，则只需在对该put对象多次赋值）
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));

        //4.向table插入数据
        table.put(put);

        //5.关闭表连接
        table.close();
    }

    //6)向表中获取数据(get)
    public static void getData(String tableName,String rowKey,String cf,String cn) throws IOException {

        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2.创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));

        //2.1指定获取的列主
        get.addFamily(Bytes.toBytes(cf));

        //2.2指定获取的列族和列
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));

        //2.3设置获取最大版本数
        get.setMaxVersions(5);

        //3.获取数据
        Result result = table.get(get);

        //4.解析result并打印
        for (Cell cell : result.rawCells()) {
            System.out.println("Column Family: " + Bytes.toString(CellUtil.cloneFamily(cell)) + "，" +
                               "Column Name: "   + Bytes.toString(CellUtil.cloneQualifier(cell)) + "，" +
                               "Value :"         + Bytes.toString(CellUtil.cloneValue(cell)));
        }
        //5.关闭表连接
        table.close();
    }

    //7)向表中获取数据（scan）
    public static void scanTable(String tableName) throws IOException {
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2.创建扫描器对象
        Scan scan = new Scan();

        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析restulScanner
        for (Result result : resultScanner) {
            //5.解析result并打印
            for (Cell cell : result.rawCells()) {
                System.out.println("Column Family: " + Bytes.toString(CellUtil.cloneFamily(cell)) + "，" +
                        "Column Name: "   + Bytes.toString(CellUtil.cloneQualifier(cell)) + "，" +
                        "Value :"         + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

        //6.关闭资源
        table.close();
    }

    //8)删除数据
    public static void deleteData(String tableName,String rowkey,String cf,String cn) throws IOException {
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2.创建delete对象(指定rowkey)
        Delete delete = new Delete(Bytes.toBytes(rowkey));

        //2.1 设定删除的列（删除最新版本,如果数据还没flush，会新被覆盖的数据放出来；如果已经flush，则不会出来） 慎用！
        delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));

        //2.2 设定删除的列（删除所有版本）
        delete.addColumns(Bytes.toBytes(cf),Bytes.toBytes(cn));

        //2.3 设定删除的列族
        delete.addFamily(Bytes.toBytes(cf));

        //3.删除数据
        table.delete(delete);

        //4.关闭资源
        table.close();
    }

    //关闭资源
    public static void close(){
        if (admin != null){
            try{
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (connection != null){
            try{
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) throws IOException {
        //1.测试表是否存在
        System.out.println(isTableExist("stu"));

        //2.创建表测试
        createTable("stu10","info1","info2");

        //3.删除表测试
        dropTbale("stu10");

        //4.创建命名空间测试
        createNameSpace("0408");

        //5.创建数据测试
        putData("stu","1001","info","name","zhangsan");

        //6.获取数据测试(get)
        getData("stu","1001","","");

        //7.扫描数据测试(scan)
        scanTable("stu");

        //8.删除数据测试
        deleteData("stu","1001","info","name");

        //关闭资源
        close();
    }
}
