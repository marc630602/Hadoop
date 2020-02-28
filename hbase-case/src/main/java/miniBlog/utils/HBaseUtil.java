package utils;

import constants.Constants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * 1.CREATE NAMESPACE
 * 2.DETERMINE IF TABLE EXISTS
 * 3.CREATE TABLES
 */
public class HBaseUtil {

    //1.CREATE NAMESPACE
    public static void createNameSpace(String nameSpace) throws IOException {
        //1)INSTANCE CONNECTION OBJECT
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //2)INSTANCE ADMIN OBJECT
        Admin admin = connection.getAdmin();

        //3)INSTANCE NAMESPACE DESCRIPTOR OBJECT
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();

        //4)CREATE NAMESPACE
        admin.createNamespace(namespaceDescriptor);

        //5)CLOSE RESOURCES
        admin.close();
        connection.close();
    }

    //2.DETERMINE IF TABLE EXISTS
    public static boolean isTableExist(String tableName) throws IOException {
        //1)INSTANCE CONNECTION OBJECT
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //2)INSTANCE ADMIN OBJECT
        Admin admin = connection.getAdmin();

        //3)DETERMINE IF TABLE EXISTS
        boolean result = admin.tableExists(TableName.valueOf(tableName));

        //4)CLOSE RESOURCES
        admin.close();
        connection.close();

        //4)RETURN RESULT
        return result;
    }

    //3.CREATE TABLE
    public static void createTable(String tableName,int versions,String ... columnFamilys) throws IOException {
        //1)DETERMINE IF COLUMNS FAMILY'S INFORMATION ARE SENT
        if (columnFamilys.length <= 0){
            System.out.println("Please enter at least one column family information");
            return ;
        }

        //2)DETERMINE IF TABLE EXISTS
        if (!isTableExist(tableName)){
            System.out.println(tableName + "has exists");
            return ;
        }

        //3)INSTANCE CONNECTION OBJECT
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //4)INSTANCE ADMIN OBJECT
        Admin admin = connection.getAdmin();

        //5)INSTANCE NAMESPACE DESCRIPTOR OBJECT
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));

        for (String columnFamily : columnFamilys) {
            //6)INSTANCE COLUMN DESCRIPTOR OBJECT IN A LOOP
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);

            //7)SET MAX VERSIONS
            hColumnDescriptor.setMaxVersions(versions);

            //8)INSERT COLUMNS FAMILY'S INTO COLUMN DESCRIPTOR OBJECT
            descriptor.addFamily(hColumnDescriptor);
        }

        //7)CREATE TABLE
        admin.createTable(descriptor);

        //8)CLOSE RESOURCE
        admin.close();
        connection.close();
    }
}
