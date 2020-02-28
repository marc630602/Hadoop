package dao;

import constants.Constants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * 1.发布微博
 * 2.关注用户
 * 3.取关用户
 */
public class HBaseDAO {

    //1.PUBLISH BLOG
    public static void publishWEIBO(String uid,String content) throws IOException {
        //CREATE CONNECT OBJECT
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //PART_1: MANIPULATING CONTENT_TABLE OF BLOG
        //1)INSTANCE CONTENT_TABLE OBJECT
        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        //2)GET CURRENT TIMESTAMP
        long ts = System.currentTimeMillis();

        //3)GET ROWKEY
        String rowKey = uid + "_" + ts;

        //4)INSTANCE PUT OBJECT FOR CONTENT TABLE
        Put contPut = new Put(Bytes.toBytes(rowKey));

        //5)ASSIGN VALUES TO PUT OBJECT
        contPut.addColumn(Bytes.toBytes(Constants.CONTENT_TABLE_COLUMNFAMILY),Bytes.toBytes("content"),Bytes.toBytes(content));

        //6)INSERT DATA
        contTable.put((contPut));

        //PART_2: MANIPULATING USER'S RELATION_TABLE OF BLOG (BATCH LIST SUBMISSION )
        //1)INSTANCE RELATION_TABLE OBJECT
        Table relaTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        //2)GET COLUMN FAMILY'S INFORMATION OF FANS OF WHICH SEND BLOGS
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes(Constants.RELATION_TABLE_COLUMNFAMILY2));
        Result result = relaTable.get(get);

        //3)CREATE A LIST OBJECT FOR STORING THE PUTS OF CONTENT TABLE
        ArrayList<Put> inboxPuts = new ArrayList<Put>();

        //4)READ FANS'S INFORMATION IN LOOP
        for (Cell cell : result.rawCells()) {
            //5)INSTANCE PUT OBJECT FOR INBOX TABLE
            Put inboxPut = new Put(CellUtil.cloneQualifier(cell));

            //6)ASSIGN VALUES TO PUT OBJECT
            inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_COLUMNFAMILY),Bytes.toBytes(uid),Bytes.toBytes(rowKey));

            //7)PUT PUT OBJECTS INTO INBOXPUTS
            inboxPuts.add(inboxPut);
        }

        //8)DETERMINE IF THERE IS FANS
        if (inboxPuts.size() > 0){
            //INSTANCE INBOXTABLE OBJECT
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));

            //INSERT DATA INTO INBOXTABLE
            inboxTable.put(inboxPuts);

            //CLOSE RESOURCE OF INBOXTABLE
            inboxTable.close();
        }

        //9)CLOSE OTHERS RESOURCES
        relaTable.close();
        contTable.close();
        connection.close();
    }


    //2.FOLLOW USERS（A FOLLOWS bCD）
    public static void addattend(String uid, String ... attends) throws IOException {
        //DETERMINE WHETHER SOMEONE HAS BEEN ADDED
        if (attends.length <= 0){
            System.out.println("Please select the person to be followed");
            return ;
        }
        //INSTANCE CONNECTION OBJECT
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //INSTANCE RELATABLE OBJECT
        Table relaTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        //PART_1 : FOLLOWER (A) 's WATCHLIST ADD FOLLOWER INFORMATION(BCD) & BEEN FOLLOWER'S FANS ADD FOLLOWER'S INFORMATION
        //1)CREATE OBJECT OF PUT LIST
        ArrayList<Put> attendPuts = new ArrayList<Put>();

        //2)INSTANCE PUT OBJECT OF FOLLOWERS
        Put uidPut = new Put(Bytes.toBytes(uid));

        for (String attend : attends) {
            //3)ASSIGN VALUES TO PUT OBJECT OF FOLLOWERS
            uidPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_COLUMNFAMILY1),Bytes.toBytes(attend),Bytes.toBytes(attend));
            //4)INSTANCE PUT OBEJCT OF BEEN-FOLLOWERS
            Put attendPut = new Put(Bytes.toBytes(attend));
            //5)ASSIGN VALUES TO PUT OBJECT OF BEEN-FOLLOWERS
            attendPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_COLUMNFAMILY2),Bytes.toBytes(uid),Bytes.toBytes(uid));
            //3)ADD PUT OBJECT INTO PUT LIST ATTENDPUTS
            attendPuts.add(attendPut);
        }

        //4)ADD PUT OBJECT OF FOLLOWERS INTO PUT LIST ATTENDPUTS
        attendPuts.add(uidPut);

        //5)INSERT DATA INTO RETABLE
        relaTable.put(attendPuts);


        //Part2:ADD THE THREE ROWKEYS (CONTENT TABLE) OF BEEN-FOLLOWER TO THE FOLLOWER'S INBOX (INBOX TABLE)

        //1)INSTANCE CONTENT OBEJCT
        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        //2)INSTANCE PUT OBJECT OF INBOX
        Put inboxPut = new Put(Bytes.toBytes(uid));

        //3)GET INFORMATION OF EACH FOLLOWER IN A LOOP
        for (String attend : attends) {

            //4）GET THE RECENT BLOGS (SCAN)-> COLLECTION LIST
            Scan scan = new Scan(Bytes.toBytes(attend+"_"),Bytes.toBytes(attend+"|"));
            ResultScanner results = contTable.getScanner(scan);

            //GET TIMESTAMP
            long ts = System.currentTimeMillis();
            //5)RECYCLE GET INFORMATION OF RESULT
            for (Result result : results) {
                //6）ASSIGN VALUES TO INBOXPUT
                inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_COLUMNFAMILY),Bytes.toBytes(attend),ts++,result.getRow());
            }
        }
        //7)DETERMINE IF CURRENT PUT OBJECT IS EMPTY
        if (!inboxPut.isEmpty()){
            //INSTANCE INBOXTABLE OBJECT
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
            //INSERT DATA INTO INBOXTABLE
            inboxTable.put(inboxPut);
            //CLOSE INBOXTABLE RESOURCE
            inboxTable.close();
        }
        //CLOSE OTHERS RESOURCES
        relaTable.close();
        contTable.close();
        connection.close();
    }


    //3.UNFOLLOW USERS
    public static void deleteattends(String uid,String ... dels) throws IOException {
        //DETERMINE IF UNFOLLOWERS HAS BEEN ADDED
        if (dels.length <= 0){
            System.out.println("PLEASE SELECT WHO YOU WANT TO UNFOLLOW");
            return ;
        }
        //INSTANCE CONNECTION OBJECT
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //INSTANCE RELATABLE OBJECT
        Table relaTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        //INSTANCE INBOXTABLE OBJECT
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));

        //1)CREATE DELETES LIST FOR RELATABLE
        ArrayList<Delete> deletes = new ArrayList<Delete>();

        //2)INSTANCE DELETE OBJECT FOR RELATABLE
        Delete uidDelete = new Delete(Bytes.toBytes(uid));

        //3)INSTANCE DELETE OBJECT FOR INBOXTABLE
        Delete inboxDelete = new Delete(Bytes.toBytes(uid));

        for (String del : dels) {
            //4)ASSIGN VALUES TO UIDDELETE OBJECT
            uidDelete.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_COLUMNFAMILY1),Bytes.toBytes(del));
            //5)INSTANCE DELETE OBJECT FOR BEEN-UNFOLLOWERS
            Delete deleteDel = new Delete(Bytes.toBytes(del));
            //6)ASSIGN VALUES TO DELETEDEL OBJECT
            deleteDel.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_COLUMNFAMILY2),Bytes.toBytes(uid));
            //7)PUT DELETEDEL OBJECT INTO DELETE LIST
            deletes.add(deleteDel);
            //8)DELETE THE BLOG POST INFORMATION OF THE FOLLOWERS INBOX FOR EACH OF THE GATEKEEPERS
            inboxDelete.addColumns(Bytes.toBytes(Constants.INBOX_TABLE_COLUMNFAMILY),Bytes.toBytes(del));
        }

        //9)PUT UIDDELETE OBJECT INTO DELETE LIST
        deletes.add(uidDelete);

        //10)DELETE DATA FROM RETABLE
        relaTable.delete(deletes);

        //11)DELETE DATA FROM INBOXTABLE
        inboxTable.delete(inboxDelete);

        //CLOSE REOURCES
        relaTable.close();
        inboxTable.close();
        connection.close();
    }

}
