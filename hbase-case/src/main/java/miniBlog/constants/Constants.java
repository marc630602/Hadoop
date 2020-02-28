package constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class Constants {
    //HBASE CONFIGURATION
    public static Configuration CONFIGURATION = HBaseConfiguration.create();

    //CREATE NAMESPACE
    public static String NAMESPACE = "weibo";

    //TABLE_1 : CONTENT OF BLOG
    public static String CONTENT_TABLE = "weibo:content";
    public static String CONTENT_TABLE_COLUMNFAMILY = "info";
    public static int CONTENT_TABLE_VERSIONS = 1;

    //TABLE_2 : RELATION OF USERS
    public static String RELATION_TABLE = "weibo:relation";
    public static String RELATION_TABLE_COLUMNFAMILY1 = "attends";
    public static String RELATION_TABLE_COLUMNFAMILY2 = "fans";
    public static int RELATION_TABLE_VERSIONS = 1;

    //TABLE_3 : INBOX
    public static String INBOX_TABLE = "weibo:inbox";
    public static String INBOX_TABLE_COLUMNFAMILY = "info";
    public static int INBOX_TABLE_VERSIONS = 2;

}
