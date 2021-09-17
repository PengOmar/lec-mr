import com.lec.common.utils.HbaseUtils;
import com.lec.common.utils.KerberosLoginUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.User;

import java.io.IOException;
import java.util.concurrent.Executors;

public class Hbase {
    private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
    private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com";
    public static void main(String[] args) throws IOException {
        Connection connection = null;
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.addResource("hbase-site.xml");
        conf.addResource("yarn-site.xml");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        if (User.isHBaseSecurityEnabled(conf)) {

            String userName = "etl";
            //chMod 777 etl.keytab
            String userKeytabFile = "C:\\home\\userapp\\apps\\etl.keytab";
            //chMod 777 krb5.conF
            String krb5File = "C:\\home\\userapp\\apps\\krb5.conf";
            KerberosLoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userName, userKeytabFile);
            KerberosLoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY,
                    ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
            KerberosLoginUtil.login(userName, userKeytabFile, krb5File, conf);
            //与HBase数据库的连接对象
            connection = ConnectionFactory.createConnection(conf, Executors.newCachedThreadPool());
        }
        HbaseUtils hbaseUtils = new HbaseUtils(connection);
        String rowkey = StringUtils.reverse("690000259936616") + StringUtils.reverse("27050115");
        hbaseUtils.getRow("lec:por_rtcus_lbl_data_archt",rowkey);
        hbaseUtils.putDataColumn("lec:por_rtcus_lbl_data_archt",rowkey,"c","PN_00001","0");
        connection.close();
        System.exit(0);
    }
}
