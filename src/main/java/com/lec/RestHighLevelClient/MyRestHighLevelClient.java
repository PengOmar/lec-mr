package com.lec.RestHighLevelClient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MyRestHighLevelClient extends RestHighLevelClient {

    public static Properties properties = new Properties();
    private static final Log LOG = LogFactory.getLog(MyRestHighLevelClient.class);
    private static String isSecureMode;
    private static String esServerHost;
    private static int connectTimeout;
    private static int socketTimeout;
    private static int connectionRequestTimeout;
    private static int maxConnTotal;
    private static int maxConnPerRoute;
    private static String principal;
    private static RestClientBuilder builder;

    static {
        try {
            properties.load(MyRestHighLevelClient.class.getClassLoader().getResourceAsStream("es-rest-client-example.properties"));
            esServerHost = properties.getProperty("esServerHost");
            isSecureMode = properties.getProperty("isSecureMode");
            connectTimeout = Integer.parseInt(properties.getProperty("connectTimeout"));
            socketTimeout = Integer.parseInt(properties.getProperty("socketTimeout"));
            connectionRequestTimeout = Integer.parseInt(properties.getProperty("connectionRequestTimeout"));
            maxConnTotal = Integer.parseInt(properties.getProperty("maxConnTotal"));
            maxConnPerRoute = Integer.parseInt(properties.getProperty("maxConnPerRoute"));
            principal = properties.getProperty("principal");
            builder = getBuilder();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public MyRestHighLevelClient() {
        super(builder);
    }

    public static RestClientBuilder getBuilder() {
        String schema;
        if ("false".equals(properties.getProperty("isSecureMode"))) {
            schema = "http";
        } else {
            schema = "https";
        }

        List<HttpHost> hosts = new ArrayList();
        String[] hostArray1 = esServerHost.split(",");
        String[] var4 = hostArray1;
        int var5 = hostArray1.length;

        for (int var6 = 0; var6 < var5; ++var6) {
            String host = var4[var6];
            String[] ipPort = host.split(":");
            HttpHost hostNew = new HttpHost(ipPort[0], Integer.parseInt(ipPort[1]), schema);
            hosts.add(hostNew);
        }

        HttpHost[] hostArray = (HttpHost[]) hosts.toArray(new HttpHost[0]);

        if ("false".equals(isSecureMode)) {
            System.setProperty("es.security.indication", "false");
        } else {
//            try {
//                LOG.info("Config path is " + this.configPath);
//                LoginUtil.setJaasFile(principal, this.configPath + "user.keytab", customJaasPath);
//                LoginUtil.setKrb5Config(this.configPath + "krb5.conf");
//                System.setProperty("elasticsearch.kerberos.jaas.appname", "EsClient");
//                System.setProperty("es.security.indication", "true");
//                LOG.info("es.security.indication is  " + System.getProperty("es.security.indication"));
//            } catch (Exception var2) {
//                LOG.error("Failed to set security conf", var2);
//            }
        }

        RestClientBuilder builder = RestClient.builder(hostArray);
        Header[] defaultHeaders = new Header[]{new BasicHeader("Accept", "application/json"), new BasicHeader("Content-type", "application/json")};
        builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                return requestConfigBuilder.setConnectTimeout(connectTimeout).setSocketTimeout(socketTimeout).setConnectionRequestTimeout(connectionRequestTimeout);
            }
        });
        builder.setDefaultHeaders(defaultHeaders);
        return builder;
    }
}
