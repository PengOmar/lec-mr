package com.lec.RestHighLevelClient;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class BulkProcessorUtil {

    public static final Logger LOG = LoggerFactory.getLogger(BulkProcessorUtil.class);

    /**
     * 数据条数达到1000时进行刷新操作
     */
    private int onceBulkMaxNum = 1000;


    /**
     * 数据量大小达到5M进行刷新操作
     */
    private int onecBulkMaxSize = 5;

    /**
     * 单个线程需要入库的总条数
     */
    private int totalNumberForThread = 20000;

    /**
     * 设置允许执行的并发请求数
     */
    private int concurrentRequestsNum = 5;

    /**
     * 设置刷新间隔时间，如果超过刷新时间则BulkRequest挂起
     */
    private int flushTime = 10;

    /**
     * 设置刷新间隔时间，如果超过刷新时间则BulkRequest挂起
     */
    private int maxRetry = 3;

    /**
     * 索引名
     */
    private String indexName = "bulkindex";

    /**
     * 默认索引类型, 不建议修改
     */
    private final String indexDefaultType = "_doc";

    /**
     * transport client
     */
    private RestHighLevelClient highLevelClient;

    /**
     * bulk processor
     */
    private BulkProcessor bulkProcessor;


    public BulkProcessorUtil(RestHighLevelClient highLevelClient, String indexName) {
        this.highLevelClient = highLevelClient;
        this.bulkProcessor = initBulkProcessor(highLevelClient);
        this.indexName = indexName;
    }

    public BulkProcessor getBulkProcessor() {
        return this.bulkProcessor;
    }

    /**
     * 生成bulkProcessor
     *
     * @return
     */
    private BulkProcessor initBulkProcessor(RestHighLevelClient highLevelClient) {
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest bulkRequest) {
                int numberOfActions = bulkRequest.numberOfActions();
                LOG.info("Executing bulk {} with {} requests.", executionId, numberOfActions);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                if (bulkResponse.hasFailures()) {
                    LOG.warn("Bulk {} executed with failures.", executionId);
                } else {
                    LOG.info("Bulk {} completed in {} milliseconds.", executionId, bulkResponse.getTook().getMillis());
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest bulkRequest, Throwable throwable) {
                LOG.error("Failed to execute bulk.", throwable);
            }
        };

        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer =
                (request, bulkListener) -> highLevelClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);

        BulkProcessor bulkProcessor = BulkProcessor.builder(bulkConsumer, listener)
                .setBulkActions(onceBulkMaxNum)
                .setBulkSize(new ByteSizeValue(onecBulkMaxSize, ByteSizeUnit.MB))
                .setConcurrentRequests(concurrentRequestsNum)
                .setFlushInterval(TimeValue.timeValueSeconds(flushTime))
                .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), maxRetry))
                .build();

        LOG.info("Init bulkProcess successfully.");

        return bulkProcessor;
    }


    /**
     * 单线程样例方法
     */
    public void singleThreadBulk() {
        //单线程
        int bulkTime = 0;
        while (bulkTime++ < totalNumberForThread) {
            Map<String, Object> dataMap = new HashMap<>();
            dataMap.put("date", "2019/12/9");
            dataMap.put("textbody", "the test text");
            dataMap.put("title", "the title");
            bulkProcessor.add(new IndexRequest(indexName, indexDefaultType).source(dataMap));
        }
        LOG.info("This thead bulks successfully, the thread name is {}.", Thread.currentThread().getName());
    }


    public void destroy() throws InterruptedException {

        //执行关闭方法会把bulk剩余的数据都写入ES再执行关闭
        bulkProcessor.awaitClose(30, TimeUnit.SECONDS);

    }

    /**
     * high level 客户端，判断索引是否存在
     *
     * @return 索引是否存在
     */
    public boolean isExistIndexForHighLevel() throws IOException {
        GetIndexRequest isExistsRequest = new GetIndexRequest(indexName);
        return highLevelClient.indices().exists(isExistsRequest, RequestOptions.DEFAULT);

    }

    public void deleteIndex() throws IOException {

        DeleteIndexRequest request = new DeleteIndexRequest(indexName);
        AcknowledgedResponse delateResponse = highLevelClient.indices().delete(request, RequestOptions.DEFAULT);
        if (delateResponse.isAcknowledged()) {
            LOG.info("Delete index is successful");
        } else {
            LOG.info("Delete index is failed");
        }

    }

    /**
     * high level rest 客户端创建索引
     *
     * @return 是否创建成功
     */
    public boolean createCpIndex() throws IOException {

        CreateIndexRequest indexRequest = new CreateIndexRequest(indexName);
        indexRequest.settings(Settings.builder()
                .put("number_of_shards", 60)
                .put("number_of_replicas", 2));


        indexRequest.mapping("{\n" +
                "    \"properties\": {\n" +
                "        \"uscc\": {\n" +
                "            \"type\": \"keyword\",\n" +
                "            \"index\": true\n" +
                "        },\n" +
                "        \"ecif_cust_no\": {\n" +
                "            \"type\": \"keyword\",\n" +
                "            \"index\": true\n" +
                "        },\n" +
                "        \"org_no\": {\n" +
                "            \"type\": \"keyword\",\n" +
                "            \"index\": true\n" +
                "        }\n" +
                "    }\n" +
                "}\n", XContentType.JSON);
        CreateIndexResponse response = highLevelClient.indices().create(indexRequest, RequestOptions.DEFAULT);
        if (response.isAcknowledged() || response.isShardsAcknowledged()) {
            LOG.info("Create index {} successful by high level client.", indexName);
            return true;
        }
        return false;

    }

    /**
     * high level rest 客户端创建索引
     *
     * @return 是否创建成功
     */
    public boolean createPsIndex() throws IOException {

        CreateIndexRequest indexRequest = new CreateIndexRequest(indexName);
        indexRequest.settings(Settings.builder()
                .put("number_of_shards", 60)
                .put("number_of_replicas", 2));


        indexRequest.mapping("{\n" +
                "    \"properties\": {\n" +
                "        \"crtf_nbr\": {\n" +
                "            \"type\": \"keyword\",\n" +
                "            \"index\": true\n" +
                "        },\n" +
                "        \"ctftp_cd\": {\n" +
                "            \"type\": \"keyword\",\n" +
                "            \"index\": true\n" +
                "        },\n" +
                "        \"ecif_cust_no\": {\n" +
                "            \"type\": \"keyword\",\n" +
                "            \"index\": true\n" +
                "        },\n" +
                "        \"org_no\": {\n" +
                "            \"type\": \"keyword\",\n" +
                "            \"index\": true\n" +
                "        }\n" +
                "    }\n" +
                "}", XContentType.JSON);
        CreateIndexResponse response = highLevelClient.indices().create(indexRequest, RequestOptions.DEFAULT);
        if (response.isAcknowledged() || response.isShardsAcknowledged()) {
            LOG.info("Create index {} successful by high level client.", indexName);
            return true;
        }

        return false;
    }

//
//    /**
//     * 主样例方法
//     *
//     * @param args
//     */
//    public static final void main(String[] args) {
//
//        RestHighLevelClient highLevelClient = null;
//        org.elasticsearch.action.bulk.BulkProcessor bulkProcessor = null;
//        BulkProcessor bulkProcessorSample = null;
//        try {
//            HwRestClient hwRestClient = new HwRestClient();
//            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
//            // 创建索引
//            if (!isExistIndexForHighLevel(highLevelClient)) {
////                createIndexForHighLevel(highLevelClient);
//            }
//
//            bulkProcessor = getBulkProcessor(highLevelClient);
//            bulkProcessorSample = new BulkProcessor(highLevelClient, bulkProcessor);
//
//
//            // 多线程样例
//            bulkProcessorSample.multiThreadBulk();
//            bulkProcessorSample.shutDownThreadPool();
//
//        } catch (Exception e) {
//            LOG.error("Do bulkProcessorSample failed.", e);
//            System.exit(-1);
//        } finally {
//            try {
//                if (bulkProcessor != null) {
//                    bulkProcessorSample.destroy();
//                }
//                if (highLevelClient != null) {
//                    highLevelClient.close();
//                }
//            } catch (IOException e) {
//                LOG.error("Failed to close RestHighLevelClient.", e);
//                System.exit(-1);
//            }
//        }
//
//    }

}
