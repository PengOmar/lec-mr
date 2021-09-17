import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

public class EsUtil {

    private RestHighLevelClient highLevelClient;

    public EsUtil(RestHighLevelClient highLevelClient) {
        this.highLevelClient = highLevelClient;
    }

    public Map<String, Object> docMap(String index, List<String> idList) throws IOException {
        Map<String, Object> docMap = new HashMap<>();
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(matchAllQuery());
        searchSourceBuilder.size(10000);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        for (int i = 0; i < searchHits.length; i++) {
            docMap.putAll(searchHits[i].getSourceAsMap());
        }
        return docMap;

    }
}
