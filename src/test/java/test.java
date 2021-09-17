import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lec.RestHighLevelClient.MyRestHighLevelClient;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.*;

public class test {

    public static void main(String[] args) throws IOException {
        JSONObject input = new JSONObject();
        JSONArray params = new JSONArray();
        for (int i = 0; i < 2; i++) {
            Map<String, Object> test = new HashMap<String, Object>();
            test.put("labelId", "666");
            params.add(new JSONObject(test));
        }

        input.put("params", params);
        System.out.println(input.toJSONString());
//        Map<String, String> map = new HashMap<>();
//        JSONArray jsonArray = new JSONArray();
//
//
//        int i = 0;
//        while (i < 100) {
//            JSONObject jsonObject = new JSONObject();
//            jsonObject.put(String.valueOf(System.currentTimeMillis()), "张三丰价格就会立刻就会更方便车厢内");
//            jsonObject.put(String.valueOf(System.currentTimeMillis() + 1), "张三丰34(⊙o⊙)…天3特容易让通话");
//            jsonObject.put(String.valueOf(System.currentTimeMillis() + 2), "张三丰答复是大啊");
//            jsonObject.put(String.valueOf(System.currentTimeMillis() + 3), "张三丰大法官反倒是官方都是");
//            jsonObject.put(String.valueOf(System.currentTimeMillis() + 4), "张三丰豆腐干豆腐事故发生的股份大使馆反倒是官方");
//            jsonObject.put(String.valueOf(System.currentTimeMillis() + 5), "张三丰集团与接电话远距离人员花港饭店");
//            jsonArray.add(jsonObject);
////            map.put(String.valueOf(i), "张三丰");
//            i++;
//        }
//        System.out.println(jsonArray.toJSONString());


//        Jedis jedis = new Jedis("9.1.10.228", 6379);
//        jedis.auth("Primeton@123");
//        Pipeline pipelined = jedis.pipelined();


//
//        for (int i = 10000; i < 20000; i++) {
//            String key = String.valueOf(i);
//            String value = "你的激发你的看到你发的能否防盗门";
//            jedis.set(key, value);
//            jedis.expire(key, 6000);
//        }

//        Map<String, Response<String>> responseMap = new HashMap<>();
//        long a = System.currentTimeMillis();
//        for (int i = 10000; i < 20000; i++) {
//            responseMap.put(i + "", pipelined.get(i + ""));
//        }
//        pipelined.sync();
//        long b = System.currentTimeMillis();
//        System.out.println( b-a);
//        String scrollId;
//
//        RestHighLevelClient highLevelClient = new MyRestHighLevelClient();
//        List<String> arrayList = new ArrayList<>();
//        try {
//            final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
//            SearchRequest searchRequest = new SearchRequest("h_por_pscst_id_rtrvl");
//            searchRequest.scroll(scroll);
//            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//            searchSourceBuilder.query(matchAllQuery());
//            searchSourceBuilder.size(10000);
//
////            searchSourceBuilder.query(idsQuery().addIds());
//
//            searchRequest.source(searchSourceBuilder);
//
//
//            SearchResponse searchResponse = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//            scrollId = searchResponse.getScrollId();
//            SearchHit[] searchHits = searchResponse.getHits().getHits();
//
//            while (searchHits != null && searchHits.length > 0) {
//                iter(searchHits, arrayList);
//                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
//                scrollRequest.scroll(scroll);
//                searchResponse = highLevelClient.scroll(scrollRequest, RequestOptions.DEFAULT);
//                scrollId = searchResponse.getScrollId();
//                searchHits = searchResponse.getHits().getHits();
//                break;
//
//            }
//
//            IdsQueryBuilder idsQueryBuilder=  QueryBuilders.idsQuery();
//            for (int i = 0; i < arrayList.size(); i++) {
//                idsQueryBuilder.addIds(arrayList.get(i));
//            }
//
//            searchSourceBuilder.query(idsQueryBuilder);
//            searchSourceBuilder.size(10000);
//            searchRequest.source(searchSourceBuilder);
//
//
//            searchResponse = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//            scrollId = searchResponse.getScrollId();
//        long a=    System.nanoTime();
//            searchHits = searchResponse.getHits().getHits();
//        long b=  System.nanoTime()-a;
//
//            while (searchHits != null && searchHits.length > 0) {
//                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
//                scrollRequest.scroll(scroll);
//                searchResponse = highLevelClient.scroll(scrollRequest, RequestOptions.DEFAULT);
//                scrollId = searchResponse.getScrollId();
//
//               Iterator iterator= Arrays.stream(searchHits).iterator();
//               while (iterator.hasNext()){
//                 SearchHit  searchHit= (SearchHit) iterator.next();
//                 Map<String, Object> map=searchHit.getSourceAsMap();
//                   System.out.println(iterator.next());
//               }
//            }
//
//
//        } catch (Exception e) {
//            e.printStackTrace();
//
//        }
//
//
//        highLevelClient.close();

    }

    public static void iter(SearchHit[] searchHits, List<String> list) {
        for (int i = 0; i < searchHits.length; i++) {
            list.add(searchHits[i].getId());

        }


    }


}
