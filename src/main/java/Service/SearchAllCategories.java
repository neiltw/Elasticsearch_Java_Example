package Service;

import DBConnect.ESConnect;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Map;

/**
 * Created by Neil on 4/7/16.
 */
public class SearchAllCategories {

    /**
     * search filter categories in all database
     * @param ES
     * @param ES_INDEX
     * @param ES_TYPE
     * */
    public void SearchAllCategories(ESConnect ES , String ES_INDEX, String ES_TYPE){
        JSONArray jsonArray = new JSONArray();
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.must(QueryBuilders.matchAllQuery());

        SearchResponse scrollResp = ES.getClient().prepareSearch(ES_INDEX)
                .setTypes(ES_TYPE)
                .setScroll(new TimeValue(60000))
                .setQuery(queryBuilder)
                .setSize(50)  // scroll Get 50 item data each time
                .execute()
                .actionGet();

        while (true){
            for (SearchHit hit : scrollResp.getHits().getHits()){
                //Choose a case
                //1. the retrieved document  String
                String result_str = hit.getSource().toString();
                System.out.println("String: "+result_str);

                //2. save model_id to JsonArray
                String result_model_id = hit.getSource().get("model_id").toString();
                JSONObject json = new JSONObject();
                json.put("model_id",result_model_id);
                jsonArray.put(json);
                System.out.println("JSONArray: "+jsonArray);

                //3. the retrieved document to map
                Map<String,Object> result_map = hit.getSource();
                System.out.println("Map: "+result_map.get("model_id"));
                System.out.println("");

            }
            scrollResp = ES.getClient().prepareSearchScroll(scrollResp.getScrollId())  // if there data scroll
                    .setScroll(new TimeValue(60000))
                    .execute()
                    .actionGet();
            if (scrollResp.getHits().getHits().length == 0) {
                break;  //not data break
            }
        }
    }
}
