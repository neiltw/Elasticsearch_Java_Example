package Service;

import DBConnect.ESConnect;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Map;

/**
 * Created by Neil on 4/7/16.
 */
public class SearchCategoryIDAndAverageScore {
    /**
     *  search filter category id "1" and average_score "2" to "5" score and create time 2016-3-1 to 2016-3-15 time
     * @param ES
     * @param ES_INDEX
     * @param ES_TYPE
     * */
    public void SearchCategoryAndAverageScore(ESConnect ES, String ES_INDEX, String ES_TYPE){
        JSONArray jsonArray = new JSONArray();
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        // select category id "1"
        queryBuilder.must(QueryBuilders.termQuery("category_id",1));
        // select average_score "2" to "5"
        queryBuilder.must(QueryBuilders.rangeQuery("average_score")
                .from(2)
                .to(5)
                .includeLower(true)
                .includeUpper(true));    //true included "3" , false not include "3"
        // select create time 2016-3-1 to 2016-3-15
        queryBuilder.must(QueryBuilders.rangeQuery("create_time")
                .from("2016-03-01 00:00:00.0")
                .to("2016-03-15 12:59:00.0")
                .includeLower(true)
                .includeUpper(true));

        SearchResponse response = ES.getClient().prepareSearch(ES_INDEX)
                .setTypes(ES_TYPE)
                .setQuery(queryBuilder)
                .execute()
                .actionGet();

        System.out.println("Get :"+ response.getHits().getTotalHits()+" Item data");

        SearchHits hits = response.getHits();
        //get data
        for (SearchHit hit: hits) {
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
    }

}
