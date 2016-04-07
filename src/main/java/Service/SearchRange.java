package Service;

import DBConnect.ESConnect;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Map;

/**
 * Created by Neil on 4/7/16.
 */
public class SearchRange {

    /**
     * range search filter 1 to 4 in category id
     * @param ES
     * @param ES_INDEX
     * @param ES_TYPE
     * */
    public void SearchCategoriesRange(ESConnect ES , String ES_INDEX, String ES_TYPE) {
        JSONArray jsonArray =  new JSONArray();
        String queryFilte = "category_id" ;
        QueryBuilder queryBuilder = QueryBuilders.rangeQuery(queryFilte)
                .gte("1")
                .to("3")
                .includeLower(true)
                .includeUpper(true); //true included "3" , false not include "3"

        System.out.println(queryBuilder);
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
