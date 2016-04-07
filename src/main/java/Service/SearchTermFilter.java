package Service;

import DBConnect.ESConnect;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Map;

/**
 * Created by Neil on 4/7/16.
 */
public class SearchTermFilter {

    /**
     *  search multi filter category id
     *
     * @param ES        elasticsearch client
     * @param ES_INDEX  elasticsearch index
     * @param ES_TYPE   elasticssearch type
     * */
    public void SearchCategoriesTerm(ESConnect ES , String ES_INDEX, String ES_TYPE){

        JSONArray jsonArray =  new JSONArray();
        ArrayList filterArray = new ArrayList();
        filterArray.add(7);
        filterArray.add(8);

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        queryBuilder.must(QueryBuilders.termsQuery("category_id",filterArray)); //search category id "7" Tools and "8" Toys filter

        System.out.println("query SQL:" + queryBuilder);

        SearchResponse response = ES.getClient().prepareSearch(ES_INDEX)
                .setTypes(ES_TYPE)
                .setQuery(queryBuilder)
                .setExplain(true)
                .setSize(5)    //Get 5 item data each time. default Size 10, Size Max_value 10000 ,  more data use scroll
                .execute()
                .actionGet();

        System.out.println("get :"+ response.getHits().getTotalHits()+" Items");

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
