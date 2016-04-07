package Service;

import DBConnect.ESConnect;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.json.JSONArray;

import java.util.Map;

/**
 * Created by Neil on 4/7/16.
 */
public class SearchSortByDate {


    /**
     * Search by Date time
     * @param ES
     * @param ES_INDEX
     * @param ES_TYPE
     * */
    public void SearchSoryByDate(ESConnect ES, String ES_INDEX, String ES_TYPE){
        JSONArray jsonArray = new JSONArray();
        String create_time = "create_time";
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.must(QueryBuilders.termQuery("author_name", "Bidy"));

        SearchResponse scrollResp = ES.getClient().prepareSearch(ES_INDEX)
                .setTypes(ES_TYPE)
                .setScroll(new TimeValue(60000))
                .setQuery(queryBuilder)
                .setSize(50)  // scroll Get 50 item data each time
                //set sort order createtime
                .addSort((create_time), SortOrder.DESC)  //ascending order
                .execute()
                .actionGet();

        while (true){
            for (SearchHit hit : scrollResp.getHits().getHits()){
                // the retrieved document to map
                Map<String,Object> result_map = hit.getSource();
                System.out.println("model_id: "+result_map.get("model_id")+", | create_time: "+result_map.get("create_time")+", | model_name: "+result_map.get("model_name"));

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
