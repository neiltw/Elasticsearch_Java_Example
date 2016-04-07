package Controller;

import DBConnect.ESConnect;
import Service.*;

/**
 * Created by Neil on 4/6/16.
 */
public class SearchMain {

    private static ESConnect esclient = new ESConnect();

    public static void main (String[] args){

        String CLUSTER_NAME = "elasticsearch";              //ES ClusterName
        String ELASTICSEARCH_HOST = "localhost" ;           //ES host
        String ES_INDEX = "test";                           //specific elasticsearch index
        String ES_TYPE = "model";                           //specific elasticsearch type

        //setting es clustName , ip address
        esclient.EsConnect(CLUSTER_NAME, ELASTICSEARCH_HOST);

        SearchCategoryIDAndAverageScore id_and_ave = new SearchCategoryIDAndAverageScore();
        id_and_ave.SearchCategoryAndAverageScore(esclient,ES_INDEX,ES_TYPE);

        SearchRange range = new SearchRange();
        range.SearchCategoriesRange(esclient,ES_INDEX,ES_TYPE);

        SearchSortByDate Bydata = new SearchSortByDate();
        Bydata.SearchSoryByDate(esclient,ES_INDEX,ES_TYPE);

        SearchTermFilter TermFilter = new SearchTermFilter();
        TermFilter.SearchCategoriesTerm(esclient,ES_INDEX,ES_TYPE);

        SearchAllCategories Alldata = new SearchAllCategories();
        Alldata.SearchAllCategories(esclient,ES_INDEX,ES_TYPE);

        esclient.CloseClient(); //close es client
    }
}
