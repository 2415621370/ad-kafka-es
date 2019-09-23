package com.itcanteen.es.controller;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author baimugudu
 * @email 2415621370@qq.com
 * @date 2019/9/18 16:56
 */


@Controller
public class AdSearchController {

    @Autowired
    private TransportClient client;

    @RequestMapping("/seacrh/keyword")
    @ResponseBody
    public  List<Map<String, Object>> search(String keyword){
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if(null!=keyword){
            boolQueryBuilder.must(QueryBuilders.matchQuery("keyword",keyword));
        }

        SearchRequestBuilder searchRequestBuilder = this.client.prepareSearch("ad")
                .setTypes("ad_unit_keyword")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQueryBuilder);

        SearchResponse searchResponse = searchRequestBuilder.get();


        SearchHits hits = searchResponse.getHits();

        //SearchHits hits = searchResponse.getHits();

        ArrayList<Map<String, Object>> list = new ArrayList<Map<String, Object>>();


        int length = hits.getHits().length;
        for(int i=0;i<length;i++){
           // System.out.println( hits.getAt(i).getSource());
            Map<String, Object> source = hits.getAt(i).getSource();
            list.add(source);
        }

        return list;

    }


    @RequestMapping("/searchAdUnitByPositionType")
    @ResponseBody
    public List<Map<String, Object>> searchAdUnitByPositionType(Integer positionType){
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if(null!=positionType){
            boolQueryBuilder.must(QueryBuilders.termQuery("position_type",positionType));
        }



        SearchRequestBuilder searchRequestBuilder = this.client.prepareSearch("ad")
                .setTypes("ad_unit")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQueryBuilder);

        SearchResponse searchResponse = searchRequestBuilder.get();


        SearchHits hits = searchResponse.getHits();

        //SearchHits hits = searchResponse.getHits();

        ArrayList<Map<String, Object>> list = new ArrayList<Map<String, Object>>();


        int length = hits.getHits().length;
        for(int i=0;i<length;i++){
            // System.out.println( hits.getAt(i).getSource());
            Map<String, Object> source = hits.getAt(i).getSource();
            list.add(source);
        }

        return list;


    }



    @RequestMapping("/searchAdUnitByUnitId")
    @ResponseBody
    public List<Map<String, Object>> searchAdUnitByUnitId(Long unitId){
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if(null!=unitId){
            boolQueryBuilder.must(QueryBuilders.termQuery("id",unitId));
        }



        SearchRequestBuilder searchRequestBuilder = this.client.prepareSearch("ad")
                .setTypes("ad_unit")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQueryBuilder);

        SearchResponse searchResponse = searchRequestBuilder.get();


        SearchHits hits = searchResponse.getHits();

        //SearchHits hits = searchResponse.getHits();

        ArrayList<Map<String, Object>> list = new ArrayList<Map<String, Object>>();


        int length = hits.getHits().length;
        for(int i=0;i<length;i++){
            // System.out.println( hits.getAt(i).getSource());
            Map<String, Object> source = hits.getAt(i).getSource();
            list.add(source);
        }

        return list;


    }




    @RequestMapping("/searchByDistrict")
    @ResponseBody
    public List<Map<String, Object>> searchByDistrict(String provice,String city){
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if(null!=provice){
            boolQueryBuilder.must(QueryBuilders.termQuery("provice",provice));
        }

        if(null!=city){
            boolQueryBuilder.must(QueryBuilders.termQuery("city",city));
        }

        SearchRequestBuilder searchRequestBuilder = this.client.prepareSearch("ad")
                .setTypes("ad_unit_district")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQueryBuilder);

        SearchResponse searchResponse = searchRequestBuilder.get();


        SearchHits hits = searchResponse.getHits();

        //SearchHits hits = searchResponse.getHits();

        ArrayList<Map<String, Object>> list = new ArrayList<Map<String, Object>>();


        int length = hits.getHits().length;
        for(int i=0;i<length;i++){
            // System.out.println( hits.getAt(i).getSource());
            Map<String, Object> source = hits.getAt(i).getSource();
            list.add(source);
        }

        return list;


    }






    @RequestMapping("/searchByits")
    @ResponseBody
    public List<Map<String, Object>> searchByits(String its){
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if(null!=its){
            boolQueryBuilder.must(QueryBuilders.termQuery("it_tag",its));
        }



        SearchRequestBuilder searchRequestBuilder = this.client.prepareSearch("ad")
                .setTypes("ad_unit_it")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQueryBuilder);

        SearchResponse searchResponse = searchRequestBuilder.get();


        SearchHits hits = searchResponse.getHits();

        //SearchHits hits = searchResponse.getHits();

        ArrayList<Map<String, Object>> list = new ArrayList<Map<String, Object>>();


        int length = hits.getHits().length;
        for(int i=0;i<length;i++){
            // System.out.println( hits.getAt(i).getSource());
            Map<String, Object> source = hits.getAt(i).getSource();
            list.add(source);
        }

        return list;


    }



    @RequestMapping("/searchAdids")
    @ResponseBody
    public List<Map<String, Object>> searchAdids(Long unitId){
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if(null!=unitId){
            boolQueryBuilder.must(QueryBuilders.termQuery("unit_id",unitId));
        }



        SearchRequestBuilder searchRequestBuilder = this.client.prepareSearch("ad")
                .setTypes("creative_unit")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQueryBuilder);

        SearchResponse searchResponse = searchRequestBuilder.get();


        SearchHits hits = searchResponse.getHits();

        //SearchHits hits = searchResponse.getHits();

        ArrayList<Map<String, Object>> list = new ArrayList<Map<String, Object>>();


        int length = hits.getHits().length;
        for(int i=0;i<length;i++){
            // System.out.println( hits.getAt(i).getSource());
            Map<String, Object> source = hits.getAt(i).getSource();
            list.add(source);
        }

        return list;


    }





    @RequestMapping("/searchCreative")
    @ResponseBody
    public List<Map<String, Object>> searchCreative(Long creativeId){
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if(null!=creativeId){
            boolQueryBuilder.must(QueryBuilders.termQuery("id",creativeId));
        }

        SearchRequestBuilder searchRequestBuilder = this.client.prepareSearch("ad")
                .setTypes("ad_creative")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQueryBuilder);

        SearchResponse searchResponse = searchRequestBuilder.get();


        SearchHits hits = searchResponse.getHits();

        //SearchHits hits = searchResponse.getHits();

        ArrayList<Map<String, Object>> list = new ArrayList<Map<String, Object>>();


        int length = hits.getHits().length;
        for(int i=0;i<length;i++){
            // System.out.println( hits.getAt(i).getSource());
            Map<String, Object> source = hits.getAt(i).getSource();
            list.add(source);
        }

        return list;


    }








}
