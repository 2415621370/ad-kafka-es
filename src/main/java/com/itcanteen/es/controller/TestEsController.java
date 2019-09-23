package com.itcanteen.es.controller;

import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.management.Query;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author baimugudu
 * @email 2415621370@qq.com
 * @date 2019/9/15 15:16
 */

@RestController
public class TestEsController {


    @Autowired
    private TransportClient client;


    /**
     * 向ES server 添加数据
     *
     * @param title
     * @param author
     * @param id
     * @return
     * @throws IOException
     */
    @PostMapping("/add/book/novel")
    public ResponseEntity add(String title
            , String author,String id) throws IOException {

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field("title", title)
                .field("author", author)
                .field("id", id)
                .endObject();

        IndexResponse indexResponse = this.client.prepareIndex(
                "book", "novel",id
        ).setSource(xContentBuilder).get();
        return new ResponseEntity(indexResponse.getId(), HttpStatus.OK);

    }


    /**
     * 更新数据
     * @param id
     * @param title
     * @param author
     * @return
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */

    @PostMapping("/update/book/novel")
    public ResponseEntity update(String id,String title,String author) throws IOException, ExecutionException, InterruptedException {
        //构建请求对象
        UpdateRequest updateRequest = new UpdateRequest("book", "novel", id);

        //构建请求字段
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();

        if(null!=title){
            xContentBuilder.field("title",title);
        }

        if(null!=author){
            xContentBuilder.field("author",author);
        }

        xContentBuilder.endObject();
        updateRequest.doc(xContentBuilder);

        UpdateResponse updateResponse = this.client.update(updateRequest).get();
        return new ResponseEntity(updateResponse.getResult().toString(),HttpStatus.OK);
    }



    @PostMapping("/add/book/novels")
    public ResponseEntity add_a(String title
            , String author) throws IOException {

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field("title", title)
                .field("author", author)
                .endObject();

        IndexResponse indexResponse = this.client.prepareIndex(
                "book", "novel"
        ).setSource(xContentBuilder).get();
        return new ResponseEntity(indexResponse.getId(), HttpStatus.OK);

    }


    /**
     * 获取数据
     * @return
     */

    @GetMapping("/get/book/novels")
    public ResponseEntity get(String id){

        GetRequestBuilder getRequestBuilder = this.client.prepareGet("ad", "ad_user", id);

        GetResponse response = getRequestBuilder.get();
        if(response.isExists()){
            return new ResponseEntity(response.getSource(),HttpStatus.OK);
        }

        return new ResponseEntity(HttpStatus.NOT_FOUND);
    }


    /**
     * 高级搜索、复杂搜索
     */
    @GetMapping("/query/book/novels")
    public ResponseEntity query(String title,String author){
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        if(null!=title){
            boolQueryBuilder.must(QueryBuilders.matchQuery("title",title));
        }

        if(null!=author){
            boolQueryBuilder.must(QueryBuilders.matchQuery("author",author));
        }


        SearchRequestBuilder searchRequestBuilder = this.client.prepareSearch("book")
                .setTypes("novel")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQueryBuilder);

       SearchResponse searchResponse = searchRequestBuilder.get();


        SearchHits hits = searchResponse.getHits();

        ArrayList<Map<String, Object>> list = new ArrayList<Map<String, Object>>();


        int length = hits.getHits().length;
        for(int i=0;i<length;i++){
            System.out.println( hits.getAt(i).getSource());
            Map<String, Object> source = hits.getAt(i).getSource();
            list.add(source);
        }
        return new ResponseEntity(list,HttpStatus.OK);
    }

}
