package com.itcanteen.es.kafka;

import com.itcanteen.es.vo.BinLogKafkaData;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * @author baimugudu
 * @email 2415621370@qq.com
 * @date 2019/9/18 9:33
 */

@Component
@Slf4j
public class EsSyncData {

    @Autowired
    private TransportClient client;


    //{"after":[{"user_status":"0","update_time":"Thu Jan 01 08:00:00 IRKT 1970",
// "create_time":"Thu Jan 01 08:00:00 IRKT 1970",
// "id":"30","username":"21ww","token":"2122323232"}],"eventType":"UpdateRowsEventData","tableName":"ad_user"}
    public  void add(String tableName,BinLogKafkaData binLogKafkaData) throws IOException {
        if (tableName.equals("creative_unit")) {
            for (int i = 0; i < binLogKafkaData.getAfter().size(); i++) {
                XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                        .field("creative_id", binLogKafkaData.getAfter().get(i).get("creative_id"))
                        .field("unit_id", binLogKafkaData.getAfter().get(i).get("unit_id"))
                        .field("id", binLogKafkaData.getAfter().get(i).get("id"))
                        .endObject();

                IndexResponse indexResponse = this.client.prepareIndex(
                        "ad", tableName, binLogKafkaData.getAfter().get(i).get("id")
                ).setSource(xContentBuilder).get();

                log.info("添加数据->{}", indexResponse.getResult().toString());
            }
        }
        //添加
        if (tableName.equals("ad_plan")) {
            for (int i = 0; i < binLogKafkaData.getAfter().size(); i++) {
                XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                        .field("user_id", binLogKafkaData.getAfter().get(i).get("user_id"))
                        .field("plan_name", binLogKafkaData.getAfter().get(i).get("plan_name"))
                        .field("plan_status", binLogKafkaData.getAfter().get(i).get("plan_status"))
                        .field("start_date", binLogKafkaData.getAfter().get(i).get("start_date"))
                        .field("end_date", binLogKafkaData.getAfter().get(i).get("end_date"))
                        .field("create_time", binLogKafkaData.getAfter().get(i).get("create_time"))
                        .field("update_time", binLogKafkaData.getAfter().get(i).get("update_time"))
                        .field("id", binLogKafkaData.getAfter().get(i).get("id"))
                        .endObject();

                IndexResponse indexResponse = this.client.prepareIndex(
                        "ad", tableName, binLogKafkaData.getAfter().get(i).get("id")
                ).setSource(xContentBuilder).get();

                log.info("添加数据->{}", indexResponse.getResult().toString());
            }
        }

        //添加
        if (tableName.equals("ad_unit")) {
            for (int i = 0; i < binLogKafkaData.getAfter().size(); i++) {
                XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                        .field("plan_id", binLogKafkaData.getAfter().get(i).get("plan_id"))
                        .field("unit_name", binLogKafkaData.getAfter().get(i).get("unit_name"))
                        .field("unit_status", binLogKafkaData.getAfter().get(i).get("unit_status"))
                        .field("position_type", binLogKafkaData.getAfter().get(i).get("position_type"))
                        .field("budget", binLogKafkaData.getAfter().get(i).get("budget"))
                        .field("create_time", binLogKafkaData.getAfter().get(i).get("create_time"))
                        .field("update_time", binLogKafkaData.getAfter().get(i).get("update_time"))
                        .field("id", binLogKafkaData.getAfter().get(i).get("id"))
                        .endObject();

                IndexResponse indexResponse = this.client.prepareIndex(
                        "ad", tableName, binLogKafkaData.getAfter().get(i).get("id")
                ).setSource(xContentBuilder).get();

                log.info("添加数据->{}", indexResponse.getResult().toString());
            }
        }

        //添加
        if (tableName.equals("ad_unit_it")) {
            for (int i = 0; i < binLogKafkaData.getAfter().size(); i++) {
                XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                        .field("unit_id", binLogKafkaData.getAfter().get(i).get("unit_id"))
                        .field("it_tag", binLogKafkaData.getAfter().get(i).get("it_tag"))
                        .field("id", binLogKafkaData.getAfter().get(i).get("id"))
                        .endObject();

                IndexResponse indexResponse = this.client.prepareIndex(
                        "ad", tableName, binLogKafkaData.getAfter().get(i).get("id")
                ).setSource(xContentBuilder).get();

                log.info("添加数据->{}", indexResponse.getResult().toString());
            }
        }

        //添加
        if (tableName.equals("ad_unit_keyword")) {
            for (int i = 0; i < binLogKafkaData.getAfter().size(); i++) {
                XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                        .field("unit_id", binLogKafkaData.getAfter().get(i).get("unit_id"))
                        .field("keyword", binLogKafkaData.getAfter().get(i).get("keyword"))
                        .field("id", binLogKafkaData.getAfter().get(i).get("id"))
                        .endObject();

                IndexResponse indexResponse = this.client.prepareIndex(
                        "ad", tableName, binLogKafkaData.getAfter().get(i).get("id")
                ).setSource(xContentBuilder).get();

                log.info("添加数据->{}", indexResponse.getResult().toString());
            }
        }

        //添加
        if(tableName.equals("ad_user")){
            for(int i=0;i<binLogKafkaData.getAfter().size();i++){
                XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                        .field("user_status", binLogKafkaData.getAfter().get(i).get("user_status"))
                        .field("update_time", binLogKafkaData.getAfter().get(i).get("update_time"))
                        .field("create_time", binLogKafkaData.getAfter().get(i).get("create_time"))
                        .field("username", binLogKafkaData.getAfter().get(i).get("username"))
                        .field("token",binLogKafkaData.getAfter().get(i).get("token"))
                        .field("id", binLogKafkaData.getAfter().get(i).get("id"))
                        .endObject();

                IndexResponse indexResponse = this.client.prepareIndex(
                        "ad", "ad_user",binLogKafkaData.getAfter().get(i).get("id")
                ).setSource(xContentBuilder).get();

                log.info("添加数据->{}",indexResponse.getResult().toString());
            }
        }

        //添加
        if (tableName.equals("ad_unit_district")) {
            for (int i = 0; i < binLogKafkaData.getAfter().size(); i++) {
                XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                        .field("unit_id", binLogKafkaData.getAfter().get(i).get("unit_id"))
                        .field("province", binLogKafkaData.getAfter().get(i).get("province"))
                        .field("city", binLogKafkaData.getAfter().get(i).get("city"))
                        .field("id", binLogKafkaData.getAfter().get(i).get("id"))
                        .endObject();

                IndexResponse indexResponse = this.client.prepareIndex(
                        "ad", tableName, binLogKafkaData.getAfter().get(i).get("id")
                ).setSource(xContentBuilder).get();

                log.info("添加数据->{}", indexResponse.getResult().toString());
            }
        }

        //添加
        if (tableName.equals("ad_creative")) {
            for (int i = 0; i < binLogKafkaData.getAfter().size(); i++) {
                XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                        .field("name", binLogKafkaData.getAfter().get(i).get("name"))
                        .field("type", binLogKafkaData.getAfter().get(i).get("type"))
                        .field("material_type", binLogKafkaData.getAfter().get(i).get("material_type"))
                        .field("height", binLogKafkaData.getAfter().get(i).get("height"))
                        .field("width", binLogKafkaData.getAfter().get(i).get("width"))
                        .field("size", binLogKafkaData.getAfter().get(i).get("size"))
                        .field("duration", binLogKafkaData.getAfter().get(i).get("duration"))
                        .field("audit_status", binLogKafkaData.getAfter().get(i).get("audit_status"))
                        .field("user_id", binLogKafkaData.getAfter().get(i).get("user_id"))
                        .field("url", binLogKafkaData.getAfter().get(i).get("url"))
                        .field("create_time", binLogKafkaData.getAfter().get(i).get("create_time"))
                        .field("update_time", binLogKafkaData.getAfter().get(i).get("update_time"))
                        .field("id", binLogKafkaData.getAfter().get(i).get("id"))
                        .endObject();

                IndexResponse indexResponse = this.client.prepareIndex(
                        "ad", tableName, binLogKafkaData.getAfter().get(i).get("id")
                ).setSource(xContentBuilder).get();

                log.info("添加数据->{}", indexResponse.getResult().toString());
            }
        }

    }



    //修改
    public void upd(String tableName,BinLogKafkaData binLogKafkaData){

        if (tableName.equals("ad_plan")) {
            binLogKafkaData.getAfter().forEach(i -> {
                UpdateRequest updateRequest = new UpdateRequest("ad", tableName, i.get("id"));

                //构建请求字段
                try {
                    XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
                    if (null != i.get("user_id")) {
                        xContentBuilder.field("user_id", i.get("user_id"));
                    }

                    if (null != i.get("plan_name")) {
                        xContentBuilder.field("plan_name", i.get("plan_name"));
                    }

                    if (null != i.get("plan_status")) {
                        xContentBuilder.field("plan_status", i.get("plan_status"));
                    }

                    if (null != i.get("start_date")) {
                        xContentBuilder.field("start_date", i.get("start_date"));
                    }

                    if (null != i.get("end_date")) {
                        xContentBuilder.field("end_date", i.get("end_date"));
                    }

                    if (null != i.get("create_time")) {
                        xContentBuilder.field("create_time", i.get("create_time"));
                    }

                    if (null != i.get("update_time")) {
                        xContentBuilder.field("update_time", i.get("update_time"));
                    }
                    xContentBuilder.endObject();
                    updateRequest.doc(xContentBuilder);

                    UpdateResponse updateResponse = this.client.update(updateRequest).get();
                    log.info("更新数据-》{}", updateResponse.getResult().toString());

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }

            });
        }

        if (tableName.equals("ad_unit")) {
            binLogKafkaData.getAfter().forEach(i -> {
                UpdateRequest updateRequest = new UpdateRequest("ad", tableName, i.get("id"));

                //构建请求字段
                try {
                    XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
                    if (null != i.get("plan_id")) {
                        xContentBuilder.field("plan_id", i.get("plan_id"));
                    }

                    if (null != i.get("unit_name")) {
                        xContentBuilder.field("unit_name", i.get("unit_name"));
                    }

                    if (null != i.get("unit_status")) {
                        xContentBuilder.field("unit_status", i.get("unit_status"));
                    }

                    if (null != i.get("position_type")) {
                        xContentBuilder.field("position_type", i.get("position_type"));
                    }

                    if (null != i.get("budget")) {
                        xContentBuilder.field("budget", i.get("budget"));
                    }

                    if (null != i.get("create_time")) {
                        xContentBuilder.field("create_time", i.get("create_time"));
                    }

                    if (null != i.get("update_time")) {
                        xContentBuilder.field("update_time", i.get("update_time"));
                    }
                    xContentBuilder.endObject();
                    updateRequest.doc(xContentBuilder);

                    UpdateResponse updateResponse = this.client.update(updateRequest).get();
                    log.info("更新数据-》{}", updateResponse.getResult().toString());

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }

            });
        }

        if (tableName.equals("ad_unit_it")) {
            binLogKafkaData.getAfter().forEach(i -> {
                UpdateRequest updateRequest = new UpdateRequest("ad", tableName, i.get("id"));

                //构建请求字段
                try {
                    XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
                    if (null != i.get("unit_id")) {
                        xContentBuilder.field("unit_id", i.get("unit_id"));
                    }

                    if (null != i.get("it_tag")) {
                        xContentBuilder.field("it_tag", i.get("it_tag"));
                    }

                    xContentBuilder.endObject();
                    updateRequest.doc(xContentBuilder);

                    UpdateResponse updateResponse = this.client.update(updateRequest).get();
                    log.info("更新数据-》{}", updateResponse.getResult().toString());

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }

            });
        }

        if (tableName.equals("ad_unit_keyword")) {
            binLogKafkaData.getAfter().forEach(i -> {
                UpdateRequest updateRequest = new UpdateRequest("ad", tableName, i.get("id"));

                //构建请求字段
                try {
                    XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
                    if (null != i.get("unit_id")) {
                        xContentBuilder.field("unit_id", i.get("unit_id"));
                    }

                    if (null != i.get("keyword")) {
                        xContentBuilder.field("keyword", i.get("keyword55X"));
                    }

                    xContentBuilder.endObject();
                    updateRequest.doc(xContentBuilder);

                    UpdateResponse updateResponse = this.client.update(updateRequest).get();
                    log.info("更新数据-》{}", updateResponse.getResult().toString());

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }

            });
        }


        if(tableName.equals("ad_user")){
            binLogKafkaData.getAfter().forEach(i-> {
                UpdateRequest updateRequest = new UpdateRequest("ad", tableName, i.get("id"));

                //构建请求字段
                try {
                    XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
                    if (null != i.get("user_status")) {
                        xContentBuilder.field("user_status", i.get("user_status"));
                    }

                    if (null != i.get("update_time")) {
                        xContentBuilder.field("update_time", i.get("update_time"));
                    }

                    if (null != i.get("create_time")) {
                        xContentBuilder.field("create_time", i.get("create_time"));
                    }

                    if (null != i.get("username")) {
                        xContentBuilder.field("username", i.get("username"));
                    }

                    if (null != i.get("token")) {
                        xContentBuilder.field("token", i.get("token"));
                    }
                    xContentBuilder.endObject();
                    updateRequest.doc(xContentBuilder);

                    UpdateResponse updateResponse = this.client.update(updateRequest).get();
                    log.info("更新数据-》{}", updateResponse.getResult().toString());

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }

            });
        }

        if (tableName.equals("ad_creative")) {
            binLogKafkaData.getAfter().forEach(i -> {
                UpdateRequest updateRequest = new UpdateRequest("ad", tableName, i.get("id"));

                //构建请求字段
                try {
                    XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
                    if (null != i.get("name")) {
                        xContentBuilder.field("name", i.get("name"));
                    }

                    if (null != i.get("type")) {
                        xContentBuilder.field("type", i.get("type"));
                    }

                    if (null != i.get("material_type")) {
                        xContentBuilder.field("material_type", i.get("material_type"));
                    }

                    if (null != i.get("height")) {
                        xContentBuilder.field("height", i.get("height"));
                    }

                    if (null != i.get("width")) {
                        xContentBuilder.field("width", i.get("width"));
                    }

                    if (null != i.get("size")) {
                        xContentBuilder.field("size", i.get("size"));
                    }

                    if (null != i.get("duration")) {
                        xContentBuilder.field("duration", i.get("duration"));
                    }

                    if (null != i.get("audit_status")) {
                        xContentBuilder.field("audit_status", i.get("audit_status"));
                    }

                    if (null != i.get("user_id")) {
                        xContentBuilder.field("user_id", i.get("user_id"));
                    }

                    if (null != i.get("url")) {
                        xContentBuilder.field("url", i.get("url"));
                    }

                    if (null != i.get("create_time")) {
                        xContentBuilder.field("create_time", i.get("create_time"));
                    }

                    if (null != i.get("update_time")) {
                        xContentBuilder.field("update_time", i.get("update_time"));
                    }

                    xContentBuilder.endObject();
                    updateRequest.doc(xContentBuilder);

                    UpdateResponse updateResponse = this.client.update(updateRequest).get();
                    log.info("更新数据-》{}", updateResponse.getResult().toString());

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }

            });
        }

        if (tableName.equals("ad_unit_keyword")) {
            binLogKafkaData.getAfter().forEach(i -> {
                UpdateRequest updateRequest = new UpdateRequest("ad", tableName, i.get("id"));

                //构建请求字段
                try {
                    XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
                    if (null != i.get("unit_id")) {
                        xContentBuilder.field("unit_id", i.get("unit_id"));
                    }

                    if (null != i.get("province")) {
                        xContentBuilder.field("province", i.get("province"));
                    }

                    if (null != i.get("city")) {
                        xContentBuilder.field("city", i.get("city"));
                    }

                    xContentBuilder.endObject();
                    updateRequest.doc(xContentBuilder);

                    UpdateResponse updateResponse = this.client.update(updateRequest).get();
                    log.info("更新数据-》{}", updateResponse.getResult().toString());

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }

            });
        }

        if (tableName.equals("creative_unit")) {
            binLogKafkaData.getAfter().forEach(i -> {
                UpdateRequest updateRequest = new UpdateRequest("ad", tableName, i.get("id"));

                //构建请求字段
                try {
                    XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
                    if (null != i.get("creative_id")) {
                        xContentBuilder.field("creative_id", i.get("creative_id"));
                    }

                    if (null != i.get("unit_id")) {
                        xContentBuilder.field("unit_id", i.get("unit_id"));
                    }

                    xContentBuilder.endObject();
                    updateRequest.doc(xContentBuilder);

                    UpdateResponse updateResponse = this.client.update(updateRequest).get();
                    log.info("更新数据-》{}", updateResponse.getResult().toString());

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }

            });
        }

    }


    public void del(String tableName,BinLogKafkaData binLogKafkaData){
        //删除
        binLogKafkaData.getAfter().forEach(i->{
            DeleteRequestBuilder deleteRequestBuilder = this.client.prepareDelete("ad", tableName, i.get("id"));
            DeleteResponse deleteResponse = deleteRequestBuilder.get();
            log.info("删除数据-》{}",deleteResponse.getResult().toString());
        });
    }

}
