package com.example.esdemo;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest
public class EsDemoApplicationTests {
    private RestHighLevelClient client;
    @Before
    public void connectES() {
        client  = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.153.128", 9200, "http")));
    }

    @Test
    //新建索引
    public void createOneIndex() throws IOException {
        //1.创建索引
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("student-info");//创建"student-info"索引

        /*
        //设置索引shards和replicas值
        createIndexRequest.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 2)
        );
        */

        //2.索引属性设置
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                //student_id
                builder.startObject("student_id");
                {
                    builder.field("type", "keyword");
                }
                builder.endObject();

                //name
                builder.startObject("name");
                {
                    builder.field("type", "text");
                }
                builder.endObject();

                //age
                builder.startObject("age");
                {
                    builder.field("type", "integer");
                }
                builder.endObject();

                //class_info
                builder.startObject("class_info");
                {
                    builder.startObject("properties");
                    {
                        //name
                        builder.startObject("name");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                        //number
                        builder.startObject("number");
                        {
                            builder.field("type", "integer");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        createIndexRequest.mapping(builder);

        //3.设置别名
        createIndexRequest.alias(new Alias("xsxx"));

        //4.链接数据库
        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);

    }

    @Test
    public void creatOneDocument() throws IOException {
        /*
        * 1.不指定id则自动创建
        * 2.id相同时，不会插入
        * */
        IndexRequest request = new IndexRequest("xsxx")
                .id("1").source("age",17,
                        "name","cffdsff","student_id","002")
                .id("2").source("age",19,
                        "name","sdf","student_id","004")
                .id("3").source("age",17,
                        "name","sdfdsf","student_id","005",
                        "class_info.name","class1")
                .id("4").source("age",18,
                        "name","afjko","student_id","004",
                        "class_info.name","class1",
                        "class_info.number",50)
                .id("5").source("age",19,
                "name","afjkofs","student_id","006",
                "class_info.name","class1",
                "class_info.number",50);
        client.index(request, RequestOptions.DEFAULT);
    }

    @Test
    //根据表名index,数据id读取数据
    public void getDocument() throws IOException {
        GetRequest getRequest = new GetRequest("student-info","5");

        getRequest.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);

        //设置返回的字段有哪些
        String[] includes = new String[]{"age","name"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext =
                new FetchSourceContext(true, includes, excludes);
        getRequest.fetchSourceContext(fetchSourceContext);

        /*
        //用这种方式会出Bug,不知道为啥？？
        getRequest.storedFields("name");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(getResponse.getField("name"));
        */
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        //效果等价，底层代码逻辑一致
        System.out.println(getResponse.getSource());
        System.out.println(getResponse.getSourceAsMap());


    }
    @Test
    //根据id判断是否存在
    public void hasDocument() throws IOException {
        GetRequest getRequest = new GetRequest("student-info","5");

        //因为只是判断是否存在
        getRequest.fetchSourceContext(new FetchSourceContext(false));

        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }
    @Test
    //根据id删除Document
    public void deleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest("student-info","3");

        DeleteResponse delete = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(delete);
    }
    @Test
    //根据id更新Document
    public void upDateDocument() throws IOException {
        UpdateRequest request = new UpdateRequest("student-info","1");
        request.doc("age",20,
                "name","ihi","student_id","002",
                "class_info.name","class2");
        //有则改，没有数据则插入
        request.docAsUpsert(true);

        UpdateResponse update = client.update(request, RequestOptions.DEFAULT);
        System.out.println(update);
    }

    @After
    public void closeES() throws IOException {
        client.close();
    }

}
