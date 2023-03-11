package com.example.esdemo;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.TermVectorsRequest;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * @author :cp
 * @description：RestHighLevelClient Test
 * 概念描述：
 * 1._doc 元数据：
 *  1)._source:内容主题，存储的数据
 *  2)._index:从属的索引名称（对应表名）
 *  3)._id:Doc里的唯一编号
 * 注意：
 * 1.进行聚合查询时。注意keyword这类没分词的可以,text会分词，查询时会报错
 */
@SpringBootTest
public class EsDemoApplicationTests {
    private RestHighLevelClient client;
    @Before
    //Es链接配置
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
    //创建一条数据
    public void creatOneDocument() throws IOException {
        /*
        * 1.不指定id则自动创建
        * 2.id相同时，不会插入
        * */
        //这样写只新增了最后一条
        IndexRequest request = new IndexRequest("xsxx")
                .id("10").source("age",17,
                        "name","cffdsff","student_id","002")
                .id("12").source("age",19,
                        "name","sdf","student_id","004")
                .id("13").source("age",17,
                        "name","sdfdsf","student_id","005",
                        "class_info.name","class1")
                .id("14").source("age",18,
                        "name","afjko","student_id","004",
                        "class_info.name","class1",
                        "class_info.number",50)
                .id("15").source("age",19,
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
        DeleteRequest request = new DeleteRequest("student-info","1");

        DeleteResponse delete = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(delete);
    }
    @Test
    //根据id更新Document
    public void upDateDocument() throws IOException {
        UpdateRequest request = new UpdateRequest("student-info","2");
        request.doc("age",20,
                "name","giug","student_id","009",
                "class_info.name","class3");
        //有则改，没有数据则插入
        request.docAsUpsert(true);

        UpdateResponse update = client.update(request, RequestOptions.DEFAULT);
        System.out.println(update);
    }

    @Test
    //term向量 可以用于统计热点词
    public void termTest() throws IOException {

        //方式一:根据已有数据进行统计
//        TermVectorsRequest request = new TermVectorsRequest("xsxx", "1");
//        request.setFields("name");

        //方式二:检索要查找的值
        XContentBuilder docBuilder = XContentFactory.jsonBuilder();
        docBuilder.startObject().field("class_info.name","class3").endObject();
        TermVectorsRequest request = new TermVectorsRequest("xsxx", docBuilder);

        //打开term的static的统计 默认是关闭的

        request.setFieldStatistics(true);
        request.setTermStatistics(true);

        TermVectorsResponse response =
                client.termvectors(request, RequestOptions.DEFAULT);

        //基本信息
        System.out.println(response.getFound());
        System.out.println(response.getId());
        System.out.println(response.getIndex());

        //ES数据库6.x后已废弃
//        System.out.println(response.getType());

        //
        List<TermVectorsResponse.TermVector> termVectorsList = response.getTermVectorsList();
        for (TermVectorsResponse.TermVector termVector : termVectorsList) {
            System.out.println("***************************");
            System.out.println(termVector.getFieldName());

            TermVectorsResponse.TermVector.FieldStatistics statistics = termVector.getFieldStatistics();
            //相关术语，统计出现次数
            System.out.println(statistics.getDocCount());
            System.out.println(statistics.getSumDocFreq());
            System.out.println(statistics.getSumTotalTermFreq());


            List<TermVectorsResponse.TermVector.Term> terms = termVector.getTerms();
            for (TermVectorsResponse.TermVector.Term term : terms) {
                System.out.println("***************************");
                System.out.println("term的属性值是:" + term.getTerm());
                System.out.println(term.getTermFreq());
                //输出的term是近期的 没有实时更新
                System.out.println(term.getTotalTermFreq());
                System.out.println(term.getDocFreq());
            }


        }

    }

    @Test
    //聚合查询
    public void aggTest() throws IOException {
        //构造查询
        SearchRequest request = new SearchRequest("xsxx");
        SearchSourceBuilder builder = new SearchSourceBuilder();

        //设置聚合条件 先group by 然后sum计算
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("by_clazz")
                .field("class_info.number");
        aggregation.subAggregation(AggregationBuilders.avg("average_age")
                .field("age"));
        //2.1 设置查询条件(单条件，多条件可以使用布尔查询器)
        builder.query(QueryBuilders.matchAllQuery())
                .aggregation(aggregation);

        //2.3 设置返回的字段有哪些
        String[] includes = new String[]{"age","name"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext =
                new FetchSourceContext(true, includes, excludes);
        builder.fetchSource(includes,excludes);

        request.source(builder);

        //2.3 链接数据库查表
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        System.out.println("*****************************");

        //3 获取查询结果
        System.out.println(response.getHits().getTotalHits());
        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        // 4.1.根据聚合名称获取聚合结果
        Aggregations aggregations = response.getAggregations();

        Terms clazz = aggregations.get("by_clazz");
        // 4.2.获取buckets
        List<? extends Terms.Bucket> buckets = clazz.getBuckets();
        // 4.3.遍历
        List<String> brandList = new ArrayList<>();
        for (Terms.Bucket bucket : buckets) {
            System.out.println(bucket.toString());
            Avg averageAge = bucket.getAggregations().get("average_age");
            System.out.println(bucket.getDocCount());
            System.out.println("avg:" + averageAge.value());
            // 4.4.获取key
            String key = bucket.getKeyAsString();
            System.out.println(key);
            brandList.add(key);
        }
    }

    @Test
    //updateByQuery；deleteQuery同理
    public void updateByQuery() throws IOException {
        UpdateByQueryRequest request = new UpdateByQueryRequest("xsxx");
        //1. 设置query条件
        request.setQuery(QueryBuilders.termQuery(
                "age",17));
        //2. 设置更新值 ctx._source['要修改的字段名']=要修改为的参数;
        request.setScript(new Script(
                "ctx._source['class_info.number']=45;"));
        request.setRefresh(true);
        BulkByScrollResponse bulkResponse =
                client.updateByQuery(request, RequestOptions.DEFAULT);
    }


    @After
    //释放链接
    public void closeES() throws IOException {
        client.close();
    }

}
