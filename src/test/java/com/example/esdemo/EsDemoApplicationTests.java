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
 * @description???RestHighLevelClient Test
 * ???????????????
 * 1._doc ????????????
 *  1)._source:??????????????????????????????
 *  2)._index:???????????????????????????????????????
 *  3)._id:Doc??????????????????
 * ?????????
 * 1.??????????????????????????????keyword????????????????????????,text??????????????????????????????
 */
@SpringBootTest
public class EsDemoApplicationTests {
    private RestHighLevelClient client;
    @Before
    //Es????????????
    public void connectES() {
        client  = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.153.128", 9200, "http")));
    }

    @Test
    //????????????
    public void createOneIndex() throws IOException {
        //1.????????????
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("student-info");//??????"student-info"??????

        /*
        //????????????shards???replicas???
        createIndexRequest.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 2)
        );
        */

        //2.??????????????????
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

        //3.????????????
        createIndexRequest.alias(new Alias("xsxx"));

        //4.???????????????
        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);

    }

    @Test
    //??????????????????
    public void creatOneDocument() throws IOException {
        /*
        * 1.?????????id???????????????
        * 2.id????????????????????????
        * */
        //?????????????????????????????????
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
    //????????????index,??????id????????????
    public void getDocument() throws IOException {
        GetRequest getRequest = new GetRequest("student-info","5");

        getRequest.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);

        //??????????????????????????????
        String[] includes = new String[]{"age","name"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext =
                new FetchSourceContext(true, includes, excludes);
        getRequest.fetchSourceContext(fetchSourceContext);

        /*
        //?????????????????????Bug,?????????????????????
        getRequest.storedFields("name");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(getResponse.getField("name"));
        */
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        //???????????????????????????????????????
        System.out.println(getResponse.getSource());
        System.out.println(getResponse.getSourceAsMap());


    }
    @Test
    //??????id??????????????????
    public void hasDocument() throws IOException {
        GetRequest getRequest = new GetRequest("student-info","5");

        //??????????????????????????????
        getRequest.fetchSourceContext(new FetchSourceContext(false));

        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }
    @Test
    //??????id??????Document
    public void deleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest("student-info","1");

        DeleteResponse delete = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(delete);
    }
    @Test
    //??????id??????Document
    public void upDateDocument() throws IOException {
        UpdateRequest request = new UpdateRequest("student-info","2");
        request.doc("age",20,
                "name","giug","student_id","009",
                "class_info.name","class3");
        //?????????????????????????????????
        request.docAsUpsert(true);

        UpdateResponse update = client.update(request, RequestOptions.DEFAULT);
        System.out.println(update);
    }

    @Test
    //term?????? ???????????????????????????
    public void termTest() throws IOException {

        //?????????:??????????????????????????????
//        TermVectorsRequest request = new TermVectorsRequest("xsxx", "1");
//        request.setFields("name");

        //?????????:?????????????????????
        XContentBuilder docBuilder = XContentFactory.jsonBuilder();
        docBuilder.startObject().field("class_info.name","class3").endObject();
        TermVectorsRequest request = new TermVectorsRequest("xsxx", docBuilder);

        //??????term???static????????? ??????????????????

        request.setFieldStatistics(true);
        request.setTermStatistics(true);

        TermVectorsResponse response =
                client.termvectors(request, RequestOptions.DEFAULT);

        //????????????
        System.out.println(response.getFound());
        System.out.println(response.getId());
        System.out.println(response.getIndex());

        //ES?????????6.x????????????
//        System.out.println(response.getType());

        //
        List<TermVectorsResponse.TermVector> termVectorsList = response.getTermVectorsList();
        for (TermVectorsResponse.TermVector termVector : termVectorsList) {
            System.out.println("***************************");
            System.out.println(termVector.getFieldName());

            TermVectorsResponse.TermVector.FieldStatistics statistics = termVector.getFieldStatistics();
            //?????????????????????????????????
            System.out.println(statistics.getDocCount());
            System.out.println(statistics.getSumDocFreq());
            System.out.println(statistics.getSumTotalTermFreq());


            List<TermVectorsResponse.TermVector.Term> terms = termVector.getTerms();
            for (TermVectorsResponse.TermVector.Term term : terms) {
                System.out.println("***************************");
                System.out.println("term???????????????:" + term.getTerm());
                System.out.println(term.getTermFreq());
                //?????????term???????????? ??????????????????
                System.out.println(term.getTotalTermFreq());
                System.out.println(term.getDocFreq());
            }


        }

    }

    @Test
    //????????????
    public void aggTest() throws IOException {
        //????????????
        SearchRequest request = new SearchRequest("xsxx");
        SearchSourceBuilder builder = new SearchSourceBuilder();

        //?????????????????? ???group by ??????sum??????
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("by_clazz")
                .field("class_info.number");
        aggregation.subAggregation(AggregationBuilders.avg("average_age")
                .field("age"));
        //2.1 ??????????????????(????????????????????????????????????????????????)
        builder.query(QueryBuilders.matchAllQuery())
                .aggregation(aggregation);

        //2.3 ??????????????????????????????
        String[] includes = new String[]{"age","name"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext =
                new FetchSourceContext(true, includes, excludes);
        builder.fetchSource(includes,excludes);

        request.source(builder);

        //2.3 ?????????????????????
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        System.out.println("*****************************");

        //3 ??????????????????
        System.out.println(response.getHits().getTotalHits());
        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        // 4.1.????????????????????????????????????
        Aggregations aggregations = response.getAggregations();

        Terms clazz = aggregations.get("by_clazz");
        // 4.2.??????buckets
        List<? extends Terms.Bucket> buckets = clazz.getBuckets();
        // 4.3.??????
        List<String> brandList = new ArrayList<>();
        for (Terms.Bucket bucket : buckets) {
            System.out.println(bucket.toString());
            Avg averageAge = bucket.getAggregations().get("average_age");
            System.out.println(bucket.getDocCount());
            System.out.println("avg:" + averageAge.value());
            // 4.4.??????key
            String key = bucket.getKeyAsString();
            System.out.println(key);
            brandList.add(key);
        }
    }

    @Test
    //updateByQuery???deleteQuery??????
    public void updateByQuery() throws IOException {
        UpdateByQueryRequest request = new UpdateByQueryRequest("xsxx");
        //1. ??????query??????
        request.setQuery(QueryBuilders.termQuery(
                "age",17));
        //2. ??????????????? ctx._source['?????????????????????']=?????????????????????;
        request.setScript(new Script(
                "ctx._source['class_info.number']=45;"));
        request.setRefresh(true);
        BulkByScrollResponse bulkResponse =
                client.updateByQuery(request, RequestOptions.DEFAULT);
    }


    @After
    //????????????
    public void closeES() throws IOException {
        client.close();
    }

}
