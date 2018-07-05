import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;




public class Main {


    static final String test_index = "test_index";
    static final String index_hotel = "hotel";

    static final RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")));

    public static void main(String[] args) throws Exception {

//        sample_code();
        kore();

    }

    public static void sample_code() throws Exception
    {


        SearchRequest searchRequest = new SearchRequest(test_index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse;
        searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();


        // key messageUID, value message-id
        // this is a list of document that contains non null messageUID

        List<sb_object> sb_objects = new LinkedList<sb_object>();
        List<sb_object> parent_data = new LinkedList<sb_object>();


        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            // do something with the SearchHit
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            sb_object sb = new sb_object();
            sb.setDocid( hit.getId());
            sb.setMessageUID( (String) sourceAsMap.get("messageUID"));
            sb.setMessageid( (String) sourceAsMap.get("message-id"));
            sb.setCmd( (String) sourceAsMap.get("cmd"));
            sb.setMsgid( (String) sourceAsMap.get("msgid"));
            sb.setSid( (String) sourceAsMap.get("sid"));

            sb_objects.add(sb);



            if (! sb.getMessageUID().startsWith("-") )
            {
                parent_data.add(sb);
            }
        }


        List<String> doc_ids;

        // find document with same messageUID
        for (sb_object sb : parent_data)
        {


            System.out.println("searching by messageUID " + sb.getMessageUID());

            doc_ids = search_by_messageUID(sb.getMessageUID());
            for (String id_for_update : doc_ids ) {
                System.out.println("Found " + id_for_update);

            }


            update_message_id(sb.getMessageid(), doc_ids);


            doc_ids = shori4(sb);
            for (String id_for_update : doc_ids ) {
                System.out.println("Found " + id_for_update);

            }
            update_message_id(sb.getMessageid(), doc_ids);


        }


    }

    // shori 2
    // returns doc id who needs to be updated on message-id
    public static List<String> search_by_messageUID(String input) throws Exception
    {

        List<String> doc_ids  = new LinkedList<>();

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery("cmd", input));

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(test_index);
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse;
        searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();

        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            doc_ids.add((hit.getId()));
        }

        return doc_ids;


    }


    // shori 3
    public static void update_message_id(String message_id, List docids) throws Exception {

        // making dummy header to accommodate  to this change.
        // https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking_70_restclient_changes.html
        Header header = new BasicHeader("a", "a");


            for (Object docid : docids)
            {

                Map<String, Object> jsonMap = new HashMap<>();
                jsonMap.put("message-id", message_id);
                UpdateRequest request = new UpdateRequest(test_index, "test_table", docid.toString())
                        .doc(jsonMap);


                UpdateResponse updateResponse = client.update(request, header);
                System.out.println("UPDATED : message-id " + message_id + " on doc " + docid.toString());


            }

    }

//     - shori ４
//    取得したmessage-idに対して、msgidが同じ値のドキュメントを特定する
    public static List<String> shori4(sb_object sb) throws Exception{


        List<String> doc_ids  = new LinkedList<>();


        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        QueryBuilder termQueryBuilder = QueryBuilders.termQuery("msgid", sb.getMsgid());
        QueryBuilder termQueryBuilder2 = QueryBuilders.termQuery("message-id", "-");


        BoolQueryBuilder qb = new BoolQueryBuilder();
        qb.filter(termQueryBuilder).filter(termQueryBuilder2);

        searchSourceBuilder.query(qb);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(test_index);
        searchRequest.source(searchSourceBuilder);


        SearchResponse searchResponse;
        searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();

        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            doc_ids.add((hit.getId()));
        }

        return doc_ids;

    }


    public static void make_data() throws Exception{
        List kore = new ArrayList();
        kore.add("hello");




        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("user", "bbbb");
            builder.field("postDate", new Date());
            builder.field("message", "trying out Elasticsearch");
        }
        builder.endObject();
        IndexRequest indexRequest = new IndexRequest("posts", "doc", "2")
                .source(builder);

        IndexResponse indexResponse = client.index(indexRequest);
        String type = indexResponse.getType();
        String id = indexResponse.getId();
        long version = indexResponse.getVersion();
        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {

        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {

        }
        ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {

        }
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                String reason = failure.reason();
            }
        }


    }




    public static void kore() throws Exception{


        SearchRequest searchRequest = new SearchRequest(index_hotel);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        HistogramAggregationBuilder aggregation = AggregationBuilders.histogram("3");
        aggregation.field("created").interval(600000);

        aggregation.subAggregation(AggregationBuilders.terms("2").field("country.keyword").size(5));


        searchSourceBuilder.aggregation(aggregation);



        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse;
        searchResponse = client.search(searchRequest);
        Aggregations aggregations = searchResponse.getAggregations();

        List<Aggregation> aggregationList = aggregations.asList();
        var kore = aggregationList.get(0);

        var aaaa = aggregationList.toArray();

        Map<String, Aggregation> aggregationMap = aggregations.getAsMap();
        Histogram histogramAggregation = (Histogram) aggregationMap.get("3");

        histogramAggregation.getBuckets().stream().forEach(System.out::println);



//        aggregationList.stream().forEach(System.out::println);
//        aggregationList.stream().map(b -> {"b.3.bucket"});

        int rr = 3;



    }



}
