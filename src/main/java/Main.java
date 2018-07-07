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
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class Main {


    static final String test_index = "test_index";
    static final String index_hotel = "hotel";

    static final RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")));

    public static void main(String[] args) throws Exception {

//        sample_code();
//        kore();
        real_sample();

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

        HistogramAggregationBuilder aggregation = AggregationBuilders.histogram("by_created");
        aggregation.field("created").interval(600000);

        aggregation.subAggregation(AggregationBuilders.terms("by_country").field("country.keyword").size(5));


        searchSourceBuilder.aggregation(aggregation);



        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse;
        searchResponse = client.search(searchRequest);
        Aggregations aggregations = searchResponse.getAggregations();

//        List<Aggregation> aggregationList = aggregations.asList();
//
//        for ( Aggregation a : aggregationList) {
//
//        }

//
        Map<String, Aggregation> aggregationMap = aggregations.getAsMap();
        Histogram histogramAggregation = (Histogram) aggregationMap.get("by_created");



//
        for ( var naibu : histogramAggregation.getBuckets()) {
            var what = ((ParsedStringTerms) naibu.getAggregations().asList().get(0)).getBuckets();

            what.stream().map(eee -> (eee).getKeyAsString()).forEach(System.out::println);

//            what.map(a -> a.getKeyAsString()).forEach(System.out::println);
//
//            for (var aaa : what) {
//               String tiger = aaa.getKeyAsString();
//               System.out.println(tiger);
//            }
//            what.stream().map(a -> ((Terms.Bucket) a).getAggregations())
//                    .collect(Collectors.toList()).forEach(System.out::println);


            var gg = 9;
        }
//        var all_people = histogramAggregation.getBuckets().stream()
//                .map(family -> (family).getAggregations());
//
//
//        var kore =  histogramAggregation.getBuckets().stream().map(b -> (b).getAggregations()).collect(Collectors.toList());
//
//
//
//
//        List<Aggregations> kore2 =  histogramAggregation.getBuckets().stream().map(b -> ((Histogram.Bucket) b).getAggregations()).collect(Collectors.toList());

//
//        for (var agg : kore2) {
//            var un = agg.asList().get(0);
//
//            var vva = un.getName().().collect(Collectors.toList());
//            var gg = 44;
//        }
//
//        var gg = kore2.stream().map(result -> result.asList())
//                .flatMap(List::stream)
//                .collect(Collectors.toList());

//        var gg1 = kore2.stream().map(result -> (T))

        List<String> uris = new ArrayList<>();
        uris.add("you ");
        uris.add("you2 ");
        Stream<String> stream4 = uris.stream().map(uri -> do_something(uri));
        List<String> korea = stream4.collect(Collectors.toList());


    uris.stream().map( uri -> do_something(uri)).forEach(System.out::println);

//    var ggaaaa = kore2.stream().map(b -> (Terms.Bucket) b).


        int gga = 9;

        // this won't work
//        var kore3 =  histogramAggregation.getBuckets().stream().map(b -> ((Histogram.Bucket) b).getAggregations())
//                .collect(Collectors.toList()).stream().map(c -> ((Terms.Bucket) c).getAggregations()).collect(Collectors.toList());




//        stream3.forEach(System.out::println);
//        aggregationList.stream().map(b -> {"b.3.bucket"});

        int rr = 3;



    }

    public static String do_something(String input) {
        return input + " something";
    }


    public static void real_sample() throws Exception{

        List<Person> soejima = new ArrayList<>();
        soejima.add(new Person("haru", 3));
        soejima.add(new Person("pu", 37));
        soejima.add(new Person("tomo", 41));

        List<Person> tanaka = new ArrayList<>();
        tanaka.add(new Person("don", 3));
        tanaka.add(new Person("raita", 37));
        tanaka.add(new Person("ken", 41));

        List<Person> yamada = new ArrayList<>();
        yamada.add(new Person("mii", 3));
        yamada.add(new Person("yuki", 37));
        yamada.add(new Person("kaori", 41));

        List<Family> families = new ArrayList<>();
        families.add(new Family("soejima", soejima));
        families.add(new Family("tanaka", tanaka));
        families.add(new Family("yamada", yamada));




        var aas = families.get(0).members.stream().toArray();




        var all_people = families.stream()
                .map(family -> family.getMembers())
                .flatMap(Collection::stream)
                .map(Person::getName).collect(Collectors.toList());


        var ff = 9;

//        familyStream.forEach(System.out::println);
//       List<Family> families_list = kore.collect(Collectors.toList());




        kore();
        int gg = 9;
//        families.stream().flatMap(Collection::stream).collect(Collectors.toList());



    }


}
