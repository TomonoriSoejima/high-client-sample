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
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.*;



public class Main {


    static final String test_index = "test_index";

    public static void main(String[] args) throws Exception {

        sample_code();
        System.exit(0);

        List kore = new ArrayList();
        kore.add("hello");


        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));

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



        System.exit(0);


    }

    public static void sample_code() throws Exception
    {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));

        SearchRequest searchRequest = new SearchRequest(test_index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse;
        searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();


        // key messageUID, value message-id
        Map<String, String> message_id_UID = new HashMap<String, String>();
        List<sb_object>  sb_objects = new LinkedList<sb_object>();
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
                message_id_UID.put(sb.getMessageUID(), sb.getMessageid());
            }
        }


        List<String> doc_ids;

        // find document with same messageUID
        for (String messageUID : message_id_UID.keySet())
        {


            System.out.println("searching by messageUID " + messageUID);

            doc_ids = search_by_messageUID(messageUID);
            for (String id_for_update : doc_ids ) {
                System.out.println("Found " + id_for_update);

            }
            update_message_id(message_id_UID.get(messageUID), doc_ids);


        }


    }

    // returns doc id who needs to be updated on message-id
    public static List<String> search_by_messageUID(String input) throws Exception
    {

        List<String> doc_ids  = new LinkedList<String>();
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));

//        SearchRequest searchRequest = new SearchRequest("test_index");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery("cmd", input));

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(test_index);
        searchRequest.source(sourceBuilder);
//        QueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("cmd", input);



//        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("cmd", input);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        searchSourceBuilder.query(sourceBuilder);
//        searchSourceBuilder.query(matchQueryBuilder);
        SearchResponse searchResponse;
        searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();

        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            doc_ids.add((hit.getId()));
        }

        return doc_ids;


    }


    public static void update_message_id(String message_id, List docids) throws Exception {


        Header header = new BasicHeader("a", "ba");

//        headers.add(new BasicHeader("User-Agent",userAgent));
        // create client


        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));

            for (Object docid : docids)
            {

                Map<String, Object> jsonMap = new HashMap<>();
                jsonMap.put("message-id", message_id);
                UpdateRequest request = new UpdateRequest(test_index, "test_table", docid.toString())
                        .doc(jsonMap);


                UpdateResponse updateResponse = client.update(request, header);


            }

            int aa = 9;
    }



}
