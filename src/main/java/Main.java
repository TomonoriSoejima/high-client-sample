import org.apache.http.HttpHost;

import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.*;



public class Main {

    static final RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")));

    public static void main(String[] args) throws Exception {

        do_bulk();

    }

    public static void do_bulk() throws IOException {

            BulkRequest bulkRequest = new BulkRequest();

            for (int i = 1; i <= 3; i++) {
                Map<String, Object> jsonMap = new HashMap<>();
                jsonMap.put("field1", "value" + i);
                jsonMap.put("field2", "value" + i);
                jsonMap.put("field3", i);

                IndexRequest indexRequest = new IndexRequest("my_index")
                        .id(Integer.toString(i))
                        .source(jsonMap, XContentType.JSON);

                bulkRequest.add(indexRequest);
            }

            // Step 4: Execute the Bulk Request
            BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            ListTasksRequest tasksRequest = new ListTasksRequest().setDetailed(true);
            ListTasksResponse tasksResponse = client.tasks().list(tasksRequest,RequestOptions.DEFAULT);

            if (bulkResponse.hasFailures()) {
                System.out.println("Bulk request has failures: " + bulkResponse.buildFailureMessage());
            } else {
                System.out.println("Bulk request completed successfully.");
            }

            // Step 6: Close the Client
            client.close();
    }

}
