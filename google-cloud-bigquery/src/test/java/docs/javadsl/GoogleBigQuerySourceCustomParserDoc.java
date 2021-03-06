/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

// #custom-parser-imports
// Imports
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.http.javadsl.unmarshalling.Unmarshaller;
import akka.stream.Materializer;
import akka.stream.alpakka.googlecloud.bigquery.BigQueryConfig;
import akka.stream.alpakka.googlecloud.bigquery.client.ResponseJsonProtocol;
import akka.stream.alpakka.googlecloud.bigquery.javadsl.BigQueryCallbacks;
import akka.stream.alpakka.googlecloud.bigquery.javadsl.GoogleBigQuerySource;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
// End imports

// #custom-parser-imports

public class GoogleBigQuerySourceCustomParserDoc {

  // #custom-parser
  static class User {
    String uid;
    String name;

    User(String uid, String name) {
      this.uid = uid;
      this.name = name;
    }
  }

  static ObjectMapper objectMapper = new ObjectMapper();

  static Unmarshaller<ByteString, JsonNode> jsonUnmarshaller =
      Unmarshaller.sync(
          byteString -> {
            try {
              return objectMapper.readTree(byteString.toArray());
            } catch (IOException ex) {
              throw new RuntimeException(ex);
            }
          });

  static Unmarshaller<JsonNode, ResponseJsonProtocol.Response> responseUnmarshaller =
      Unmarshaller.sync(
          jsonNode -> {
            Optional<ResponseJsonProtocol.JobReference> jobReference =
                Optional.ofNullable(jsonNode.path("jobReference"))
                    .map(
                        jobReferenceJsonNode -> {
                          Optional<String> jobId =
                              Optional.ofNullable(jobReferenceJsonNode.path("jobId"))
                                  .map(JsonNode::textValue);
                          return ResponseJsonProtocol.createJobReference(jobId);
                        });

            Optional<String> pageToken =
                Optional.ofNullable(jsonNode.get("pageToken")).map(JsonNode::textValue);
            Optional<String> nextPageToken =
                Optional.ofNullable(jsonNode.get("nextPageToken")).map(JsonNode::textValue);

            Optional<Boolean> jobComplete =
                Optional.ofNullable(jsonNode.get("jobComplete")).map(JsonNode::booleanValue);

            return ResponseJsonProtocol.createResponse(
                jobReference, pageToken, nextPageToken, jobComplete);
          });

  static Unmarshaller<JsonNode, ResponseJsonProtocol.ResponseRows<User>> rowsUnmarshaller =
      Unmarshaller.sync(
          jsonNode -> {
            Optional<List<User>> rows =
                Optional.ofNullable(jsonNode.get("rows"))
                    .map(
                        rowsJsonNode -> {
                          List<User> users = new ArrayList(rowsJsonNode.size());
                          for (int i = 0; i < rowsJsonNode.size(); ++i) {
                            JsonNode userJsonNode = rowsJsonNode.get(i);
                            users.add(readUserFromTree(userJsonNode));
                          }

                          return users;
                        });

            return ResponseJsonProtocol.createResponseRows(rows);
          });

  static User readUserFromTree(JsonNode jsonNode) {
    JsonNode f = jsonNode.get("f");
    String uid = jsonNode.get(0).get("v").textValue();
    String name = jsonNode.get(1).get("v").textValue();
    return new User(uid, name);
  }

  private static Source<User, NotUsed> customParser() {
    ActorSystem system = ActorSystem.create();
    Materializer materializer = Materializer.createMaterializer(system);
    BigQueryConfig config =
        BigQueryConfig.create(
            "project@test.test",
            "privateKeyFromGoogle",
            "projectID",
            "bigQueryDatasetName",
            system);
    return GoogleBigQuerySource.runQuery(
        "SELECT uid, name FROM bigQueryDatasetName.myTable",
        rowsUnmarshaller,
        responseUnmarshaller,
        jsonUnmarshaller,
        BigQueryCallbacks.ignore(),
        config);
  }
  // #custom-parser

}
