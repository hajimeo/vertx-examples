package io.vertx.example.jdbc.streaming;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.example.util.Runner;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.SQLRowStream;

/*
 * @author <a href="mailto:pmlopes@gmail.com">Paulo Lopes</a>
 */
public class JDBCExample extends AbstractVerticle {

  // Convenience method so you can run it in your IDE
  public static void main(String[] args) {
    Runner.runExample(JDBCExample.class);
  }

  @Override
  public void start() throws Exception {

    final JDBCClient client = JDBCClient.createShared(vertx, new JsonObject()
      .put("url", "jdbc:hive2://atscale731:11111/")
      .put("driver_class", "org.apache.hive.jdbc.HiveDriver")
      .put("max_pool_size", 5)
      .put("user", "admin")
      .put("password", "admin"));

    client.getConnection(conn -> {
      if (conn.failed()) {
        System.err.println(conn.cause().getMessage());
        return;
      }

      // Not asynchronous way but just looping
      for (int i = 0; i < 40; i++) {
        final SQLConnection connection = conn.result();
        connection.queryStream("SELECT Gender, SUM(orderquantity1) AS q FROM `Sales Insights`.`Internet Sales Cube` GROUP BY Gender", stream -> {
          if (stream.succeeded()) {
            SQLRowStream sqlRowStream = stream.result();

            sqlRowStream
              .handler(row -> {
                // do something with the row...
                System.out.println(row.encode());
              })
              .endHandler(v -> {
                // no more data available, close the connection
                connection.close(done -> {
                  if (done.failed()) {
                    throw new RuntimeException(done.cause());
                  }
                });
              });
          }
        });
      }
    });
  }
}
