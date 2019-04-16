package io.vertx.example.jdbc.streaming;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.example.util.Runner;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.SQLRowStream;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/*
 * @author <a href="mailto:pmlopes@gmail.com">Paulo Lopes</a>
 */
public class JDBCExample extends AbstractVerticle {
  DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  // Convenience method so you can run it in your IDE
  public static void main(String[] args) {
    Runner.runExample(JDBCExample.class);
  }

  @Override
  public void start() throws Exception {

    final JDBCClient client = JDBCClient.createShared(vertx, new JsonObject()
      .put("url", "jdbc:hive2://atscale731:11111/")
      .put("driver_class", "org.apache.hive.jdbc.HiveDriver")
      .put("max_pool_size", 10)
      .put("user", "admin")
      .put("password", "admin"));

    for (int i = 0; i < 40; i++) {
      String s = Integer.toString(i); //variable used in lambda should be final or effectively final

      client.getConnection(conn -> {
        if (conn.failed()) {
          System.err.println("[" + dateFormat.format(new Date()) + "] " + conn.cause().getMessage());
          return;
        }

        // Not asynchronous way but just looping
        final SQLConnection connection = conn.result();
        connection.queryStream("SELECT Gender, SUM(orderquantity1) AS q FROM `Sales Insights`.`Internet Sales Cube` GROUP BY Gender", stream -> {
          if (stream.succeeded()) {
            System.err.println("[" + dateFormat.format(new Date()) + "] stream Success! " + s);
            SQLRowStream sqlRowStream = stream.result();

            sqlRowStream
              .handler(row -> {
                // do something with the row...
                System.out.println(row.toString());
              })
              .endHandler(v -> {
                // no more data available, close the connection
                connection.close(done -> {
                  if (done.failed()) {
                    throw new RuntimeException(done.cause());
                  }
                });
              });
          } else {
            System.err.println("[" + dateFormat.format(new Date()) + "] stream Failed! " + s);
          }
        });
      });
    }
  }
}
