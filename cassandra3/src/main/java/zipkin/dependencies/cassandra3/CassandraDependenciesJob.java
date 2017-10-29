/**
 * Copyright 2016-2017 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.dependencies.cassandra3;

import com.datastax.driver.mapping.annotations.Column;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import zipkin2.DependencyLink;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;
import static zipkin.internal.Util.checkNotNull;
import static zipkin.internal.Util.midnightUTC;

public final class CassandraDependenciesJob {
  private static final Logger log = LoggerFactory.getLogger(CassandraDependenciesJob.class);

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {

    final Map<String, String> sparkProperties = new LinkedHashMap<>();
    String keyspace = getEnv("CASSANDRA_KEYSPACE", "zipkin2");
    String contactPoints = getEnv("CASSANDRA_CONTACT_POINTS", "localhost");
    String localDc = getEnv("CASSANDRA_LOCAL_DC", null);
    // local[*] master lets us run & test the job locally without setting a Spark cluster
    String sparkMaster = getEnv("SPARK_MASTER", "local[*]");
    // needed when not in local mode
    String[] jars;
    Runnable logInitializer;
    // By default the job only works on traces whose first timestamp is today
    long day = midnightUTC(System.currentTimeMillis());

    Builder() {
      sparkProperties.put("spark.ui.enabled", "false");
      sparkProperties.put(
          "spark.cassandra.connection.ssl.enabled", getEnv("CASSANDRA_USE_SSL", "false"));
      sparkProperties.put(
          "spark.cassandra.connection.ssl.trustStore.password",
          System.getProperty("javax.net.ssl.trustStorePassword", ""));
      sparkProperties.put(
          "spark.cassandra.connection.ssl.trustStore.path",
          System.getProperty("javax.net.ssl.trustStore", ""));
      sparkProperties.put("spark.cassandra.auth.username", getEnv("CASSANDRA_USERNAME", ""));
      sparkProperties.put("spark.cassandra.auth.password", getEnv("CASSANDRA_PASSWORD", ""));
    }

    /** When set, this indicates which jars to distribute to the cluster. */
    public Builder jars(String... jars) {
      this.jars = jars;
      return this;
    }

    /** Keyspace to store dependency rowsToLinks. Defaults to "zipkin2" */
    public Builder keyspace(String keyspace) {
      this.keyspace = checkNotNull(keyspace, "keyspace");
      return this;
    }

    /** Day (in epoch milliseconds) to process dependencies for. Defaults to today. */
    public Builder day(long day) {
      this.day = midnightUTC(day);
      return this;
    }

    /** Ensures that logging is setup. Particularly important when in cluster mode. */
    public Builder logInitializer(Runnable logInitializer) {
      this.logInitializer = checkNotNull(logInitializer, "logInitializer");
      return this;
    }

    /** Comma separated list of hosts / IPs part of Cassandra cluster. Defaults to localhost */
    public Builder contactPoints(String contactPoints) {
      this.contactPoints = contactPoints;
      return this;
    }

    /** The local DC to connect to (other nodes will be ignored) */
    public Builder localDc(@Nullable String localDc) {
      this.localDc = localDc;
      return this;
    }

    public CassandraDependenciesJob build() {
      return new CassandraDependenciesJob(this);
    }
  }


  final String keyspace;
  final long day;
  final String dateStamp;
  final SparkConf conf;
  @Nullable final Runnable logInitializer;

  CassandraDependenciesJob(Builder builder) {
    this.keyspace = builder.keyspace;
    this.day = builder.day;
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    this.dateStamp = df.format(new Date(builder.day));
    this.conf = new SparkConf(true).setMaster(builder.sparkMaster).setAppName(getClass().getName());
    conf.set("spark.cassandra.connection.host", parseHosts(builder.contactPoints));
    conf.set("spark.cassandra.connection.port", parsePort(builder.contactPoints));
    if (builder.localDc != null) conf.set("connection.local_dc", builder.localDc);
    if (builder.jars != null) conf.setJars(builder.jars);
    for (Map.Entry<String, String> entry : builder.sparkProperties.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    this.logInitializer = builder.logInitializer;
  }

  public void run() {
    long microsLower = day * 1000;
    long microsUpper = (day * 1000) + TimeUnit.DAYS.toMicros(1) - 1;

    log.info(
        "Running Dependencies job for {}: {} ≤ Span.timestamp {}",
        dateStamp,
        microsLower,
        microsUpper);

    SparkContext sc = new SparkContext(conf);
    try {
      JavaRDD<DependencyLink> links =
          javaFunctions(sc)
              .cassandraTable(keyspace, "span")
              .spanBy(r -> r.getString("trace_id"), String.class)
              .flatMapValues(
                  new CassandraRowsToDependencyLinks(logInitializer, microsLower, microsUpper))
              .values()
              .mapToPair(link -> new Tuple2<>(new Tuple2<>(link.parent(), link.child()), link))
              .reduceByKey(
                  (l, r) ->
                      DependencyLink.newBuilder()
                          .parent(l.parent())
                          .child(l.child())
                          .callCount(l.callCount() + r.callCount())
                          .errorCount(l.errorCount() + r.errorCount())
                          .build())
              .values();
      if (links.isEmpty()) {
        log.info("No spans found at {}/span", keyspace);
      } else {
        log.info("Saving dependency links to {}/dependency", keyspace);
        // TODO: not sure how to add a constant result.. withConstantTimestamp(day) is similar
        AddDayToDependencyLink converter = new AddDayToDependencyLink(day);
        javaFunctions(links.map(converter))
            .writerBuilder(keyspace, "dependency", mapToRow(DependencyLinkWithDate.class))
            .saveToCassandra();
      }
    } finally {
      sc.stop();
    }
  }

  static final class AddDayToDependencyLink implements
      Function<DependencyLink, DependencyLinkWithDate>, Serializable {
    final Date day;

    AddDayToDependencyLink(long day) {
      this.day = new Date(day);
    }

    @Override public DependencyLinkWithDate call(DependencyLink link) {
      DependencyLinkWithDate result = new DependencyLinkWithDate();
      result.day = day;
      result.parent = link.parent();
      result.child = link.child();
      result.calls = link.callCount();
      result.errors = link.errorCount();
      return result;
    }
  }

  public static final class DependencyLinkWithDate {
    Date day; // needs to both be serializable and coersable to LocalDate (which isn't serializable)
    String parent;
    String child;
    long calls;
    long errors;

    @Column public Date getDay() {
      return day;
    }

    @Column public String getParent() {
      return parent;
    }

    @Column public String getChild() {
      return child;
    }

    @Column public long getCalls() {
      return calls;
    }

    @Column public long getErrors() {
      return errors;
    }
  }

  private static String getEnv(String key, String defaultValue) {
    String result = System.getenv(key);
    return result != null ? result : defaultValue;
  }

  static String parseHosts(String contactPoints) {
    List<String> result = new LinkedList<>();
    for (String contactPoint : contactPoints.split(",")) {
      HostAndPort parsed = HostAndPort.fromString(contactPoint);
      result.add(parsed.getHostText());
    }
    return Joiner.on(',').join(result);
  }

  /** Returns the consistent port across all contact points or 9042 */
  static String parsePort(String contactPoints) {
    Set<Integer> ports = Sets.newLinkedHashSet();
    for (String contactPoint : contactPoints.split(",")) {
      HostAndPort parsed = HostAndPort.fromString(contactPoint);
      ports.add(parsed.getPortOrDefault(9042));
    }
    return ports.size() == 1 ? String.valueOf(ports.iterator().next()) : "9042";
  }
}
