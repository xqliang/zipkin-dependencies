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

import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.types.TypeConverter;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;
import zipkin2.DependencyLink;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.internal.DependencyLinker;

final class CassandraRowsToDependencyLinks
    implements Serializable, Function<Iterable<CassandraRow>, Iterable<DependencyLink>> {
  private static final long serialVersionUID = 0L;
  private static final Logger log = LoggerFactory.getLogger(CassandraRowsToDependencyLinks.class);

  @Nullable final Runnable logInitializer;
  final long startTs;
  final long endTs;

  CassandraRowsToDependencyLinks(Runnable logInitializer, long startTs, long endTs) {
    this.logInitializer = logInitializer;
    this.startTs = startTs;
    this.endTs = endTs;
  }

  static Span rowToSpan(CassandraRow row) {
    zipkin2.Span.Builder builder =
        zipkin2.Span.newBuilder()
            .traceId(row.getString("trace_id"))
            .parentId(row.getString("parent_id"))
            .id(row.getString("id"))
            .timestamp(row.getLong("ts"))
            .shared(row.getBoolean("shared"));

    Map<String, String> tags =
        row.getMap(
            "tags", TypeConverter.StringConverter$.MODULE$, TypeConverter.StringConverter$.MODULE$);
    String error = tags.get("error");
    if (error != null) {
      builder.putTag("error", error);
    }
    String kind = row.getString("kind");
    if (kind != null) {
      try {
        builder.kind(Span.Kind.valueOf(kind));
      } catch (IllegalArgumentException ignored) {
      }
    }
    Endpoint localEndpoint = readEndpoint(row.getObject("l_ep"));
    if (localEndpoint != null) {
      builder.localEndpoint(localEndpoint);
    }
    Endpoint remoteEndpoint = readEndpoint(row.getObject("r_ep"));
    if (remoteEndpoint != null) {
      builder.remoteEndpoint(remoteEndpoint);
    }
    return builder.build();
  }

  // UDT type doesn't work
  private static Endpoint readEndpoint(Object endpoint) {
    if (endpoint != null) {
      String serviceName = null;
      try {
        Field field = endpoint.getClass().getDeclaredField("service");
        field.setAccessible(true);
        serviceName = field.get(endpoint).toString();
      } catch (Exception e) {
        e.printStackTrace();
      }
      if (serviceName != null && !"".equals(serviceName)) { // not possible if written via zipkin
        return Endpoint.newBuilder().serviceName(serviceName).build();
      }
    }
    return null;
  }

  @Override
  public Iterable<DependencyLink> call(Iterable<CassandraRow> rows) {
    if (logInitializer != null) logInitializer.run();
    // use a hash set to dedupe any redundantly accepted spans
    Set<Span> sameTraceId = new LinkedHashSet<>();
    for (CassandraRow row : rows) {
      Span span;
      try {
        span = rowToSpan(row);
      } catch (RuntimeException e) {
        log.warn(
            String.format(
                "Unable to decode span from traces where trace_id=%s and ts=%s and span='%s'",
                row.getLong("trace_id"), row.getDate("ts").getTime(), row.getString("span")),
            e);
        continue;
      }
      // check to see if the trace is within the interval
      if (span.parentId() == null) {
        Long timestamp = span.timestamp();
        if (timestamp == null || timestamp < startTs || timestamp > endTs) {
          return Collections.emptyList();
        }
      }
      sameTraceId.add(span);
    }
    return new DependencyLinker().putTrace(sameTraceId.iterator()).link();
  }
}
