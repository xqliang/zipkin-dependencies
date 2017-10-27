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
package zipkin2.storage.cassandra;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.ClassRule;
import zipkin.Span;
import zipkin.dependencies.cassandra3.CassandraDependenciesJob;
import zipkin.internal.MergeById;
import zipkin.internal.V2SpanConverter;
import zipkin.internal.V2StorageComponent;
import zipkin.storage.DependenciesTest;
import zipkin.storage.QueryRequest;
import zipkin.storage.StorageComponent;

import static zipkin.internal.ApplyTimestampAndDuration.guessTimestamp;
import static zipkin.internal.Util.midnightUTC;

public class CassandraDependenciesTest extends DependenciesTest {
  @ClassRule public static CassandraStorageRule storage =
      new CassandraStorageRule("openzipkin/zipkin-cassandra:2.2.0", "test_zipkin3");

  @Override protected final StorageComponent storage() {
    return V2StorageComponent.create(storage.get());
  }

  @Override public void clear() throws IOException {
    storage.clear();
  }

  /**
   * This processes the job as if it were a batch. For each day we had traces, run the job again.
   */
  @Override public void processDependencies(List<Span> spans) {
    /** Makes sure the test cluster doesn't fall over on BusyPoolException */
    List<Span> page = new ArrayList<>();
    for (int i = 0; i < spans.size(); i++) {
      page.add(spans.get(i));
      if (page.size() == 10) {
        accept(page);
        page.clear();
      }
    }
    accept(page);

    // Now, block until writes complete, notably so we can read them.
    Session.State state = storage.session().getState();
    refresh:
    while (true) {
      for (Host host : state.getConnectedHosts()) {
        if (state.getInFlightQueries(host) > 0) {
          Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
          state = storage.session().getState();
          continue refresh;
        }
      }
      break;
    }

    /// Now actually process the dependencies
    Set<Long> days = new LinkedHashSet<>();
    for (List<Span> trace : storage().spanStore()
        .getTraces(QueryRequest.builder().limit(10000).build())) {
      days.add(midnightUTC(guessTimestamp(MergeById.apply(trace).get(0)) / 1000));
    }

    for (long day : days) {
      CassandraDependenciesJob.builder()
          .keyspace(storage.keyspace)
          .localDc(storage.get().localDc())
          .contactPoints(storage.contactPoints())
          .day(day).build().run();
    }
  }

  void accept(List<Span> page) {
    try {
      storage.get().spanConsumer().accept(V2SpanConverter.fromSpans(page)).execute();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
