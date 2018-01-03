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
package zipkin.dependencies.elasticsearch;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;
import javax.annotation.Nullable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import zipkin.Codec;
import zipkin.DependencyLink;
import zipkin.internal.Util;
import zipkin.internal.V2SpanConverter;
import zipkin.internal.gson.stream.JsonReader;
import zipkin.internal.gson.stream.MalformedJsonException;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;

import static zipkin.internal.Util.checkNotNull;
import static zipkin.internal.Util.midnightUTC;

public final class ElasticsearchDependenciesJob {
  private static final Logger log = LoggerFactory.getLogger(ElasticsearchDependenciesJob.class);

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {

    String index = getEnv("ES_INDEX", "zipkin");
    String hosts = getEnv("ES_HOSTS", "127.0.0.1");
    String username = getEnv("ES_USERNAME", null);
    String password = getEnv("ES_PASSWORD", null);

    final Map<String, String> sparkProperties = new LinkedHashMap<>();

    Builder() {
      sparkProperties.put("spark.ui.enabled", "false");
      // don't die if there are no spans
      sparkProperties.put("es.index.read.missing.as.empty", "true");
      sparkProperties.put("es.nodes.wan.only", getEnv("ES_NODES_WAN_ONLY", "false"));
      sparkProperties.put("es.net.ssl.keystore.location",
              getSystemPropertyAsFileResource("javax.net.ssl.keyStore"));
      sparkProperties.put("es.net.ssl.keystore.pass",
              System.getProperty("javax.net.ssl.keyStorePassword", ""));
      sparkProperties.put("es.net.ssl.truststore.location",
              getSystemPropertyAsFileResource("javax.net.ssl.trustStore"));
      sparkProperties.put("es.net.ssl.truststore.pass",
              System.getProperty("javax.net.ssl.trustStorePassword", ""));
    }

    // local[*] master lets us run & test the job locally without setting a Spark cluster
    String sparkMaster = getEnv("SPARK_MASTER", "local[*]");
    // needed when not in local mode
    String[] jars;
    Runnable logInitializer;

    // By default the job only works on traces whose first timestamp is today
    long day = midnightUTC(System.currentTimeMillis());

    /** When set, this indicates which jars to distribute to the cluster. */
    public Builder jars(String... jars) {
      this.jars = jars;
      return this;
    }

    /** The index prefix to use when generating daily index names. Defaults to "zipkin" */
    public Builder index(String index) {
      this.index = checkNotNull(index, "index");
      return this;
    }

    public Builder hosts(String hosts) {
      this.hosts = checkNotNull(hosts, "hosts");
      sparkProperties.put("es.nodes.wan.only", "true");
      return this;
    }

    /** username used for basic auth. Needed when Shield or X-Pack security is enabled */
    public Builder username(String username) {
      this.username = username;
      return this;
    }

    /** password used for basic auth. Needed when Shield or X-Pack security is enabled */
    public Builder password(String password) {
      this.password = password;
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

    public ElasticsearchDependenciesJob build() {
      return new ElasticsearchDependenciesJob(this);
    }
  }

  private static String getSystemPropertyAsFileResource(String key) {
    String prop = System.getProperty(key, "");
    return prop != null && !prop.isEmpty() ? "file:" + prop : prop;
  }

  final String index;
  final long day;
  final String dateStamp;
  final SparkConf conf;
  @Nullable final Runnable logInitializer;

  ElasticsearchDependenciesJob(Builder builder) {
    this.index = builder.index;
    this.day = builder.day;
    String dateSeparator = getEnv("ES_DATE_SEPARATOR", "-");
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd".replace("-", dateSeparator));
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    this.dateStamp = df.format(new Date(builder.day));
    this.conf = new SparkConf(true)
        .setMaster(builder.sparkMaster)
        .setAppName(getClass().getName());
    if (builder.jars != null) conf.setJars(builder.jars);
    if (builder.username != null) conf.set("es.net.http.auth.user", builder.username);
    if (builder.password != null) conf.set("es.net.http.auth.pass", builder.password);
    conf.set("es.nodes", parseHosts(builder.hosts));
    if (builder.hosts.indexOf("https") != -1) conf.set("es.net.ssl", "true");
    for (Map.Entry<String, String> entry : builder.sparkProperties.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    this.logInitializer = builder.logInitializer;
  }

  public void run() {
    run( // multi-type index
        index + "-" + dateStamp + "/span",
        index + "-" + dateStamp + "/dependencylink",
        SpanDecoder.INSTANCE
    );

    run( // single-type index
        index + ":span-" + dateStamp + "/span",
        index + ":dependency-" + dateStamp + "/dependency",
        Span2Decoder.INSTANCE
    );
    log.info("Done");
  }

  interface SpanAcceptor {
    void decodeInto(String json, Collection<Span> sameTraceId);
  }

  // enums are used here because they are naturally serializable
  enum SpanDecoder implements SpanAcceptor {
    INSTANCE {
      @Override public void decodeInto(String json, Collection<Span> sameTraceId) {
        zipkin.Span v1Span = Codec.JSON.readSpan(json.getBytes(Util.UTF_8));
        sameTraceId.addAll(V2SpanConverter.fromSpan(v1Span));
      }
    };
  }

  enum Span2Decoder implements SpanAcceptor {
    INSTANCE;

    @Override public void decodeInto(String json, Collection<Span> sameTraceId) {
      SpanBytesDecoder.JSON_V2.decode(json.getBytes(Util.UTF_8), sameTraceId);
    }
  }

  void run(String spanResource, String dependencyLinkResource, SpanAcceptor decoder) {
    log.info("Processing spans from {}", spanResource);
    JavaSparkContext sc = new JavaSparkContext(conf);
    log.info("conf:{}",conf.toDebugString());
    log.info("conf:{}",conf.toString());
    log.info("sc:{}",sc.toString());
    try {
      JavaPairRDD<String, String> t0 = JavaEsSpark.esJsonRDD(sc, spanResource);
      log.info("t0:{}", t0.toDebugString());
      JavaPairRDD<String, Iterable<Tuple2<String, String>>> t1 = t0.groupBy(pair -> traceId(pair._2));
      List<Tuple2<String, Iterable<Tuple2<String, String>>>> m1 = t1.collect();
      log.info("m1 len:" + m1.size());
      for (Tuple2<String, Iterable<Tuple2<String, String>>> row:m1){
        Iterator<Tuple2<String, String>> iterator = row._2().iterator();
        while (iterator.hasNext()){
          Tuple2<String, String> next = iterator.next();
          String l1 = next._1();
          String l2 = next._2();
            log.info("l1:"+l1);
            log.info("l2:"+l2);
            break;
        }
      }
      log.warn("TraceIdAndJsonToDependencyLinks logInitializer:" + logInitializer.toString());
      log.warn("TraceIdAndJsonToDependencyLinks decoder:" + decoder.toString());
      log.warn("TraceIdAndJsonToDependencyLinks2 logInitializer:" + logInitializer);
      log.warn("TraceIdAndJsonToDependencyLinks2 decoder:" + decoder);

      TraceIdAndJsonToDependencyLinks unkonw = new TraceIdAndJsonToDependencyLinks(logInitializer, decoder);
      log.warn("TraceIdAndJsonToDependencyLinks unkonw:" + unkonw.toString());
      log.warn("TraceIdAndJsonToDependencyLinks2 unkonw:" + unkonw);

      JavaPairRDD<String, DependencyLink> t2 = t1.flatMapValues(new TraceIdAndJsonToDependencyLinks(logInitializer, decoder));
      List<Tuple2<String, DependencyLink>> m2 = t2.collect();
      log.info("m2 len:"+m2.size());
      for (Tuple2<String, DependencyLink> row:m2){
        String m21 = row._1();
        DependencyLink m22 = row._2();
        log.info("m21:"+m21);
        log.info("m2:"+m22.toString());
      }
      JavaRDD<DependencyLink> t3 = t2.values();
      List<DependencyLink> m3 = t3.collect();
      log.info("m3 len:"+m3.size());
      for (DependencyLink row:m3){
        log.info("m3:"+ row.toString());
        break;
      }
      JavaPairRDD<Tuple2<String, String>, DependencyLink> t4 = t3.mapToPair(link -> new Tuple2<>(new Tuple2<>(link.parent, link.child), link));
      List<Tuple2<Tuple2<String, String>, DependencyLink>> m4 = t4.collect();
      log.info("m4 len:"+m4.size());
      for (Tuple2<Tuple2<String, String>, DependencyLink> row:m4){
        Tuple2<String, String> m31 = row._1();
        log.info("m41:"+m31.toString());
        String m32 = m31._1();
        log.info("m42:"+m32);
        String m33 = m31._2();
        log.info("m43:"+m33);
        DependencyLink m34 = row._2();
        log.info("m44:" +m34.toString());
        break;
      }
      JavaPairRDD<Tuple2<String, String>, DependencyLink> t5 = t4.reduceByKey((l, r) -> DependencyLink.builder().parent(l.parent)
              .child(l.child)
              .callCount(l.callCount + r.callCount)
              .errorCount(l.errorCount + r.errorCount).build());
      List<Tuple2<Tuple2<String, String>, DependencyLink>> m5 = t5.collect();
      log.info("m5 len:" + m5.size());
      for (Tuple2<Tuple2<String, String>, DependencyLink> row:m5){
        Tuple2<String, String> m51 = row._1();
        log.info("m51:"+m51);
        String m52 = m51._1();
        log.info("m52:"+m52);
        String m53 = m51._2();
        log.info("m53:"+m53);
        DependencyLink m54 = row._2();
        log.info("m54:"+m54.toString());
        break;
      }
      JavaRDD<DependencyLink> t6 = t5.values();
      List<DependencyLink> m6 = t6.collect();
      log.info("m6 len:"+m6.size());
      for (DependencyLink row:m6){
        log.info("m61:"+row.toString());
      }
      JavaRDD<Map<String, Object>> t7 = t6.map(ElasticsearchDependenciesJob::dependencyLinkJson);
      List<Map<String, Object>> m7 = t7.collect();
      log.info("m7 len:"+m7.size());
      for (Map<String, Object> row:m7){
        log.info("m71:"+m7.toString());
        break;
      }

      JavaRDD<Map<String, Object>> links = JavaEsSpark.esJsonRDD(sc, spanResource)
          .groupBy(pair -> traceId(pair._2))
          .flatMapValues(new TraceIdAndJsonToDependencyLinks(logInitializer, decoder))
          .values()
          .mapToPair(link -> new Tuple2<>(new Tuple2<>(link.parent, link.child), link))
          .reduceByKey((l, r) -> DependencyLink.builder()
              .parent(l.parent)
              .child(l.child)
              .callCount(l.callCount + r.callCount)
              .errorCount(l.errorCount + r.errorCount).build())
          .values()
          .map(ElasticsearchDependenciesJob::dependencyLinkJson);
      List<Map<String, Object>> tmp = links.collect();
      for (Map<String, Object> row:tmp){
          log.info("row origin:",row);
          log.info("row origin2:"+row);
          log.info("row:", row.toString());
      }
      if (links.isEmpty()) {
        log.info("No spans found at {}", spanResource);
      } else {
        log.info("Saving dependency links to {}", dependencyLinkResource);
        log.info("Saving debug links {}", links.toDebugString());

        log.info("Saving mapping string {}", Collections.singletonMap("es.mapping.id", "id").toString());
        log.info("Saving mapping {}", Collections.singletonMap("es.mapping.id", "id"));
        JavaEsSpark.saveToEs(links, dependencyLinkResource,
            Collections.singletonMap("es.mapping.id", "id")); // allows overwriting the link
      }
    } finally {
      sc.stop();
    }
  }

  /**
   * Same as {@linkplain DependencyLink}, except it adds an ID field so the job can be re-run,
   * overwriting a prior run's value for the link.
   */
  static Map<String, Object> dependencyLinkJson(DependencyLink l) {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("id", l.parent + "|" + l.child);
    result.put("parent", l.parent);
    result.put("child", l.child);
    result.put("callCount", l.callCount);
    result.put("errorCount", l.errorCount);
    log.info("result string:{}",result.toString());
    return result;
  }

  private static String getEnv(String key, String defaultValue) {
    String result = System.getenv(key);
    return result != null && !result.isEmpty() ? result : defaultValue;
  }

  /** returns the lower 64 bits of the trace ID */
  static String traceId(String json) throws IOException {
//    log.info("json string:{}",json);
    JsonReader reader = new JsonReader(new StringReader(json));
    reader.beginObject();
    while (reader.hasNext()) {
      String nextName = reader.nextName();
      if (nextName.equals("traceId")) {
        String traceId = reader.nextString();
//        log.info("traceId string:{}",traceId);
        String ret = traceId.length() > 16 ? traceId.substring(traceId.length() - 16) : traceId;
//        log.info("ret string:{}",ret);
        return ret;
      } else {
        reader.skipValue();
      }
    }
    throw new MalformedJsonException("no traceId in " + json);
  }

  static String parseHosts(String hosts) {
    StringBuilder to = new StringBuilder();
    String[] hostParts = hosts.split(",");
    for (int i = 0; i < hostParts.length; i++) {
      String host = hostParts[i];
      if (host.startsWith("http")) {
        URI httpUri = URI.create(host);
        int port = httpUri.getPort();
        if (port == -1) {
          port = host.startsWith("https") ? 443 : 80;
        }
        to.append(httpUri.getHost() + ":" + port);
      } else {
        to.append(host);
      }
      if (i + 1 < hostParts.length) {
        to.append(',');
      }
    }
    return to.toString();
  }
}
