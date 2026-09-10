import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Header;

/** Independent Kafka Java consumed-log oracle and bounded public-Producer driver. */
public final class VerifyRecords {
    static final int MAX_FILE = 32 * 1024 * 1024, MAX_RECORDS = 6000, MAX_PARTITIONS = 256;
    static void require(boolean condition, String message) {
        if (!condition) throw new IllegalArgumentException(message);
    }
    @SuppressWarnings("unchecked") static Map<String,Object> object(Object value) {
        require(value instanceof Map, "object required"); return (Map<String,Object>)value;
    }
    @SuppressWarnings("unchecked") static List<Object> array(Object value) {
        require(value instanceof List, "array required"); return (List<Object>)value;
    }
    static long number(Object value) { require(value instanceof Long, "integer required"); return (Long)value; }
    static String string(Object value) { require(value instanceof String, "string required"); return (String)value; }
    static byte[] hex(Object value) {
        if (value == null) return null;
        String text = string(value);
        require(text.length() % 2 == 0 && text.length() <= MAX_FILE, "invalid hex length");
        byte[] bytes = new byte[text.length()/2];
        for (int i=0; i<bytes.length; i++) {
            int high = Character.digit(text.charAt(2*i),16), low = Character.digit(text.charAt(2*i+1),16);
            require(high >= 0 && low >= 0, "invalid hex digit"); bytes[i]=(byte)(16*high+low);
        }
        return bytes;
    }
    static String hex(byte[] bytes) {
        if (bytes == null) return null;
        char[] output = new char[2*bytes.length]; String alphabet="0123456789abcdef";
        for (int i=0;i<bytes.length;i++) { output[2*i]=alphabet.charAt((bytes[i]&255)>>>4); output[2*i+1]=alphabet.charAt(bytes[i]&15); }
        return new String(output);
    }
    static String uuid(Uuid id) { return hex(ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array()); }
    static Map<String,Object> row(Object... pairs) {
        Map<String,Object> map=new LinkedHashMap<>();
        for(int i=0;i<pairs.length;i+=2) map.put((String)pairs[i],pairs[i+1]); return map;
    }
    static String identity(ConsumerRecord<byte[],byte[]> record, String key) {
        String value=null;
        for (Header header:record.headers().headers(key)) {
            require(value==null && header.value()!=null, "missing/duplicate identity header");
            value=new String(header.value(),StandardCharsets.UTF_8);
        }
        require(value!=null,"missing identity header "+key); return value;
    }
    static void compare(ConsumerRecord<byte[],byte[]> record, Map<String,Object> expected) {
        require(record.partition()==number(expected.get("expected_partition")),"partition mismatch");
        require(Arrays.equals(record.key(),hex(expected.get("key_hex"))),"key mismatch");
        require(Arrays.equals(record.value(),hex(expected.get("value_hex"))),"value mismatch");
        Object timestamp=expected.containsKey("timestamp_ms")?expected.get("timestamp_ms"):expected.get("timestamp");
        require(record.timestamp()==number(timestamp),"timestamp mismatch");
        List<Object> headers=array(expected.get("headers")); int index=0;
        for(Header actual:record.headers()) {
            require(index<headers.size(),"extra header"); Map<String,Object> header=object(headers.get(index++));
            require(actual.key().equals(string(header.get("key"))) && Arrays.equals(actual.value(),hex(header.get("value_hex"))),"header mismatch");
        }
        require(index==headers.size(),"missing header");
    }
    static void compareAcknowledgement(ConsumerRecord<byte[],byte[]> record, Map<String,Object> outcome, String topicId) {
        require(number(outcome.get("partition"))==record.partition(),"Acked partition mismatch");
        require(number(outcome.get("offset"))==record.offset(),"Acked absolute offset mismatch");
        require(topicId.equalsIgnoreCase(string(outcome.get("topic_id"))),"Acked UUID mismatch");
        // This optional field is the broker's ProduceResponse.logAppendTime,
        // not the encoded per-record CreateTime checked by compare(). Kafka4.3
        // UnifiedLog.java1247–1251 returns duplicate.timestamp() here on dedup;
        // ProducerAppendInfo.java217 stores batch.maxTimestamp() in that entry.
        // FutureRecordMetadata.java109–110 also prefers any returned batch time.
        if(outcome.get("timestamp_ms")!=null)require(number(outcome.get("timestamp_ms"))>=0,"negative optional broker timestamp");
    }
    static Map<String,Object> verify(Map<String,String> args) throws Exception {
        Path ledgerPath=Path.of(args.get("--ledger"));
        require(Files.size(ledgerPath)<=MAX_FILE,"ledger too large");
        Map<String,Object> ledger=object(new Json(Files.readString(ledgerPath)).parse());
        boolean javaProducer="kr-kafka-java-producer-check/v1".equals(ledger.get("schema"));
        require(javaProducer||"kr-kafka-producer-check/v1".equals(ledger.get("schema")),"ledger schema");
        Map<String,Object> config=object(ledger.get("config")), profile=object(config.get("profile"));
        String run=string(config.get("run_id")), topic=args.getOrDefault("--topic",string(profile.get("topic")));
        int generation=Integer.parseInt(args.getOrDefault("--generation","0"));
        String mode=args.getOrDefault("--mode","final");
        require(List.of("final","checkpoint","negative","unavailable","failed").contains(mode),"unknown verification mode");
        require(generation==0||generation==1,"generation bound");
        if(mode.equals("final")||mode.equals("unavailable")) {
            require(Boolean.TRUE.equals(ledger.get("complete"))&&Boolean.TRUE.equals(ledger.get("closed"))&&Boolean.TRUE.equals(ledger.get("joined"))&&ledger.get("error")==null,"producer final state is incomplete");
            if(!javaProducer)for(Object value:array(ledger.get("final_credits")))require(number(object(value).get("held"))==0,"producer credits still held");
        }
        if(mode.equals("negative"))require(Boolean.FALSE.equals(ledger.get("complete"))&&ledger.get("error")!=null,"negative producer did not fail");
        long timeout=Long.parseLong(args.getOrDefault("--timeout-ms","30000"));
        require(timeout>0&&timeout<=120000,"timeout bound");
        List<Object> admitted=array(ledger.get("accepted")), delivered=array(ledger.get("deliveries"));
        if(javaProducer)require(number(ledger.get("callbacks"))==admitted.size()&&number(ledger.get("futures"))==admitted.size(),"Java callback/future obligations incomplete");
        require(admitted.size()<=MAX_RECORDS&&delivered.size()<=MAX_RECORDS,"ledger record bound");
        Map<Long,Map<String,Object>> expected=new HashMap<>(), outcomes=new HashMap<>(), allAccepted=new HashMap<>();
        Map<Integer,Long> nextOrder=new HashMap<>(); Map<Long,Long> order=new HashMap<>();
        for(Object value:admitted) {
            Map<String,Object> record=object(value); long id=number(record.get("record_id"));
            require(allAccepted.put(id,record)==null,"duplicate accepted ID across generations");
            if(number(record.getOrDefault("generation",0L))!=generation) continue;
            require(expected.put(id,record)==null,"duplicate accepted ID");
            int partition=Math.toIntExact(number(record.get("expected_partition")));
            long ordinal=nextOrder.getOrDefault(partition,0L); order.put(id,ordinal); nextOrder.put(partition,ordinal+1);
        }
        for(Object value:delivered) {
            Map<String,Object> record=object(value); long id=number(record.get("record_id"));
            require(allAccepted.containsKey(id),"delivery for unaccepted ID");
            if(!javaProducer)require(number(record.get("token"))==number(allAccepted.get(id).get("token"))&&number(record.get("topic_handle"))==number(allAccepted.get(id).get("topic_handle")),"delivery admission identity mismatch");
            if(expected.containsKey(id)) require(outcomes.put(id,record)==null,"duplicate delivery ID");
        }
        require(expected.size()==outcomes.size(),"accepted records lack terminal delivery");
        require(mode.equals("failed")||(mode.equals("negative")?expected.isEmpty():!expected.isEmpty()),"unexpected empty/nonempty selected generation");
        Properties props=new Properties();
        props.put("bootstrap.servers",args.get("--bootstrap")); props.put("client.id","kr-independent-log-verifier");
        props.put("request.timeout.ms",Long.toString(timeout)); props.put("default.api.timeout.ms",Long.toString(timeout));
        String topicId;
        try(Admin admin=Admin.create(props)) { topicId=uuid(admin.describeTopics(List.of(topic)).allTopicNames().get(timeout,TimeUnit.MILLISECONDS).get(topic).topicId()); }
        Object expectedIds=ledger.get("topic_ids");
        Object expectedId=object(expectedIds).get(Integer.toString(generation));
        if(!List.of("negative","unavailable","failed").contains(mode))require(expectedId!=null,"missing selected immutable topic ID");
        if(expectedId!=null)require(topicId.equalsIgnoreCase(string(expectedId)),"immutable topic ID mismatch");
        props.put("key.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("enable.auto.commit","false"); props.put("allow.auto.create.topics","false");
        props.put("isolation.level","read_uncommitted"); props.put("max.poll.records","256");
        props.put("fetch.max.bytes","8388608"); props.put("max.partition.fetch.bytes","2097152");
        Map<String,Object> report=row("schema","kr-kafka-log-verification/v1","run_id",run,"topic",topic,"topic_id",topicId,"generation",generation);
        List<Object> observed=new ArrayList<>(), watermarkRows=new ArrayList<>();
        Set<Long> seen=new HashSet<>(); Map<Integer,Long> previousOrder=new HashMap<>();
        Path evidencePath=Path.of(args.get("--output")+".records.ndjson");
        try(KafkaConsumer<byte[],byte[]> consumer=new KafkaConsumer<>(props);
            var evidence=Files.newBufferedWriter(evidencePath,StandardCharsets.UTF_8)) {
            int partitions=consumer.partitionsFor(topic).size();
            require(partitions>0&&partitions<=MAX_PARTITIONS&&partitions==number(profile.get("partitions")),"partition count mismatch");
            List<TopicPartition> assigned=new ArrayList<>(); for(int p=0;p<partitions;p++)assigned.add(new TopicPartition(topic,p));
            consumer.assign(assigned); Map<TopicPartition,Long> starts=consumer.beginningOffsets(assigned), ends=consumer.endOffsets(assigned);
            for(TopicPartition partition:assigned) {
                require(starts.get(partition)==0,"fresh fixture log was truncated"); consumer.seek(partition,0);
                watermarkRows.add(row("partition",partition.partition(),"start",starts.get(partition),"end",ends.get(partition)));
            }
            long deadline=System.nanoTime()+timeout*1_000_000, evidenceBytes=0;
            while(true) {
                boolean done=true; for(TopicPartition partition:assigned) if(consumer.position(partition)<ends.get(partition))done=false;
                if(done)break;
                require(System.nanoTime()<deadline,"consumer did not reach captured end offsets");
                for(ConsumerRecord<byte[],byte[]> record:consumer.poll(Duration.ofMillis(100))) {
                    List<Object> rawHeaders=new ArrayList<>();
                    for(Header header:record.headers())rawHeaders.add(row("key",header.key(),"value_hex",hex(header.value())));
                    String raw=json(row("partition",record.partition(),"offset",record.offset(),"timestamp_ms",record.timestamp(),"key_hex",hex(record.key()),"value_hex",hex(record.value()),"headers",rawHeaders))+"\n";
                    evidenceBytes+=raw.getBytes(StandardCharsets.UTF_8).length;
                    require(evidenceBytes<=2L*MAX_FILE,"consumed evidence byte bound");
                    evidence.write(raw); evidence.flush();
                    TopicPartition partition=new TopicPartition(topic,record.partition());
                    require(record.offset()<ends.get(partition),"record appended after captured end");
                    require(run.equals(identity(record,"kr-check-run")),"foreign run in isolated topic");
                    long id=Long.parseLong(identity(record,"kr-check-id"));
                    Map<String,Object> wanted=expected.get(id); require(wanted!=null,"unaccepted or wrong-generation record committed: "+id);
                    require(seen.add(id),"duplicate record ID committed: "+id); compare(record,wanted);
                    long ordinal=order.get(id); require(ordinal>previousOrder.getOrDefault(record.partition(),-1L),"per-partition admission order violated"); previousOrder.put(record.partition(),ordinal);
                    Map<String,Object> outcome=outcomes.get(id); String kind=string(outcome.get("kind"));
                    require(!kind.equals("NotWritten"),"NotWritten record committed: "+id);
                    require(kind.equals("Acked")||kind.equals("Unknown"),"unknown delivery kind");
                    if(kind.equals("Acked")) {
                        compareAcknowledgement(record,outcome,topicId);
                    }
                    observed.add(row("record_id",id,"partition",record.partition(),"offset",record.offset(),"timestamp",record.timestamp(),"broker_log_append_time",outcome.get("timestamp_ms"),"kind",kind));
                    require(observed.size()<=MAX_RECORDS,"consumed bound exceeded");
                }
            }
            require(ends.equals(consumer.endOffsets(assigned)),"log changed during final verification");
        }
        long acked=0,unknownPresent=0;
        for(var entry:outcomes.entrySet()) {
            String kind=string(entry.getValue().get("kind"));
            if(kind.equals("Acked")){acked++;require(seen.contains(entry.getKey()),"Acked record missing: "+entry.getKey());}
            else if(kind.equals("Unknown")){if(seen.contains(entry.getKey()))unknownPresent++;}
            else require(kind.equals("NotWritten"),"unknown delivery kind");
        }
        report.putAll(row("verified",true,"mode",mode,"accepted",expected.size(),"acked",acked,"committed",seen.size(),"unknown_committed",unknownPresent,"watermarks",watermarkRows,"observed",observed,"records_path",evidencePath.toString()));
        return report;
    }
    static void phase(String name, int accepted, int callbacks) throws Exception {
        System.out.println(json(row("kind","phase","phase",name,"accepted",accepted,"callbacks",callbacks)));
        System.out.flush();
        var response=new java.util.concurrent.CompletableFuture<String>();
        Thread.ofVirtual().start(()->{
            try {
                StringBuilder text=new StringBuilder();
                for(int next;(next=System.in.read())!='\n';){require(next>=0&&text.length()<64,"bounded phase response required");text.append((char)next);}
                response.complete(text.toString());
            } catch(Throwable failure){response.completeExceptionally(failure);}
        });
        require(response.get(60,TimeUnit.SECONDS).equals("continue"),"phase requires continue");
    }
    static boolean expectedRejection(Throwable error, String scenario, String producerClass) throws Exception {
        if(error==null)return false;
        if(producerClass.startsWith("org.apache."))return scenario.equals("broker_size")
            ?error instanceof org.apache.kafka.common.errors.RecordTooLargeException:error instanceof org.apache.kafka.common.errors.TopicAuthorizationException;
        Throwable diagnostic=nativeDiagnostic(error);
        if(diagnostic==null)return false;
        int outcome=(Integer)diagnostic.getClass().getMethod("outcome").invoke(diagnostic);
        if(outcome==2&&!error.getClass().getName().equals("io.krkafka.producer.DeliveryUnknownException"))return false;
        if(outcome==1&&scenario.equals("broker_size")&&!(error instanceof org.apache.kafka.common.errors.RecordTooLargeException))return false;
        return (outcome==1||outcome==2)&&((Integer)diagnostic.getClass().getMethod("reason").invoke(diagnostic))==(scenario.equals("broker_size")?6:8);
    }
    static Throwable nativeDiagnostic(Throwable error) {
        if(error.getClass().getName().equals("io.krkafka.producer.NativeDeliveryException"))return error;
        for(Throwable detail:error.getSuppressed())if(detail.getClass().getName().equals("io.krkafka.producer.NativeDeliveryException"))return detail;
        return null;
    }
    static Map<String,Object> rejectedDelivery(int id,Throwable error) throws Exception {
        Throwable diagnostic=nativeDiagnostic(error);
        int outcome=diagnostic==null?1:(Integer)diagnostic.getClass().getMethod("outcome").invoke(diagnostic);
        Map<String,Object> delivery=row("record_id",(long)id,"kind",outcome==2?"Unknown":"NotWritten","exception",error.getClass().getName());
        if(diagnostic!=null)delivery.put("native_diagnostic",row("outcome",outcome,"reason",diagnostic.getClass().getMethod("reason").invoke(diagnostic),"attempts",diagnostic.getClass().getMethod("attempts").invoke(diagnostic)));
        return delivery;
    }
    /** A real Kafka4.3 header-aware interceptor, loaded by both public producers. */
    public static final class HeaderAudit implements org.apache.kafka.clients.producer.ProducerInterceptor<byte[],byte[]> {
        static final java.util.concurrent.ConcurrentHashMap<Integer,List<Object>> pending=new java.util.concurrent.ConcurrentHashMap<>();
        static final java.util.concurrent.atomic.AtomicInteger sends=new java.util.concurrent.atomic.AtomicInteger(),acks=new java.util.concurrent.atomic.AtomicInteger(),closes=new java.util.concurrent.atomic.AtomicInteger();
        static final AtomicReference<Throwable> failure=new AtomicReference<>();
        public void configure(Map<String,?> config) {}
        static List<Object> headers(org.apache.kafka.common.header.Headers headers) {
            List<Object> rows=new ArrayList<>();
            for(Header header:headers)rows.add(row("key",header.key(),"value_hex",hex(header.value())));
            return rows;
        }
        public ProducerRecord<byte[],byte[]> onSend(ProducerRecord<byte[],byte[]> record) {
            try {
                record.headers().add("interceptor-audit",new byte[]{0,(byte)255});
                int id=Integer.parseInt(new String(record.headers().lastHeader("kr-check-id").value(),StandardCharsets.UTF_8));
                require(pending.size()<MAX_RECORDS&&pending.putIfAbsent(id,headers(record.headers()))==null,"interceptor send identity/bound");sends.incrementAndGet();
            } catch(Throwable error){failure.compareAndSet(null,error);}
            return record;
        }
        public void onAcknowledgement(RecordMetadata metadata,Exception error) {
            failure.compareAndSet(null,new IllegalStateException("header-aware acknowledgement was skipped"));
        }
        public void onAcknowledgement(RecordMetadata metadata,Exception error,org.apache.kafka.common.header.Headers headers) {
            try {
                require(error==null&&metadata!=null&&headers!=null,"interceptor acknowledgement failed");
                int id=Integer.parseInt(new String(headers.lastHeader("kr-check-id").value(),StandardCharsets.UTF_8));
                require(headers(headers).equals(pending.remove(id)),"interceptor headers/identity changed");acks.incrementAndGet();
            } catch(Throwable failure){HeaderAudit.failure.compareAndSet(null,failure);}
        }
        public void close(){closes.incrementAndGet();}
    }
    /** Produces a bounded common workload through the public Producer interface.
     * Reflection keeps the consumed-log verifier independent of binding classes. */
    @SuppressWarnings("unchecked")
    static Map<String,Object> produce(Map<String,String> args) throws Exception {
        String producerClass=args.get("--producer-class"), topic=args.get("--topic"), run=args.getOrDefault("--run-id","java-comparison");
        require(List.of("org.apache.kafka.clients.producer.KafkaProducer","io.krkafka.producer.KrKafkaProducer").contains(producerClass),"supported producer class required");
        require(topic!=null&&!topic.isBlank()&&run.length()<=128,"topic/run identity required");
        String scenario=args.getOrDefault("--scenario","positive");
        require(List.of("positive","restart","broker_size","authorization","recreate").contains(scenario),"supported scenario required");
        boolean rejectedForSize=scenario.equals("broker_size");
        boolean rejectedByBroker=rejectedForSize||scenario.equals("authorization");
        boolean interceptor=Boolean.parseBoolean(args.getOrDefault("--interceptor","false"));
        require(!interceptor||scenario.equals("positive"),"header audit is a positive scenario");
        int records=Integer.parseInt(args.getOrDefault("--records","192")), callers=Integer.parseInt(args.getOrDefault("--callers","4"));
        long timeout=Long.parseLong(args.getOrDefault("--timeout-ms","30000"));
        long timestampBase=Long.parseLong(args.getOrDefault("--timestamp-ms",Long.toString(System.currentTimeMillis())));
        require(records>=12&&records<=2000&&callers>=1&&callers<=16&&timeout>0&&timeout<=120000,"workload bound");
        require(!rejectedByBroker||callers==1,"broker rejection mapping requires one sequential caller");
        Map<String,Object> config=new HashMap<>();
        if(args.containsKey("--properties")) {
            Properties supplied=new Properties();
            try(var input=Files.newInputStream(Path.of(args.get("--properties")))){supplied.load(input);}
            supplied.forEach((key,value)->config.put((String)key,value));
        }
        config.put("bootstrap.servers",args.get("--bootstrap"));
        config.put("client.id","kr-java-comparison");
        config.put("key.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
        config.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
        config.put("acks","all"); config.put("enable.idempotence",true); config.put("retries",254);
        config.put("request.timeout.ms",3000); config.put("delivery.timeout.ms",Math.toIntExact(timeout));
        config.put("max.block.ms",timeout); config.put("linger.ms",1); config.put("batch.size",16384);
        if(scenario.equals("recreate"))config.put("metadata.max.age.ms",1);
        if(rejectedByBroker)config.put("max.in.flight.requests.per.connection",1);
        if(interceptor)config.put("interceptor.classes",HeaderAudit.class.getName());
        config.put("compression.type",args.getOrDefault("--compression","none"));
        if("zstd".equals(config.get("compression.type")))config.put("compression.zstd.level",1);
        if(producerClass.startsWith("io.krkafka."))config.put("kr.transport",args.getOrDefault("--transport","readiness"));
        Properties adminProps=new Properties(); adminProps.put("bootstrap.servers",args.getOrDefault("--admin-bootstrap",args.get("--bootstrap")));
        adminProps.put("default.api.timeout.ms",Long.toString(timeout));
        org.apache.kafka.clients.admin.TopicDescription description;
        try(Admin admin=Admin.create(adminProps)){description=admin.describeTopics(List.of(topic)).allTopicNames().get(timeout,TimeUnit.MILLISECONDS).get(topic);}
        int partitions=description.partitions().size(); require(partitions>0&&partitions<=MAX_PARTITIONS,"partition bound");
        String topicId=uuid(description.topicId());
        Map<String,Object> topicIds=row("0",topicId);
        List<Object> accepted=java.util.Collections.synchronizedList(new ArrayList<>());
        List<Object> delivered=java.util.Collections.synchronizedList(new ArrayList<>());
        List<Future<RecordMetadata>> futures=new ArrayList<>(java.util.Collections.nCopies(records,null));
        List<RecordMetadata> callbackMetadata=new ArrayList<>(java.util.Collections.nCopies(records,null));
        List<Map<String,Object>> expectedById=new ArrayList<>(java.util.Collections.nCopies(records,null));
        int maximumPartitions=partitions+(scenario.equals("recreate")?1:0);
        Object[] partitionLocks=new Object[maximumPartitions]; for(int p=0;p<maximumPartitions;p++)partitionLocks[p]=new Object();
        long[] ordinals=new long[maximumPartitions], lastCallback=new long[maximumPartitions]; Arrays.fill(lastCallback,-1);
        AtomicIntegerArray callbackSeen=new AtomicIntegerArray(records);
        AtomicReference<Throwable> callbackFailure=new AtomicReference<>();
        Object callbackGate=new Object();
        Map<String,Object> ledger=row("schema","kr-kafka-java-producer-check/v1","producer_class",producerClass,"scenario",scenario,
            "config",row("run_id",run,"profile",row("topic",topic,"partitions",partitions)),"topic_ids",topicIds,
            "accepted",accepted,"deliveries",delivered,"callbacks",0,"futures",0,"complete",false,"closed",false,"joined",false,"error",null,
            "native_credit_observation","not exposed through Producer API");
        Producer<byte[],byte[]> producer=null;
        try {
            producer=(Producer<byte[],byte[]>)Class.forName(producerClass).getConstructor(Map.class).newInstance(config);
            List<PartitionInfo> snapshot=producer.partitionsFor(topic);
            require(snapshot.size()==partitions,"partitionsFor count differs from Admin");
            for(var expected:description.partitions()) {
                PartitionInfo actual=snapshot.stream().filter(p->p.partition()==expected.partition()).findFirst().orElseThrow();
                require(actual.leader()!=null&&actual.leader().id()==expected.leader().id(),"partitionsFor leader mismatch");
                require(Arrays.equals(Arrays.stream(actual.replicas()).mapToInt(n->n.id()).toArray(),expected.replicas().stream().mapToInt(n->n.id()).toArray()),"partitionsFor replicas mismatch");
                require(Arrays.equals(Arrays.stream(actual.inSyncReplicas()).mapToInt(n->n.id()).toArray(),expected.isr().stream().mapToInt(n->n.id()).toArray()),"partitionsFor ISR mismatch");
            }
            Producer<byte[],byte[]> active=producer;
            try(var workers=Executors.newFixedThreadPool(callers)) {
                List<Callable<Void>> submits=new ArrayList<>();
                for(int index=0;index<records;index++) {
                    final int id=index;
                    submits.add(()->{
                        long generation=scenario.equals("recreate")&&id>=records/3?1:0;
                        int routingPartitions=generation==1?maximumPartitions:partitions;
                        byte[] key=id%6==0?null:id%6==1?new byte[0]:new byte[]{(byte)id,0,(byte)255,(byte)(id>>>8)};
                        byte[] value=id%7==0?null:id%7==1?new byte[0]:new byte[]{(byte)id,(byte)255,0,1,2,3};
                        if(rejectedForSize){value=new byte[4096];for(int b=0;b<value.length;b++)value[b]=(byte)(id+b);}
                        Integer hint=key==null||id%6==5?id%routingPartitions:null;
                        int partition=hint!=null?hint:org.apache.kafka.common.utils.Utils.toPositive(org.apache.kafka.common.utils.Utils.murmur2(key))%routingPartitions;
                        long timestamp=Math.addExact(timestampBase,id);
                        var headers=new org.apache.kafka.common.header.internals.RecordHeaders();
                        headers.add("kr-check-run",run.getBytes(StandardCharsets.UTF_8));
                        headers.add("kr-check-id",Integer.toString(id).getBytes(StandardCharsets.UTF_8));
                        headers.add("duplicate",null);headers.add("duplicate",new byte[0]);headers.add("binary-λ",new byte[]{0,(byte)255});
                        List<Object> headerRows=new ArrayList<>();for(Header header:headers)headerRows.add(row("key",header.key(),"value_hex",hex(header.value())));
                        if(interceptor)headerRows.add(row("key","interceptor-audit","value_hex","00ff"));
                        Map<String,Object> expected=row("record_id",(long)id,"generation",generation,"expected_partition",(long)partition,"key_hex",hex(key),"value_hex",hex(value),"timestamp_ms",timestamp,"headers",headerRows);
                        expectedById.set(id,expected);
                        synchronized(partitionLocks[partition]) {
                            long ordinal=ordinals[partition]++;
                            accepted.add(expected);
                            futures.set(id,active.send(new ProducerRecord<>(topic,hint,timestamp,key,value,headers),(metadata,error)->{
                                try {
                                    require(rejectedByBroker?expectedRejection(error,scenario,producerClass):error==null,"unexpected delivery result: "+error);
                                    require(rejectedByBroker?metadata==null||metadata.partition()==partition&&!metadata.hasOffset():metadata!=null&&metadata.partition()==partition,"callback partition mismatch");
                                    synchronized(callbackGate){require(ordinal>lastCallback[partition],"per-partition callback order violated");lastCallback[partition]=ordinal;}
                                    require(callbackSeen.compareAndSet(id,0,1),"duplicate callback");
                                    callbackMetadata.set(id,metadata);
                                    delivered.add(rejectedByBroker?rejectedDelivery(id,error):
                                        row("record_id",(long)id,"kind","Acked","partition",(long)metadata.partition(),"offset",metadata.offset(),"timestamp_ms",metadata.timestamp(),"topic_id",topicIds.get(Long.toString(generation))));
                                } catch(Throwable failure){callbackFailure.compareAndSet(null,failure);}
                            }));
                        }
                        if(rejectedByBroker) {
                            boolean rejected=false;
                            try{futures.get(id).get(timeout,TimeUnit.MILLISECONDS);}catch(java.util.concurrent.ExecutionException error){rejected=expectedRejection(error.getCause(),scenario,producerClass);}
                            require(rejected,"sequential broker rejection lost its cause");
                        }
                        return null;
                    });
                }
                if(scenario.equals("restart")||scenario.equals("recreate")) {
                    int initial=records/3;
                    for(Future<Void> submitted:workers.invokeAll(submits.subList(0,initial),timeout,TimeUnit.MILLISECONDS))submitted.get();
                    producer.flush();
                    require(delivered.size()==initial&&callbackFailure.get()==null,"initial cohort failed");
                    if(scenario.equals("recreate")) {
                        for(int id=0;id<initial;id++)futures.get(id).get(timeout,TimeUnit.MILLISECONDS);
                        ledger.put("callbacks",initial);ledger.put("futures",initial);
                        Files.writeString(Path.of(args.get("--output")),json(ledger)+"\n");
                        phase("before_recreate",accepted.size(),delivered.size());
                        try(Admin admin=Admin.create(adminProps)) {
                            var replacementDescription=admin.describeTopics(List.of(topic)).allTopicNames().get(timeout,TimeUnit.MILLISECONDS).get(topic);
                            require(replacementDescription.partitions().size()==maximumPartitions,"replacement partition count");
                            String replacement=uuid(replacementDescription.topicId());
                            require(!replacement.equals(topicId),"recreated topic retained old UUID");topicIds.put("1",replacement);
                        }
                        object(object(ledger.get("config")).get("profile")).put("partitions",maximumPartitions);
                        boolean nativeProducer=producerClass.startsWith("io.krkafka."), retired=false;
                        long until=System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(timeout);
                        while(true) {
                            require(System.nanoTime()<until,"recreated topic did not retire/reopen within budget");
                            try {
                                int visiblePartitions=producer.partitionsFor(topic).size();
                                require(visiblePartitions==partitions||visiblePartitions==maximumPartitions,"recreated partition count mismatch");
                                if(visiblePartitions==maximumPartitions)break;
                            } catch(org.apache.kafka.common.KafkaException error) {
                                require(nativeProducer&&error.getClass().getName().equals("io.krkafka.producer.NativeDeliveryException")
                                    && ((Integer)error.getClass().getMethod("outcome").invoke(error))==1
                                    && ((Integer)error.getClass().getMethod("reason").invoke(error))==3,"unexpected recreation failure: "+error);
                                retired=true;
                            }
                            Thread.sleep(1);
                        }
                        ledger.put("native_retirement_observed",nativeProducer&&retired);
                        ledger.put("replacement_metadata_observed",true);
                    } else phase("before_fault",accepted.size(),delivered.size());
                    for(Future<Void> submitted:workers.invokeAll(submits.subList(initial,records),timeout,TimeUnit.MILLISECONDS))submitted.get();
                    if(scenario.equals("restart")) {
                        require(accepted.size()==records&&delivered.size()==initial,"outage cohort did not remain pending");
                        phase("after_fault",accepted.size(),delivered.size());
                    }
                } else {
                    long phaseTimeout=rejectedByBroker?Math.multiplyExact(timeout,4):timeout;
                    for(Future<Void> submitted:workers.invokeAll(submits,phaseTimeout,TimeUnit.MILLISECONDS))submitted.get();
                }
            }
            producer.flush();
            require(callbackFailure.get()==null,"callback validation failed: "+callbackFailure.get());
            int done=0;
            for(int id=0;id<records;id++) {
                require(callbackSeen.get(id)==1,"flush returned before callback");
                Future<RecordMetadata> future=futures.get(id); require(future!=null&&future.isDone(),"flush returned before future completion");
                require(!future.cancel(false),"delivery future accepted cancellation");
                if(rejectedByBroker) {
                    boolean rejected=false;
                    try{future.get(timeout,TimeUnit.MILLISECONDS);}catch(java.util.concurrent.ExecutionException error){rejected=expectedRejection(error.getCause(),scenario,producerClass);}
                    require(rejected,"broker rejection future lost its cause");done++;continue;
                }
                RecordMetadata metadata=future.get(timeout,TimeUnit.MILLISECONDS); Map<String,Object> expected=expectedById.get(id);
                RecordMetadata callback=callbackMetadata.get(id);
                require(callback!=null&&metadata.offset()==callback.offset()&&metadata.timestamp()==callback.timestamp()&&metadata.topic().equals(callback.topic()),"callback/future metadata mismatch");
                require(metadata.partition()==number(expected.get("expected_partition"))&&metadata.timestamp()>=0,"future metadata partition/time invalid");
                byte[] key=hex(expected.get("key_hex")),value=hex(expected.get("value_hex"));
                require(metadata.serializedKeySize()==(key==null?-1:key.length)&&metadata.serializedValueSize()==(value==null?-1:value.length),"future serialized size mismatch");
                done++;
            }
            if(rejectedByBroker&&delivered.stream().noneMatch(value->"Unknown".equals(object(value).get("kind"))))
                require(producer.partitionsFor(topic).size()==partitions,"producer metadata unavailable after definitive broker rejection");
            ledger.put("callbacks",delivered.size());ledger.put("futures",done);ledger.put("complete",true);
            if(interceptor)require(HeaderAudit.failure.get()==null&&HeaderAudit.sends.get()==records&&HeaderAudit.acks.get()==records&&HeaderAudit.pending.isEmpty(),"header-aware interceptor obligations failed: "+HeaderAudit.failure.get());
        } catch(Throwable error) {
            error.printStackTrace(System.err);
            while(error instanceof java.lang.reflect.InvocationTargetException&&error.getCause()!=null)error=error.getCause();
            ledger.put("error",error.toString());
        } finally {
            if(producer!=null)try{
                producer.close(Duration.ofMillis(timeout));ledger.put("closed",true);ledger.put("joined",true);
                require(callbackFailure.get()==null,"callback validation failed during close: "+callbackFailure.get());
                if(interceptor)require(HeaderAudit.failure.get()==null,"interceptor validation failed during close: "+HeaderAudit.failure.get());
                if(interceptor){require(HeaderAudit.closes.get()==1,"interceptor close count");ledger.put("interceptor",row("sends",HeaderAudit.sends.get(),"acknowledgements",HeaderAudit.acks.get(),"closes",HeaderAudit.closes.get()));}
            }catch(Throwable error){ledger.put("error",error.toString());ledger.put("complete",false);}
        }
        return ledger;
    }
    public static void main(String[] argv) throws Exception {
        if(argv.length==1&&argv[0].equals("--self-test")){selfTest();return;}
        Map<String,String> args=new HashMap<>();
        for(int i=0;i<argv.length;i+=2){require(i+1<argv.length,"missing argument");require(args.put(argv[i],argv[i+1])==null,"duplicate option");}
        if(args.containsKey("--producer-class")) {
            require(args.containsKey("--bootstrap")&&args.containsKey("--output"),"producer mode requires bootstrap/output");
            Map<String,Object> result;
            try{result=produce(args);}catch(Throwable error){result=row("schema","kr-kafka-java-producer-check/v1","complete",false,"error",error.toString());}
            Files.writeString(Path.of(args.get("--output")),json(result)+"\n");
            if(!Boolean.TRUE.equals(result.get("complete"))||result.get("error")!=null)System.exit(1);
            return;
        }
        require(args.containsKey("--bootstrap")&&args.containsKey("--ledger")&&args.containsKey("--output"),"required: --bootstrap HOST:PORT --ledger FILE --output FILE [--generation N]");
        Map<String,Object> result; boolean okay=false;
        try{result=verify(args);okay=true;}catch(Exception error){result=row("schema","kr-kafka-log-verification/v1","verified",false,"error",error.toString());}
        Files.writeString(Path.of(args.get("--output")),json(result)+"\n"); if(!okay)System.exit(1);
    }
    static void selfTest() {
        Object parsed=new Json("{\"integer\":9223372036854775807,\"array\":[null,true,\"a\\n\\u0041\"]}").parse();
        require(number(object(parsed).get("integer"))==Long.MAX_VALUE,"integer precision");
        require(json(parsed).contains("9223372036854775807"),"serialization precision");
        for(String bad:List.of("{\"x\":1,\"x\":2}","[1,]","01","1.2","\"\\uD800\"","{} trailing")) {
            boolean rejected=false;try{new Json(bad).parse();}catch(IllegalArgumentException error){rejected=true;}require(rejected,"malformed JSON accepted");
        }
        require(Arrays.equals(hex("0080ff"),new byte[]{0,(byte)128,(byte)255}),"binary hex");
        var headers=new org.apache.kafka.common.header.internals.RecordHeaders();
        headers.add("kr-check-run","fixture".getBytes(StandardCharsets.UTF_8));
        headers.add("kr-check-id","9".getBytes(StandardCharsets.UTF_8));
        headers.add("duplicate",null);headers.add("duplicate",new byte[0]);headers.add("binary",new byte[]{0,(byte)255});
        var record=new ConsumerRecord<byte[],byte[]>("fixture",2,42,123,org.apache.kafka.common.record.TimestampType.CREATE_TIME,-1,0,null,new byte[0],headers,java.util.Optional.empty());
        List<Object> expectedHeaders=new ArrayList<>();
        for(Header header:headers)expectedHeaders.add(row("key",header.key(),"value_hex",hex(header.value())));
        Map<String,Object> expected=row("expected_partition",2L,"key_hex",null,"value_hex","","timestamp_ms",123L,"headers",expectedHeaders);
        compare(record,expected);require(identity(record,"kr-check-id").equals("9"),"wire identity");
        Map<String,Object> ack=row("partition",2L,"offset",42L,"topic_id","00000000000000000000000000000001","timestamp_ms",999L);
        compareAcknowledgement(record,ack,string(ack.get("topic_id"))); // valid dedup batchmax != record CreateTime123
        for(var change:List.of(row("offset",43L),row("timestamp_ms",-1L),row("topic_id","00000000000000000000000000000002"))){
            Map<String,Object> corrupted=new HashMap<>(ack);corrupted.putAll(change);boolean rejected=false;
            try{compareAcknowledgement(record,corrupted,string(ack.get("topic_id")));}catch(IllegalArgumentException error){rejected=true;}require(rejected,"corrupt acknowledgement accepted");
        }
        for(var change:List.of(row("value_hex",null),row("key_hex",""),row("timestamp_ms",124L),row("expected_partition",3L),row("headers",expectedHeaders.subList(0,4)))){
            Map<String,Object> corrupted=new HashMap<>(expected);corrupted.putAll(change);boolean rejected=false;
            try{compare(record,corrupted);}catch(IllegalArgumentException error){rejected=true;}require(rejected,"content corruption accepted");
        }
        headers.add("kr-check-id","9".getBytes(StandardCharsets.UTF_8));boolean rejected=false;
        try{identity(record,"kr-check-id");}catch(IllegalArgumentException error){rejected=true;}require(rejected,"duplicate identity accepted");
        System.out.println("Verifier strict JSON, null/empty/binary content, timestamp, partition and header corruption checks passed");
    }
    static String json(Object value) {
        if(value==null)return "null"; if(value instanceof Number||value instanceof Boolean)return value.toString();
        if(value instanceof String text){StringBuilder b=new StringBuilder("\"");for(char c:text.toCharArray()){if(c=='"'||c=='\\')b.append('\\').append(c);else if(c<32)b.append(String.format("\\u%04x",(int)c));else b.append(c);}return b.append('"').toString();}
        StringBuilder b=new StringBuilder();
        if(value instanceof Map<?,?> map){b.append('{');for(var entry:map.entrySet()){if(b.length()>1)b.append(',');b.append(json(entry.getKey())).append(':').append(json(entry.getValue()));}return b.append('}').toString();}
        if(value instanceof List<?> list){b.append('[');for(Object item:list){if(b.length()>1)b.append(',');b.append(json(item));}return b.append(']').toString();}
        throw new IllegalArgumentException("unserializable value");
    }
    /** Strict bounded JSON subset: integer numbers only; duplicate keys rejected. */
    static final class Json {
        final String text; int at=0,nodes=0;
        Json(String text){require(text.length()<=MAX_FILE,"JSON bound");this.text=text;}
        Object parse(){Object value=value(0);space();require(at==text.length(),"trailing JSON");return value;}
        void space(){while(at<text.length()&&" \t\r\n".indexOf(text.charAt(at))>=0)at++;}
        char take(){require(at<text.length(),"truncated JSON");return text.charAt(at++);}
        boolean eat(char c){space();if(at<text.length()&&text.charAt(at)==c){at++;return true;}return false;}
        Object value(int depth){require(depth<32&&++nodes<=500000,"JSON complexity bound");space();char c=take();
            if(c=='"')return quoted();
            if(c=='{'){Map<String,Object> map=new LinkedHashMap<>();if(eat('}'))return map;do{space();require(take()=='"',"object key");String key=quoted();require(eat(':'),"object colon");require(!map.containsKey(key),"duplicate key");map.put(key,value(depth+1));}while(eat(','));require(eat('}'),"object terminator");return map;}
            if(c=='['){List<Object> list=new ArrayList<>();if(eat(']'))return list;do{list.add(value(depth+1));}while(eat(','));require(eat(']'),"array terminator");return list;}
            for(String literal:List.of("true","false","null"))if(c==literal.charAt(0)){require(text.startsWith(literal,at-1),"literal");at+=literal.length()-1;return literal.equals("null")?null:literal.equals("true");}
            int start=at-1;if(c=='-')c=take();require(c>='0'&&c<='9',"number");if(c!='0')while(at<text.length()&&Character.isDigit(text.charAt(at)))at++;
            try{return Long.parseLong(text.substring(start,at));}catch(NumberFormatException error){throw new IllegalArgumentException("integer overflow",error);}
        }
        String quoted(){StringBuilder b=new StringBuilder();while(true){char c=take();if(c=='"')break;require(c>=32,"string control");if(c=='\\'){c=take();switch(c){case '"','\\','/'->b.append(c);case 'b'->b.append('\b');case 'f'->b.append('\f');case 'n'->b.append('\n');case 'r'->b.append('\r');case 't'->b.append('\t');case 'u'->{int n=0;for(int i=0;i<4;i++){int d=Character.digit(take(),16);require(d>=0,"unicode escape");n=n*16+d;}b.append((char)n);}default->throw new IllegalArgumentException("escape");}}else b.append(c);}
            for(int i=0;i<b.length();i++){char c=b.charAt(i);if(Character.isHighSurrogate(c)){require(++i<b.length()&&Character.isLowSurrogate(b.charAt(i)),"unpaired surrogate");}else require(!Character.isLowSurrogate(c),"unpaired surrogate");}return b.toString();}
    }
}
