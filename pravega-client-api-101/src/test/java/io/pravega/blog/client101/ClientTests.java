package io.pravega.blog.client101;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.pravega.client.BatchClientFactory;
import io.pravega.client.ByteStreamClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamInfo;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.StreamSegmentsIterator;
import io.pravega.client.byteStream.ByteStreamReader;
import io.pravega.client.byteStream.ByteStreamWriter;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ClientTests {
    static final Logger log = LoggerFactory.getLogger(ClientTests.class);

    static StreamConfiguration streamConfig = StreamConfiguration.builder()
            .scalingPolicy(ScalingPolicy.fixed(1))
            .build();
    static URI controllerURI = URI.create("tcp://localhost:9090");
    static ClientConfig clientConfig = ClientConfig.builder()
            .controllerURI(controllerURI)
            .build();
    static EventWriterConfig writerConfig = EventWriterConfig.builder().build();;

    @BeforeAll
    public static void beforeTests() {
        try (StreamManager streamManager = StreamManager.create(controllerURI)) {
            streamManager.createScope("tutorial");
            streamManager.createStream("tutorial", "numbers", streamConfig);
        }
    }

    @Test
    @Order(1)
    public void writeThreeNumbers() throws InterruptedException, ExecutionException {
        EventStreamClientFactory factory = EventStreamClientFactory
                .withScope("tutorial", clientConfig);
        EventStreamWriter<Integer> writer = factory
                .createEventWriter("numbers", new JavaSerializer<Integer>(), writerConfig);
        writer.writeEvent(1);
        writer.writeEvent(2);
        writer.writeEvent(3);
        writer.close();
        factory.close();
    }

    @Test
    @Order(2)
    public void readThreeNumbers() {
        try (ReaderGroupManager manager = ReaderGroupManager.withScope("tutorial", clientConfig)) {
            ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                    .stream("tutorial/numbers")
                    .build();
            manager.createReaderGroup("numReader", readerGroupConfig);
        }
        EventStreamClientFactory factory = EventStreamClientFactory
                .withScope("tutorial", clientConfig);
        try (EventStreamReader<Integer> reader = factory
                .createReader("myId", "numReader",
                              new JavaSerializer<Integer>(), ReaderConfig.builder().build())) {
            Integer intEvent;
            while ((intEvent = reader.readNextEvent(1000).getEvent()) != null) {
                log.info("{}", intEvent);
            }
        }
        factory.close();
    }

    @Test
    @Order(3)
    public void skipThreeNumbersAndReadJustTheNextThreeNumbers() throws InterruptedException, ExecutionException {
        StreamInfo streamInfo;
        StreamCut tail; // current end of stream
        try (StreamManager streamManager = StreamManager.create(controllerURI)) {
            streamInfo = streamManager.getStreamInfo("tutorial", "numbers");
            tail = streamInfo.getTailStreamCut();
        }

        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope("tutorial", clientConfig);
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream("tutorial/numbers", tail).build();
        readerGroupManager.createReaderGroup("tailNumReader", readerGroupConfig);
        readerGroupManager.close();
        EventStreamClientFactory factory = EventStreamClientFactory
                .withScope("tutorial", clientConfig);

        EventWriterConfig writerConfig = EventWriterConfig.builder().build();
        EventStreamWriter<Integer> writer = factory
                .createEventWriter("numbers", new JavaSerializer<Integer>(), writerConfig);
        writer.writeEvent(4);
        writer.writeEvent(5);
        writer.writeEvent(6);
        writer.close();

        try (ReaderGroupManager manager = ReaderGroupManager.withScope("tutorial", clientConfig);
             ReaderGroup readerGroup = manager.getReaderGroup("tailNumReader");
             EventStreamReader<Integer> tailReader = factory
                 .createReader("tailId", "tailNumReader",
                               new JavaSerializer<Integer>(), ReaderConfig.builder().build())) {
            Integer intEvent;
            while ((intEvent = tailReader.readNextEvent(2000).getEvent()) != null
                   || readerGroup.getMetrics().unreadBytes() != 0) {
                log.info("{}", intEvent);
            }
        }
        factory.close();
    }

    @Test
    @Order(4)
    public void writeNumbersToParallelStream() throws InterruptedException, ExecutionException {
        StreamConfiguration parallelConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(5))
                .build();
        try (StreamManager streamManager = StreamManager.create(controllerURI)) {
            streamManager.createStream("tutorial", "parallel-numbers", parallelConfig);
        }
        EventWriterConfig writerConfig = EventWriterConfig.builder().build();
        EventStreamClientFactory factory = EventStreamClientFactory.withScope("tutorial", clientConfig);
        EventStreamWriter<Integer> paraWriter = factory
                .createEventWriter("parallel-numbers", new JavaSerializer<Integer>(), writerConfig);
        paraWriter.writeEvent(1);
        paraWriter.writeEvent(2);
        paraWriter.writeEvent(3);
        paraWriter.writeEvent(4);
        paraWriter.writeEvent(5);
        paraWriter.writeEvent(6);
        paraWriter.close();

        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope("tutorial", clientConfig);
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream("tutorial/parallel-numbers")
                .build();
        readerGroupManager.createReaderGroup("paraNumReader", readerGroupConfig);
        readerGroupManager.close();
        try (EventStreamReader<Integer> reader = factory
                .createReader("paraId", "paraNumReader",
                              new JavaSerializer<Integer>(), ReaderConfig.builder().build())) {
            Integer intEvent;
            while ((intEvent = reader.readNextEvent(1000).getEvent()) != null) {
                log.info("{}", intEvent);
            }
        }

        factory.close();
    }

    @Test
    @Order(5)
    public void writeDecadesToParallelStreamWithRoutingKeys() throws InterruptedException, ExecutionException {
        StreamConfiguration parallelConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(5))
                .build();
        try (StreamManager streamManager = StreamManager.create(controllerURI)) {
            streamManager.createStream("tutorial", "parallel-decades", parallelConfig);
        }
        EventWriterConfig writerConfig = EventWriterConfig.builder().build();
        EventStreamClientFactory factory = EventStreamClientFactory.withScope("tutorial", clientConfig);
        EventStreamWriter<Integer> decWriter = factory
                .createEventWriter("parallel-decades", new JavaSerializer<Integer>(), writerConfig);

        Map<String, Integer> decades = new LinkedHashMap<>();
        decades.put("ones", 0);
        decades.put("tens", 10);
        decades.put("twenties", 20);
        decades.put("thirties", 30);
        //decades.put("forties", 40);
        decades.entrySet().stream().forEach(decade -> {
            IntStream.range(decade.getValue(), decade.getValue() + 10)
            .forEachOrdered(n -> {
                decWriter.writeEvent(decade.getKey(), n);
            });
        });
        decWriter.close();

        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope("tutorial", clientConfig);
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream("tutorial/parallel-decades")
                .build();
        readerGroupManager.createReaderGroup("paraDecReader", readerGroupConfig);
        readerGroupManager.close();
        EventStreamReader<Integer> reader = factory
                .createReader("decId", "paraDecReader", new JavaSerializer<Integer>(), ReaderConfig.builder().build());

        Integer intEvent;
        while ((intEvent = reader.readNextEvent(1000).getEvent()) != null) {
            log.info("{}", intEvent);
        }

        reader.close();
        factory.close();
    }

    @Test
    @Order(6)
    public void scaleStreamAndUseBatchReader() throws InterruptedException, ExecutionException {
        try (StreamManager streamManager = StreamManager.create(controllerURI)) {
            streamManager.createScope("tutorial");
            streamManager.createStream("tutorial", "scaled-stream", streamConfig);
        }
        EventWriterConfig writerConfig = EventWriterConfig.builder().build();
        EventStreamClientFactory factory = EventStreamClientFactory.withScope("tutorial", clientConfig);
        EventStreamWriter<Integer> scaledWriter = factory
                .createEventWriter("scaled-stream", new JavaSerializer<Integer>(), writerConfig);
        scaledWriter.writeEvent(1);
        scaledWriter.writeEvent(2);
        scaledWriter.writeEvent(3);
        scaledWriter.flush();

        // scale stream
        ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(4, "executor");
        Controller controller = new ControllerImpl(ControllerImplConfig.builder()
                .clientConfig(clientConfig).build(), executor);
        StreamSegments streamSegments = controller.getCurrentSegments("tutorial", "scaled-stream").get();
        log.info("Number of segments: " + streamSegments.getSegments().size());
        long segmentId = streamSegments.getSegments().iterator().next().getSegmentId();
        log.info("Segment ID to scale: " + segmentId);
        Map<Double, Double> newKeyRanges = new HashMap<>();
        newKeyRanges.put(0.0, 0.5);
        newKeyRanges.put(0.5, 1.0);
        CompletableFuture<Boolean> scaleStream = controller
                .scaleStream(Stream.of("tutorial/scaled-stream"),
                             Collections.singletonList(segmentId),
                             newKeyRanges, executor).getFuture();

        // write 4 thru 9 to scaled stream
        if (scaleStream.join()) {
            List<CompletableFuture<Void>> writes = IntStream.range(4, 10).parallel()
                    .mapToObj(scaledWriter::writeEvent).collect(Collectors.toList());
            Futures.allOf(writes).join();
        } else {
            throw new RuntimeException("Oops, something went wrong!");
        }
        controller.close();
        scaledWriter.close();

        // get segments with batch client
        BatchClientFactory batchClient = BatchClientFactory.withScope("tutorial", clientConfig);
        StreamSegmentsIterator segments = batchClient.getSegments(Stream.of("tutorial/scaled-stream"), null, null);
        Set<SegmentIterator<Integer>> segmentIterators = new HashSet<>();
        iteratorToStream(segments.getIterator())
        .collect(Collectors.toSet())
        .parallelStream()
        .flatMap(segmentRange -> {
            log.info("Segment ID: " + segmentRange.getSegmentId());
            SegmentIterator<Integer> segmentIterator = batchClient
                    .readSegment(segmentRange, new JavaSerializer<Integer>());
            segmentIterators.add(segmentIterator);
            return iteratorToStream(segmentIterator);
        }).map(String::valueOf).forEach(log::info);
        segmentIterators.stream().forEach(SegmentIterator::close);
        batchClient.close();
    }

    private <T> java.util.stream.Stream<T> iteratorToStream(Iterator<T> itor) {
        return StreamSupport.stream(((Iterable<T>) () -> itor).spliterator(), false);
    }

    @Test
    @Order(7)
    public void byteStreamOfIntegers() throws IOException {
        try (StreamManager streamManager = StreamManager.create(controllerURI)) {
            streamManager.createScope("tutorial");
            streamManager.createStream("tutorial", "int-bytestream", streamConfig);
        }
        try (ByteStreamClientFactory byteFactory = ByteStreamClientFactory
                .withScope("tutorial", clientConfig)) {
            try (ByteStreamWriter byteWriter = byteFactory
                    .createByteStreamWriter("int-bytestream");
                 DataOutputStream outStream = new DataOutputStream(byteWriter)) {
                outStream.writeInt(1);
                outStream.writeInt(2);
                outStream.writeInt(3);
            }
            try (ByteStreamReader byteReader = byteFactory
                    .createByteStreamReader("int-bytestream");
                 DataInputStream inStream = new DataInputStream(byteReader)) {
                log.info("{}", inStream.readInt());
                log.info("{}", inStream.readInt());
                log.info("{}", inStream.readInt());
            }
        }
    }

    @Test
    @Order(Integer.MAX_VALUE)
    public void deleteReaderGroupsAndTutorialStreams() {
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope("tutorial", clientConfig)) {
            try {
                readerGroupManager.deleteReaderGroup("numReader");
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                readerGroupManager.deleteReaderGroup("tailNumReader");
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                readerGroupManager.deleteReaderGroup("paraNumReader");
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                readerGroupManager.deleteReaderGroup("paraDecReader");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        try (StreamManager streamManager = StreamManager.create(controllerURI)) {
            try {
                streamManager.sealStream("tutorial", "numbers");
                streamManager.deleteStream("tutorial", "numbers");
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                streamManager.sealStream("tutorial", "parallel-numbers");
                streamManager.deleteStream("tutorial", "parallel-numbers");
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                streamManager.sealStream("tutorial", "parallel-decades");
                streamManager.deleteStream("tutorial", "parallel-decades");
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                streamManager.sealStream("tutorial", "scaled-stream");
                streamManager.deleteStream("tutorial", "scaled-stream");
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                streamManager.sealStream("tutorial", "int-bytestream");
                streamManager.deleteStream("tutorial", "int-bytestream");
            } catch (Exception e) {
                e.printStackTrace();
            }
            streamManager.deleteScope("tutorial");
        }
    }

}
