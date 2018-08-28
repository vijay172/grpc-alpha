package com.intel.flink.state;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import com.codahale.metrics.SlidingWindowReservoir;
import com.intel.flink.datatypes.CameraTuple;
import com.intel.flink.datatypes.CameraWithCube;
import com.intel.flink.datatypes.InputMetadata;
import com.intel.flink.jni.NativeLoader;
import com.intel.flink.sources.CheckpointedCameraWithCubeSource;
import com.intel.flink.sources.CheckpointedInputMetadataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvMapWriter;
import org.supercsv.io.ICsvMapWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 *
 */
public class MapTwoStreamsDemo1 {
    private static final Logger logger = LoggerFactory.getLogger(MapTwoStreamsDemo1.class);

    private static final String ALL = "all";
    private static final String COPY = "copy";
    private static final String READ = "read";

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final int parallelCamTasks = params.getInt("parallelCam", 38);
        final int parallelCubeTasks = params.getInt("parallelCube", 1000);
        final int servingSpeedMs = params.getInt("servingSpeedMs", 33);
        final int nbrCameras = params.getInt("nbrCameras", 38);
        final long maxSeqCnt = params.getLong("maxSeqCnt", 36000);
        final int nbrCubes = params.getInt("nbrCubes", 1000);
        final int nbrCameraTuples = params.getInt("nbrCameraTuples", 19);
        final String inputFile = params.get("inputFile");
        final String outputFile = params.get("outputFile");
        final String options = params.get("options");
        final String outputPath = params.get("outputPath");
        final long timeout = params.getLong("timeout", 10000L);
        final long shutdownWaitTS = params.getLong("shutdownWaitTS", 20000);
        final int nThreads = params.getInt("nThreads", 30);
        final int nCapacity = params.getInt("nCapacity", 100);
        final Boolean local = params.getBoolean("local", false);
        final int bufferTimeout = params.getInt("bufferTimeout", 10);
        final String host = params.get("host", "localhost");
        final int port = params.getInt("port", 50051);
        final long deadlineDuration = params.getInt("deadlineDuration", 5000);
        final int sourceDelay = params.getInt("sourceDelay", 15000);
        final String action = params.get("action", ALL);//values are: copy,read,all

        logger.info("parallelCam:{}, parallelCube:{}, servingSpeedMs:{}, nbrCameras:{}, maxSeqCnt:{}, nbrCubes:{}, " +
                        "nbrCameraTuples:{}, inputFile:{}, outputFile:{}, options:{}, outputPath:{}, local: {}, " +
                        "bufferTimeout:{}, host:{}, port:{}, deadlineDuration:{}, sourceDelay:{}, action:{}",
                parallelCamTasks, parallelCubeTasks,
                servingSpeedMs, nbrCameras, maxSeqCnt, nbrCubes, nbrCameraTuples, inputFile, outputFile, options,
                outputPath, local, bufferTimeout, host, port, deadlineDuration, sourceDelay, action);
        //zipped files
        final StreamExecutionEnvironment env;
        if (local) {
            Configuration configuration = new Configuration();
            configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 2);
            env = StreamExecutionEnvironment.createLocalEnvironment(1, configuration);
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime); //EventTime not needed here as we are not dealing with EventTime timestamps
        // register the Google Protobuf serializer with Kryo
        //env.getConfig().registerTypeWithKryoSerializer(MyCustomType.class, ProtobufSerializer.class);
        //TODO: restart and checkpointing strategies
        /*env.setRestartStrategy(RestartStrategies.failureRateRestart(10, Time.minutes(1), Time.milliseconds(100)));
        env.enableCheckpointing(100);*/
        //env.enableCheckpointing(10000L, CheckpointingMode.AT_LEAST_ONCE);
        //period of the emitted Latency markers is 5 milliseconds
        //env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        env.getConfig().setLatencyTrackingInterval(5L);
        env.setBufferTimeout(bufferTimeout);
        long startTime = System.currentTimeMillis();
        DataStream<InputMetadata> inputMetadataDataStream = env
                .addSource(new CheckpointedInputMetadataSource(maxSeqCnt, servingSpeedMs, startTime, nbrCubes, nbrCameraTuples, sourceDelay, outputFile), "InputMetadata")
                .uid("InputMetadata")
                .keyBy((inputMetadata) ->
                        inputMetadata.inputMetadataKey != null ? inputMetadata.inputMetadataKey.ts : new Object());
        logger.debug("past inputMetadataFile source");

        DataStream<CameraWithCube> keyedByCamCameraStream = env
                .addSource(new CheckpointedCameraWithCubeSource(maxSeqCnt, servingSpeedMs, startTime, nbrCameras, outputFile, sourceDelay), "TileDB Camera")
                .uid("TileDB-Camera")
                .setParallelism(1);

        DataStream<CameraWithCube> cameraWithCubeDataStream;
        //for copy, only perform copyImage functionality
        if (action != null && (action.equalsIgnoreCase(ALL) || action.equalsIgnoreCase(COPY))) {
            AsyncFunction<CameraWithCube, CameraWithCube> cameraWithCubeAsyncFunction =
                    new SampleCopyAsyncFunction(host, port, shutdownWaitTS, inputFile, options, nThreads, deadlineDuration);
            String copySlotSharingGroup = "default";
            if (!local) {
                copySlotSharingGroup = "copyImage";
            }
            DataStream<CameraWithCube> cameraWithCubeDataStreamAsync =
                    AsyncDataStream.unorderedWait(keyedByCamCameraStream, cameraWithCubeAsyncFunction, timeout, TimeUnit.MILLISECONDS, nCapacity)
                            .slotSharingGroup(copySlotSharingGroup)
                            .setParallelism(parallelCamTasks)
                            .rebalance();//.startNewChain()
            cameraWithCubeDataStream = cameraWithCubeDataStreamAsync.keyBy((cameraWithCube) -> cameraWithCube.cameraKey != null ?
                    cameraWithCube.cameraKey.getTs() : new Object());
        } else {
            cameraWithCubeDataStream = keyedByCamCameraStream.keyBy((cameraWithCube) -> cameraWithCube.cameraKey != null ?
                    cameraWithCube.cameraKey.getTs() : new Object());
        }
        logger.debug("past cameraFile source");

        if (action != null && (action.equalsIgnoreCase(ALL) || action.equalsIgnoreCase(READ))) {
            String uuid = UUID.randomUUID().toString();
            DataStream<Tuple2<InputMetadata, CameraWithCube>> enrichedCameraFeed = inputMetadataDataStream
                    .connect(cameraWithCubeDataStream)
                    .flatMap(new SyncLatchFunction(outputFile, outputPath, uuid))
                    .uid("connect2Streams")
                    .setParallelism(1).startNewChain(); //TODO:is this efficient or has to be for a logically correct Latch
            logger.debug("before Read");
            String readSlotSharingGroup = "default";
            if (!local) {
                readSlotSharingGroup = "readImage";
            }
            //AsyncIo function with multiple threads for readImage
            AsyncFunction<Tuple2<InputMetadata, CameraWithCube>, Tuple2<InputMetadata, CameraWithCube>> cubeSinkAsyncFunction =
                    new SampleSinkAsyncFunction(host, port, shutdownWaitTS, outputPath, options, nThreads, uuid, deadlineDuration);
            DataStream<Tuple2<InputMetadata, CameraWithCube>> enrichedCameraFeedSinkAsync =
                    AsyncDataStream.unorderedWait(enrichedCameraFeed, cubeSinkAsyncFunction, timeout, TimeUnit.MILLISECONDS, nCapacity)
                            .slotSharingGroup(readSlotSharingGroup)
                            .setParallelism(parallelCubeTasks)
                            .uid("Read-Image-Async");
            enrichedCameraFeedSinkAsync.print();
            logger.debug("after Read");
        }

        // execute program
        long t = System.currentTimeMillis();
        logger.debug("Execution Start env Time(millis) = " + t);
        env.execute("Join InputMetadata with Camera feed using JNI & feed to CubeProcessingSink");
        logger.debug("Execution Total duration env Time = " + (System.currentTimeMillis() - t) + " millis");

    }

    /**
     * An sample of {@link AsyncFunction} using a thread pool and executing working threads
     * to simulate multiple async operations. Only 1 instance of AsyncFunction exists.
     * It is called sequentially for each record in the respective partition of the stream.
     * Unless the asyncInvoke(...) method returns fast and relies on a callback (by the client),
     * it will not result in proper asynchronous I/O.
     *
     * <p>For the real use case in production environment, the thread pool may stay in the
     * async client.
     */
    private static class SampleCopyAsyncFunction extends RichAsyncFunction<CameraWithCube, CameraWithCube> {
        private static final long serialVersionUID = 2098635244857937781L;

        private ExecutorService executorService;

        private int counter;

        private final long shutdownWaitTS;
        private final String inputFile;
        private final String options;
        private final int nThreads;
        private static NativeLoader nativeLoader;
        private final String host;
        private final int port;
        private final long deadlineDuration;

        private transient Histogram histogram;
        private transient Histogram beforeCopyDiffGeneratedHistogram;

        SampleCopyAsyncFunction(final String host, final int port, final long shutdownWaitTS, final String inputFile,
                                final String options, final int nThreads, final long deadlineDuration) {
            this.shutdownWaitTS = shutdownWaitTS;
            this.inputFile = inputFile;
            this.options = options;
            this.nThreads = nThreads;
            this.host = host;
            this.port = port;
            this.deadlineDuration = deadlineDuration;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            synchronized (SampleCopyAsyncFunction.class) {
                if (counter == 0) {
                    executorService = Executors.newFixedThreadPool(nThreads);
                    logger.debug("SampleCopyAsyncFunction - executorService starting with {} threads", nThreads);
                    nativeLoader = NativeLoader.getInstance(host, port);
                }
                ++counter;
            }
            //TODO: does it need to be within synchronized block for transient variable ??
            com.codahale.metrics.Histogram dropwizardHistogram =
                    new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500)); //TODO: what value should it be ?
            this.histogram = getRuntimeContext()
                    .getMetricGroup()
                    .addGroup("CopyMetrics")
                    .histogram("copyImageMetrics", new DropwizardHistogramWrapper(dropwizardHistogram));
            this.beforeCopyDiffGeneratedHistogram = getRuntimeContext()
                    .getMetricGroup()
                    .addGroup("DiffOfCopyAndGeneratedMetrics")
                    .histogram("diffOfCopyAndGeneratedMetrics", new DropwizardHistogramWrapper(dropwizardHistogram));

        }

        @Override
        public void close() throws Exception {

            super.close();

            synchronized (SampleCopyAsyncFunction.class) {
                --counter;
                logger.debug("counter:{}", counter);
                if (counter == 0) {
                    logger.debug("SampleCopyAsyncFunction - executorService shutting down");
                    executorService.shutdown();
                    nativeLoader.shutDown();//to shutdown channel
                    nativeLoader = null;
                    try {
                        if (!executorService.awaitTermination(shutdownWaitTS, TimeUnit.MILLISECONDS)) {
                            executorService.shutdownNow();
                        }
                    } catch (InterruptedException e) {
                        executorService.shutdownNow();
                    }
                }
            }
        }

        /**
         * Trigger async operation for each Stream input of CameraWithCube.
         *
         * @param cameraWithCube input - Emitter value
         * @param resultFuture   output async Collector of the result when query returns - Wrap Emitter in a Promise/Future
         */
        @Override
        public void asyncInvoke(final CameraWithCube cameraWithCube, final ResultFuture<CameraWithCube> resultFuture) {
            // each seq# in a diff thread
            this.executorService.submit(() -> {
                try {
                    logger.info("SampleCopyAsyncFunction - before JNI copyImage to copy S3 file to EFS");
                    //add ts/cam.jpg
                    String outputFile1 = cameraWithCube.getFileLocation();
                    logger.debug("outputFile1: {}", outputFile1);
                    long beforeCopyImageTS = System.currentTimeMillis();
                    //TODO: make it an async request through NativeLoader- currently synchronous JNI call - not reqd
                    final HashMap<String, Long> cameraWithCubeTimingMap = cameraWithCube.getTimingMap();
                    long generatedTS = cameraWithCubeTimingMap.get("Generated");
                    cameraWithCubeTimingMap.put("BeforeCopyImage", beforeCopyImageTS);
                    long diffOfCopyToGenerated = beforeCopyImageTS - generatedTS;
                    this.beforeCopyDiffGeneratedHistogram.update(diffOfCopyToGenerated);
                    if (diffOfCopyToGenerated > 500) {
                        //print error msg in log & size of queue? & AfterCopyImage -1 & emit it
                        logger.error("CopyImage Diff from BeforeCopyImage for GeneratedTS:{} is:{} which is > 500 ms", generatedTS, diffOfCopyToGenerated);
                        //size of queue?
                        cameraWithCubeTimingMap.put("AfterCopyImage", -1L);
                    } else {
                        String checkStrValue = nativeLoader.copyImage(deadlineDuration, inputFile, outputFile1, options);
                        //ERROR:FILE_OPEN:Could not open file for reading
                        //OK:1204835 bytes
                        if (checkStrValue != null && checkStrValue.startsWith("ERROR")) {
                            logger.error("Error copyImage:%s for CameraWithCube:%s", checkStrValue, cameraWithCube);
                            cameraWithCube.getTimingMap().put("Error", -1L);
                        }
                        long afterCopyImageTS = System.currentTimeMillis();
                        long timeTakenForCopyImage = afterCopyImageTS - beforeCopyImageTS;
                        this.histogram.update(timeTakenForCopyImage);
                        cameraWithCubeTimingMap.put("AfterCopyImage", afterCopyImageTS);
                        logger.info("copyImage checkStrValue: {}", checkStrValue);
                        logger.debug("SampleCopyAsyncFunction - after JNI copyImage to copy S3 file to EFS with efsLocation:{}", outputFile1);
                    }
                    //polls Queue of wrapped Promises for Completed
                    //The ResultFuture is completed with the first call of ResultFuture.complete. All subsequent complete calls will be ignored.
                    resultFuture.complete(
                            Collections.singletonList(cameraWithCube));
                } catch (Exception e) {
                    logger.error("SampleCopyAsyncFunction - Exception while making copyImage call: {}", e);
                    resultFuture.complete(new ArrayList<>(0));
                }
            });
        }
    }

    private static class SampleSinkAsyncFunction extends RichAsyncFunction<Tuple2<InputMetadata, CameraWithCube>, Tuple2<InputMetadata, CameraWithCube>> {

        private ExecutorService executorServiceRead;

        private int counter1;
        private final String options;
        private final String outputPath;
        private String fileName;
        ICsvMapWriter csvMapWriter = null;
        private final String uuid;
        private final long shutdownWaitTS;
        private final int nThreads;
        private final String host;
        private final int port;
        private final long deadlineDuration;
        private transient Histogram histogram;
        private transient Histogram diffOfAfterReadToBeforeHistogram;

        private static NativeLoader nativeLoaderRead;

        SampleSinkAsyncFunction(final String host, final int port, final long shutdownWaitTS,
                                final String outputPath, final String options, final int nThreads,
                                final String uuid, final long deadlineDuration) {
            this.options = options;
            this.outputPath = outputPath;
            this.uuid = uuid;
            this.fileName = this.outputPath + "/InputMetadata-" + this.uuid;// + ".csv";//TODO: do we need to add thread info to this ?
            this.shutdownWaitTS = shutdownWaitTS;
            this.nThreads = nThreads;
            this.host = host;
            this.port = port;
            this.deadlineDuration = deadlineDuration;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            logger.debug("SampleSinkAsyncFunction - parameters:{}", parameters);
            int inputMetaIdx = this.getRuntimeContext().getIndexOfThisSubtask();
            logger.debug("SampleSinkAsyncFunction - inputMetaIdx:{}", inputMetaIdx);
            this.fileName = fileName + "-" + inputMetaIdx + ".csv";
            logger.debug("SampleSinkAsyncFunction - fileName after inputMetaIdx: {}", fileName);
            csvMapWriter = new CsvMapWriter(new FileWriter(fileName),
                    CsvPreference.STANDARD_PREFERENCE);
            String[] header = {"ts", "cube", "cameraLst", "timingMap"};
            csvMapWriter.writeHeader(header);

            com.codahale.metrics.Histogram dropwizardHistogram =
                    new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500)); //TODO: what value should it be ?
            this.histogram = getRuntimeContext()
                    .getMetricGroup()
                    .addGroup("ReadMetrics")
                    .histogram("readImageMetrics", new DropwizardHistogramWrapper(dropwizardHistogram));
            this.diffOfAfterReadToBeforeHistogram = getRuntimeContext()
                    .getMetricGroup()
                    .addGroup("diffOfAfterReadImageToBeforeReadImageHistogramMetrics")
                    .histogram("diffOfAfterReadImageToBeforeReadImageHistogramMetrics", new DropwizardHistogramWrapper(dropwizardHistogram));

            synchronized (SampleSinkAsyncFunction.class) {
                if (counter1 == 0) {
                    executorServiceRead = Executors.newFixedThreadPool(nThreads);
                    logger.debug("SampleSinkAsyncFunction - executorService starting with {} threads", nThreads);
                    nativeLoaderRead = NativeLoader.getInstance(host, port);
                }
                ++counter1;
            }
        }

        @Override
        public void close() throws Exception {

            super.close();

            if (csvMapWriter != null) {
                try {
                    csvMapWriter.close();
                } catch (IOException ex) {
                    logger.error("Error closing the inputMetadata writer: " + ex);
                }
            }

            synchronized (SampleSinkAsyncFunction.class) {
                --counter1;
                logger.debug("counter1:{}", counter1);
                if (counter1 == 0) {
                    logger.debug("SampleSinkAsyncFunction - executorService shutting down");
                    executorServiceRead.shutdown();
                    nativeLoaderRead.shutDown();//to shutdown channel
                    nativeLoaderRead = null;
                    try {
                        if (!executorServiceRead.awaitTermination(shutdownWaitTS, TimeUnit.MILLISECONDS)) {
                            executorServiceRead.shutdownNow();
                        }
                    } catch (InterruptedException e) {
                        executorServiceRead.shutdownNow();
                    }
                }
            }
        }

        /**
         * Supply a tuple2 back after calling readImage using the provided ExecutirThread from the thread pool.
         *
         * @param tuple2              Tuple2<InputMetadata, CameraWithCube>
         * @param cameraTuple         CameraTuple
         * @param executorServiceRead ExecutorService provided to use to get a thread from a Thread pool
         * @return inputMetadata Tuple2<<InputMetadata,CameraWithCube>>
         */
        private CompletableFuture<Tuple2<InputMetadata, CameraWithCube>> readImageAsync(final Tuple2<InputMetadata, CameraWithCube> tuple2, final CameraTuple cameraTuple, final ExecutorService executorServiceRead) {
            logger.debug("Entered SampleSinkAsyncFunction.readImageAsync()");
            return CompletableFuture.supplyAsync(() -> {
                try {
                    String camFileLocation = cameraTuple.getCamFileLocation();
                    logger.info("SampleSinkAsyncFunction - before JNI readImage to retrieve EFS file from camFileLocation: {}", camFileLocation);
                    logger.debug("tuple2.f0:{}, tuple2.f1:{}, options:{}", tuple2.f0, tuple2.f1, options);
                    InputMetadata inputMetadata = tuple2.f0;
                    final HashMap<String, Long> inputMetadataTimingMap = inputMetadata.getTimingMap();
                    long generatedTS = inputMetadataTimingMap.get("Generated");
                    long startOfReadImageTS = System.currentTimeMillis();
                    /*
                    final CameraWithCube cameraWithCube = tuple2.f1;
                    final HashMap<String, Long> cameraWithCubeTimingMap = cameraWithCube.getTimingMap();
                    long afterCopyImageTS = cameraWithCubeTimingMap.get("AfterCopyImage");
                    long timeAfterCopyToStartOfReadImage = startOfReadImageTS - afterCopyImageTS;
                    this.histogram.update(timeAfterCopyToStartOfReadImage);
                    inputMetadataTimingMap.put("BeforeReadImage", startOfReadImageTS);*/

                    long diff = startOfReadImageTS - generatedTS;
                    if (diff > 20500) { //TODO: diff changed from 500
                        logger.error("ReadImage Diff for GeneratedTS:{} is:{} which is > 500 ms", generatedTS, diff);
                        //TODO: size of queue?
                        //inputMetadataTimingMap.put("AfterReadImage", -1L);
                    } else {
                        String checkStrValue = nativeLoaderRead.readImage(deadlineDuration, camFileLocation, 0, 0, 10, 5, options);
                        if (checkStrValue != null && checkStrValue.startsWith("ERROR")) {
                            logger.error("Error readImage:%s for InputMetadata:%s", checkStrValue, tuple2.f0);
                            inputMetadataTimingMap.put("Error", -1L);
                        }
                        //inputMetadataTimingMap.put("AfterReadImage", System.currentTimeMillis());
                        //writeInputMetadataCsv(tuple2);
                        logger.info("readImage checkStrValue: {}", checkStrValue);
                        logger.debug("SampleSinkAsyncFunction - after JNI readImage to retrieve EFS file from camFileLocation: {}", camFileLocation);
                    }
                    //The ResultFuture is completed with the first call of ResultFuture.complete. All subsequent complete calls will be ignored.
                    return tuple2;
                } catch (Exception e) {
                    logger.error("SampleSinkAsyncFunction - Exception while making readImage call in async call: {}", e);
                    return new Tuple2<>(); //TODO:???
                }
            }, executorServiceRead);

            /* .exceptionally(ex -> {
                logger.error("SampleSinkAsyncFunction - Exception while making readImage call in async call: {}", ex);
                return new Tuple2<InputMetadata, CameraWithCube>(new InputMetadata(), new CameraWithCube()); //TODO:???
            })*/
        }

        @Override
        public void asyncInvoke(final Tuple2<InputMetadata, CameraWithCube> tuple2, final ResultFuture<Tuple2<InputMetadata, CameraWithCube>> resultFuture) {
            logger.debug("Entered SampleSinkAsyncFunction.asyncInvoke()");
            //read all 38 cameras for 1 cube in 38 threads
            //tuple2.f0.cameraLst[i].camFileLocation for each 38 cameras within 1 cube/InputMetadata
            try {
                InputMetadata inputMetadata = tuple2.f0;
                final HashMap<String, Long> inputMetadataTimingMap = inputMetadata.getTimingMap();

                final CameraWithCube cameraWithCube = tuple2.f1;
                final HashMap<String, Long> cameraWithCubeTimingMap = cameraWithCube.getTimingMap();
                logger.debug("cameraWithCubeTimingMap:{}", cameraWithCubeTimingMap);
                long afterCopyImageTS = cameraWithCubeTimingMap != null ?
                        cameraWithCubeTimingMap.get("AfterCopyImage") != null ? cameraWithCubeTimingMap.get("AfterCopyImage") : System.currentTimeMillis()
                        : System.currentTimeMillis();//TODO: what to put here when cameraWithCubeTimingMap = null
                long startOfReadImageTS = System.currentTimeMillis();
                long timeAfterCopyToStartOfReadImage = startOfReadImageTS - afterCopyImageTS;
                this.histogram.update(timeAfterCopyToStartOfReadImage);
                inputMetadataTimingMap.put("BeforeReadImage", startOfReadImageTS);

                final List<CameraTuple> cameraTupleList = tuple2.f0.cameraLst;
                //read all camera images within cameraLst of InputMetadata async
                final List<CompletableFuture<Tuple2<InputMetadata, CameraWithCube>>> completableReadFutures = cameraTupleList.stream()
                        .map(cameraTuple -> readImageAsync(tuple2, cameraTuple, executorServiceRead))
                        .collect(Collectors.toList());
                //Ugly piece of java - crap Async API
                //Create a combined Future using allOf which return Void
                final CompletableFuture<Void> allCompletableReadFutures = CompletableFuture.allOf(completableReadFutures
                        .toArray(new CompletableFuture[completableReadFutures.size()]));
                //Need to do below as CompletableFuture.allOf() returns CompletableFuture<Void>
                //When all Futures are completed, call `future.join()` to get their results & collect the results in a List
                //join is called when all futures are complete, so no blocking anywhere
                //TODO: join throws an unchecked exception if the underlying CompletableFuture completes exceptionally.
                final CompletableFuture<List<Tuple2<InputMetadata, CameraWithCube>>> allReadFutures = allCompletableReadFutures.thenApply(v -> {
                    return completableReadFutures.stream()
                            .map(completableReadFuture -> completableReadFuture.join())
                            .collect(Collectors.toList());
                });
                //return input Tuple2 as ResultFuture saying all parallel threads completed
                //hence use thenAccept which accepts a Consumer with args with nothing returned
                allReadFutures.thenAccept(inputTupleList -> {
                    logger.info("All read image futures completed for camera list within Inputmetadata");
                    long afterReadImageTS = System.currentTimeMillis();
                    inputMetadataTimingMap.put("AfterReadImage", afterReadImageTS);
                    long diffOfAfterReadToBeforeReadTS = afterReadImageTS - startOfReadImageTS;
                    this.diffOfAfterReadToBeforeHistogram.update(diffOfAfterReadToBeforeReadTS);
                    writeInputMetadataCsv(tuple2);
                    resultFuture.complete(
                            Collections.singletonList(tuple2));
                });

                /*final CompletableFuture<Void> allCompletableReadFutures = CompletableFuture.allOf(completableReadFutures
                        .toArray(new CompletableFuture[completableReadFutures.size()]));
                allCompletableReadFutures.get();//blocking
                logger.info("All read image futures completed for camera list within Inputmetadata");
                inputMetadataTimingMap.put("AfterReadImage", System.currentTimeMillis());
                resultFuture.complete(
                        Collections.singletonList(tuple2));*/

            } catch (Exception e) {
                logger.error("SampleSinkAsyncFunction - Exception while making readImage call in async call: {}", e);
                resultFuture.complete(new ArrayList<>(0));
            }

            /*for (CameraTuple cameraTuple :
                    tuple2.f0.cameraLst) {
                String camFileLocation = cameraTuple.getCamFileLocation();

                this.executorServiceRead.submit(() -> {
                    try {
                        logger.info("SampleSinkAsyncFunction - before JNI readImage to retrieve EFS file from camFileLocation: {}", camFileLocation);
                        logger.debug("tuple2.f0:{}, tuple2.f1:{}, options:{}", tuple2.f0, tuple2.f1, options);
                        //TODO: change to use ROI values ??
                        InputMetadata inputMetadata = tuple2.f0;
                        final HashMap<String, Long> inputMetadataTimingMap = inputMetadata.getTimingMap();
                        long generatedTS = inputMetadataTimingMap.get("Generated");
                        long t = System.currentTimeMillis();
                        inputMetadataTimingMap.put("BeforeReadImage", t);
                        long diff = t - generatedTS;
                        if (diff > 500) {
                            logger.error("ReadImage Diff for GeneratedTS:{} is:{} which is > 500 ms", generatedTS, diff);
                            //TODO: size of queue?
                            inputMetadataTimingMap.put("AfterReadImage", -1L);
                        } else {
                            String checkStrValue = nativeLoaderRead.readImage(deadlineDuration, camFileLocation, 0, 0, 10, 5, options);
                            inputMetadataTimingMap.put("AfterReadImage", System.currentTimeMillis());
                            writeInputMetadataCsv(tuple2);
                            logger.info("readImage checkStrValue: {}", checkStrValue);
                            logger.debug("SampleSinkAsyncFunction - after JNI readImage to retrieve EFS file from camFileLocation: {}", camFileLocation);
                        }
                        //The ResultFuture is completed with the first call of ResultFuture.complete. All subsequent complete calls will be ignored.
                        resultFuture.complete(
                                Collections.singletonList(tuple2));
                    } catch (Exception e) {
                        logger.error("SampleSinkAsyncFunction - Exception while making readImage call in async call: {}", e);
                        resultFuture.complete(new ArrayList<>(0));
                    }
                });
                //TODO: how to wait for all 38 cameras to complete with async calls on separate threads
            }*/
        }

        private void writeInputMetadataCsv(final Tuple2<InputMetadata, CameraWithCube> tuple2) {
            logger.info("SampleSinkAsyncFunction - writeInputMetadataCsv()");
            InputMetadata inputMetadata = tuple2.f0;
            InputMetadata.InputMetadataKey inputMetadataKey = inputMetadata.inputMetadataKey;
            // create mapper and schema
            CellProcessor[] processors = new CellProcessor[]{
                    new NotNull(), // ts
                    new NotNull(), // cube
                    new NotNull(), // cameraLst
                    new NotNull() // timingMap
            };

            try {
                String[] header = {"ts", "cube", "cameraLst", "timingMap"};
                final Map<String, Object> inputMetadataRow = new HashMap<>();
                inputMetadataRow.put(header[0], inputMetadataKey.ts);
                inputMetadataRow.put(header[1], inputMetadataKey.cube);
                inputMetadataRow.put(header[2], inputMetadata.cameraLst);
                inputMetadataRow.put(header[3], inputMetadata.timingMap);

                csvMapWriter.write(inputMetadataRow, header, processors);

            } catch (IOException ex) {
                logger.error("Error writing the inputMetadata CSV file: " + ex);
            } finally {
            }
        }

    }

    /**
     * SyncLatchFunction FlatMap function for keyed managed store.
     * Latch only  allows Cube InputMetadata to flow through when all the corrresponding camera input arrives.
     * Stores Cube InputMetadata in memory buffer till all the camera data arrives.
     */
    static final class SyncLatchFunction extends RichCoFlatMapFunction<InputMetadata, CameraWithCube, Tuple2<InputMetadata, CameraWithCube>> {
        private final String outputFile;
        private final String outputPath;
        private final String fileName;
        ICsvMapWriter csvCameraMapWriter = null;
        private final String uuid;

        SyncLatchFunction(final String outputFile, final String outputPath, final String uuid) {
            this.outputFile = outputFile;
            this.outputPath = outputPath;
            this.uuid = uuid;
            fileName = outputPath + "/Camera-" + this.uuid + ".csv";
        }

        private transient Meter meter;
        //keyed, managed state
        //1,cu1, [(cam1,roi1),(cam2,roi2)...],count
        private MapState<InputMetadata.InputMetadataKey, InputMetadata> inputMetadataState;
        //1,cam1,fileName
        private MapState<CameraWithCube.CameraKey, CameraWithCube> cameraWithCubeState;

        /**
         * Register all State declaration.
         *
         * @param config Configuration
         */
        @Override
        public void open(Configuration config) throws Exception {
            logger.debug("SyncLatchFunction Entered open");
            MapStateDescriptor<InputMetadata.InputMetadataKey, InputMetadata> inputMetadataMapStateDescriptor =
                    new MapStateDescriptor<>("inputMetadataState",
                            InputMetadata.InputMetadataKey.class, InputMetadata.class);
            inputMetadataState = getRuntimeContext().getMapState(inputMetadataMapStateDescriptor);
            MapStateDescriptor<CameraWithCube.CameraKey, CameraWithCube> cameraMapStateDescriptor =
                    new MapStateDescriptor<>("cameraWithCubeState",
                            CameraWithCube.CameraKey.class, CameraWithCube.class);
            cameraWithCubeState = getRuntimeContext().getMapState(cameraMapStateDescriptor);
            this.meter = getRuntimeContext()
                    .getMetricGroup()
                    .addGroup("MyMetrics")
                    .meter("syncLatchMeter", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
            csvCameraMapWriter = new CsvMapWriter(new FileWriter(fileName),
                    CsvPreference.STANDARD_PREFERENCE);
            String[] header = {"ts", "cam", "timingMap"};
            csvCameraMapWriter.writeHeader(header);
        }

        @Override
        public void close() throws Exception {
            logger.debug("SyncLatchFunction Entered close");
            super.close();
            inputMetadataState.clear();

            if (csvCameraMapWriter != null) {
                try {
                    csvCameraMapWriter.close();
                } catch (IOException ex) {
                    logger.error("Error closing the camera writer: " + ex);
                }
            }
        }

        /**
         * Data comes in from Input Metadata with (TS1,Cube1) as key with values [Camera1, Camera2], count = 2
         * Insert into InputMetadata state 1st.
         * Then check CameraWithCubeState with key (TS1, Camera1) key for existence in Camera(cameraWithCubeState) state.
         * If it doesn't exist, insert into CameraWithCube state TS1, Camera1 as key, values- [CU1], tileExists=false
         * if Camera row exists with tileExists=true(Tile Camera input exists), reduce count for (TS1,CU1) etc in a loop for all cubeLst entries.
         * if Camera row exists with tileExists=false (No Tile Camera input), update Camera state with new CU2 in cubeLst entry.
         *
         * @param inputMetadata incoming InputMetadata
         * @param collector     Collects data for output
         * @throws Exception Exception thrown
         */
        @Override
        public void flatMap1(InputMetadata inputMetadata, Collector<Tuple2<InputMetadata, CameraWithCube>> collector) throws Exception {
            long t2 = System.currentTimeMillis();
            logger.info("SyncLatchFunction- Entered flatMap1 with inputMetadata:{} count:{} with timestamp:{}", inputMetadata, inputMetadata.count, t2);
            inputMetadata.getTimingMap().put("IntoLatch", t2);
            final List<CameraTuple> inputMetaCameraLst = inputMetadata.cameraLst;
            inputMetadata.count = inputMetaCameraLst != null ? inputMetaCameraLst.size() : 0L;
            final InputMetadata.InputMetadataKey inputMetadataKey = inputMetadata.inputMetadataKey; //(1,CU1)
            final long inputMetaTs = inputMetadataKey != null ? inputMetadataKey.ts : 0L;
            final String inputMetaCube = inputMetadataKey != null ? inputMetadataKey.cube : null;
            if (inputMetaCube == null) {
                return;
            }
            //Insert into InputMetadata state 1st.
            inputMetadataState.put(inputMetadataKey, inputMetadata);
            //check in a loop for incoming inputMetadata with TS1, C1 key against existing Camera state data - cameraWithCube Map entries
            Iterator<CameraTuple> inputMetaCameraLstIterator = inputMetaCameraLst != null ? inputMetaCameraLst.iterator() : null;
            for (; inputMetaCameraLstIterator != null && inputMetaCameraLstIterator.hasNext(); ) {
                CameraTuple inputMetaCam = inputMetaCameraLstIterator.next();
                String inputMetaCamera = inputMetaCam != null ? inputMetaCam.getCamera() : null;
                //TS1,C1
                CameraWithCube.CameraKey cameraKeyFromInputMetadata = new CameraWithCube.CameraKey(inputMetaTs, inputMetaCamera);
                //check with key in cameraWithCubeState
                CameraWithCube cameraWithCube = cameraWithCubeState.get(cameraKeyFromInputMetadata);
                if (cameraWithCube != null) {
                    //key exists - hence check if tileExists
                    if (cameraWithCube.tileExists) {
                        logger.debug("[flatMap1] inputMetadata cameraWithCube tileExists:{}", cameraWithCube);
                        //reduce count in inputMetadata for TS1,CU1
                        List<String> existingCameraWithCubeLst = cameraWithCube.cubeLst;
                        //if tile exists & empty cubeLst, then reduce inputMetadata state count for ts1,cu1 by 1
                        if (existingCameraWithCubeLst != null && existingCameraWithCubeLst.size() == 0) {
                            //TODO: DUPLICATE CODE - REFACTOR LATER
                            final InputMetadata.InputMetadataKey existingMetadataKey = new InputMetadata.InputMetadataKey(inputMetaTs, inputMetaCube); //(TS1,CU1), (TS1,CU2)
                            final InputMetadata existingInputMetadata = inputMetadataState.get(existingMetadataKey); //(1,CU1)
                            if (existingInputMetadata != null) {
                                //reduce count by 1 for inputMetadata state
                                existingInputMetadata.count -= 1;
                                inputMetadataState.put(existingMetadataKey, existingInputMetadata);

                                if (existingInputMetadata.count == 0) {
                                    logger.info("$$$$$[flatMap1]Release Countdown latch with inputMetadata Collecting existingInputMetadata:{}, cameraWithCube:{}", existingInputMetadata, cameraWithCube);
                                    long t = System.currentTimeMillis();
                                    existingInputMetadata.getTimingMap().put("OutOfLatch", t);

                                    Tuple2<InputMetadata, CameraWithCube> tuple2 = new Tuple2<>(existingInputMetadata, cameraWithCube);
                                    collector.collect(tuple2);

                                } else {
                                    logger.debug("!!!!![flatMap1]  with inputMetadata reducing count:{} ,existingInputMetadata:{}, cameraWithCube:{}", existingInputMetadata.count, existingInputMetadata, cameraWithCube);
                                }
                            }
                        } else {
                            //reduce count for (1,CU1) from InputMetadata etc in a loop for all cubeLst entries.
                            Iterator<String> existingCameraWithCubeIterator = Objects.requireNonNull(existingCameraWithCubeLst).iterator();
                            for (; existingCameraWithCubeIterator.hasNext(); ) {
                                String existingCameraWithCube = existingCameraWithCubeIterator.next(); //CU1, CU2
                                // if incoming inputMetadata's cu1 (inputCube) matches existingCube, remove from cameraWithCubeState's cubeLst
                                //if cameraWithCubeState's cubeLst's size is 0, remove key from cameraWithCubeState
                                if (existingCameraWithCube != null && existingCameraWithCube.equals(inputMetaCube)) {
                                    //TODO: do we need to do this - remove existingCube
                                    existingCameraWithCubeIterator.remove();
                                }
                                //for tile exists condition, reduce count for [1,cu1] key of inputMetadataState
                                final InputMetadata.InputMetadataKey existingMetadataKey = new InputMetadata.InputMetadataKey(inputMetaTs, inputMetaCube); //(1,CU1), (1,CU2)
                                final InputMetadata existingInputMetadata = inputMetadataState.get(existingMetadataKey); //(1,CU1)
                                if (existingInputMetadata != null) {
                                    //reduce count by 1 for inputMetadata state
                                    existingInputMetadata.count -= 1;
                                    inputMetadataState.put(existingMetadataKey, existingInputMetadata);

                                    if (existingInputMetadata.count == 0) {
                                        logger.info("$$$$$[flatMap1] Release Countdown latch with inputMetadata Collecting existingInputMetadata:{}, cameraWithCube:{}", existingInputMetadata, cameraWithCube);
                                        long t = System.currentTimeMillis();
                                        existingInputMetadata.getTimingMap().put("OutOfLatch", t);
                                        //cameraWithCube.getTimingMap().put("OutOfLatch", t);
                                        Tuple2<InputMetadata, CameraWithCube> tuple2 = new Tuple2<>(existingInputMetadata, cameraWithCube);
                                        collector.collect(tuple2);

                                    } else {
                                        logger.debug("$$$$$[flatMap1]  with inputMetadata reducing count existingInputMetadata.count:{}, cameraWithCube:{}", existingInputMetadata.count, cameraWithCube);
                                    }
                                }

                            }
                        }

                        if (existingCameraWithCubeLst.size() == 0) {
                            //if no cubeLst and tileExists=true, remove the key from the camera state ???
                            //TODO: Use ProcessFunction with a timer to remove the camera data after a certain stipulated time
                            // cameraWithCubeState.remove(cameraKeyFromInputMetadata);
                            logger.info("[flatMap1] inputMetadata if no cubeLst and tileExists=true, remove the cameraKeyFromInputMetadata key from the camera state :{}", cameraKeyFromInputMetadata);
                        } else {
                            //update state with reduced cubeLst
                            cameraWithCubeState.put(cameraKeyFromInputMetadata, cameraWithCube);
                        }
                    } else {
                        //update into CameraWithCube with updated cubeLst containing new inputCube from inputMetadata
                        List<String> existingCubeLst = cameraWithCube.cubeLst;
                        if (existingCubeLst == null) {
                            existingCubeLst = new ArrayList<>();
                        }

                        if (!existingCubeLst.contains(inputMetaCube)) {
                            existingCubeLst.add(inputMetaCube);
                        }

                        cameraWithCube.getTimingMap().put("IntoLatch", t2);
                        cameraWithCubeState.put(cameraKeyFromInputMetadata, cameraWithCube);
                    }
                } else {
                    //insert into CameraWithCube with tileExists=false - i.e waiting for TileDB camera input to come in
                    List<String> newCubeLst = new ArrayList<>(Collections.singletonList(inputMetaCube));
                    CameraWithCube newCameraWithCube = new CameraWithCube(inputMetaTs, Objects.requireNonNull(inputMetaCam).getCamera(), newCubeLst, false, outputFile);
                    newCameraWithCube.getTimingMap().put("IntoLatch", t2);
                    cameraWithCubeState.put(cameraKeyFromInputMetadata, newCameraWithCube);
                }
            }
            this.meter.markEvent();
        }

        /**
         * Data comes in from the Camera feed(cameraWithCube) with (TS1, Camera1) as key
         * Check if key exists in CameraWithCubeState
         * If camera key doesn't exist, insert into CameraWithCubeState with key & value- { empty cubeLst and tileExists= true }
         * If camera key exists, update CameraWithCubeState with key & value having tileExists= true
         * For camera key exists, check if value has a non-empty cubeLst. If no, stop.
         * If value has a non-empty cubeLst, reduce count for (TS1,CU1) etc in a loop for all cubeLst entries of Camera feed
         *
         * @param cameraWithCube Incoming Camera data
         * @param collector      Output collector
         * @throws Exception Exception thrown
         */
        @Override
        public void flatMap2(CameraWithCube cameraWithCube, Collector<Tuple2<InputMetadata, CameraWithCube>> collector) throws Exception {
            long t1 = System.currentTimeMillis();
            //TS1, C1
            logger.info("SyncLatchFunction- Entered flatMap2 with Camera data:{} with timestamp:{}", cameraWithCube, t1);
            //start TS cameIntoLatch
            cameraWithCube.getTimingMap().put("IntoLatch", t1);
            writeCameraCsv(cameraWithCube);
            final CameraWithCube.CameraKey cameraKey = cameraWithCube.cameraKey;
            final long cameraTS = cameraKey.ts;
            final String cameraKeyCam = cameraKey.getCam();
            final CameraWithCube existingCameraWithCube = cameraWithCubeState.get(cameraKey);
            if (existingCameraWithCube != null) {
                boolean tileExists = existingCameraWithCube.tileExists;
                final List<String> existingCubeLst = existingCameraWithCube.cubeLst;
                if (!tileExists) {
                    //update tileExists to true in camera state
                    existingCameraWithCube.tileExists = true;
                    cameraWithCubeState.put(cameraKey, existingCameraWithCube);
                }
                //if cubeLst exists
                logger.debug("[flatMap2] cameraWithCube existingCubeLst:{}", existingCubeLst);
                if (existingCubeLst != null && existingCubeLst.size() > 0) {
                    for (String existingCube : existingCubeLst) { //CU1, CU2
                        final InputMetadata.InputMetadataKey existingMetadataKey = new InputMetadata.InputMetadataKey(cameraTS, existingCube); //(TS1,CU1), (TS1,CU2)
                        final InputMetadata existingInputMetadata = inputMetadataState.get(existingMetadataKey);
                        if (existingInputMetadata != null) {
                            List<CameraTuple> existingInputMetaCameraLst = existingInputMetadata.cameraLst;
                            for (CameraTuple existingInputMetaCam : existingInputMetaCameraLst) {
                                String existingInputMetaCamStr = existingInputMetaCam.getCamera();
                                if (existingInputMetaCamStr != null && existingInputMetaCamStr.equals(cameraKeyCam)) {
                                    //want to keep existing inputMetaData & not remove incoming camera from cameraLst of inputMetadata state
                                    existingInputMetadata.count -= 1;
                                }
                            }

                            if (existingInputMetadata.count == 0) {
                                logger.info("$$$$$[flatMap2] Release Countdown latch with Camera data Collecting existingInputMetadata:{}, cameraWithCube:{}", existingInputMetadata, cameraWithCube);
                                long t = System.currentTimeMillis();
                                existingInputMetadata.getTimingMap().put("OutOfLatch", t);
                                Tuple2<InputMetadata, CameraWithCube> tuple2 = new Tuple2<>(existingInputMetadata, cameraWithCube);
                                collector.collect(tuple2);
                            } else {
                                //updated reduced count in inputMetadata
                                inputMetadataState.put(existingMetadataKey, existingInputMetadata);
                                logger.debug("$$$$$[flatMap2] with Camera data reducing count of existingInputMetadata:{}", existingInputMetadata);
                            }
                        }
                    }
                }
            } else {
                //insert into CameraWithCubeState with key & value- { empty cubeLst and tileExists= true }
                CameraWithCube newCameraWithCube = new CameraWithCube(cameraKey, Collections.emptyList(), true, outputFile);
                cameraWithCubeState.put(cameraKey, newCameraWithCube);
            }
            this.meter.markEvent();
        }

        private void writeCameraCsv(CameraWithCube cameraWithCube) {
            logger.info("writeCameraCsv start with cameraWithCube:{}", cameraWithCube);
            CameraWithCube.CameraKey cameraKey = cameraWithCube.cameraKey;
            // create mapper and schema
            //ICsvMapWriter csvCameraMapWriter = null;
            CellProcessor[] processors = new CellProcessor[]{
                    new NotNull(), // ts
                    new NotNull(), // cam
                    new NotNull() // timingMap
            };

            try {
                String[] header = {"ts", "cam", "timingMap"};
                final Map<String, Object> cameraRow = new HashMap<>();
                cameraRow.put(header[0], cameraKey.ts);
                cameraRow.put(header[1], cameraKey.cam);
                cameraRow.put(header[2], cameraWithCube.timingMap);

                csvCameraMapWriter.write(cameraRow, header, processors);
            } catch (IOException ex) {
                logger.error("Error writing the camera CSV file: " + ex);
            } finally {
            }
        }
    }


}
