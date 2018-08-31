package com.intel.flink.sources;

import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.intel.flink.datatypes.CameraTuple;
import com.intel.flink.datatypes.InputMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Reads InputMetadata records from a source csv file and emits a stream of InputMetadata events.
 * Source operates in event-time.
 *
 */
public class CheckpointedInputMetadataSource implements SourceFunction<InputMetadata>, ListCheckpointed<Long>, CheckpointListener {
    private static final Logger logger = LoggerFactory.getLogger(CheckpointedInputMetadataSource.class);

    private final long maxSeqCnt;
    //number of cubes being processed for every seq
    private final int nbrCubes;
    //number of camera tuples created for each cube
    private final int nbrCameraTuples;
    //An event generated every servingSpeedMs
    private final int servingSpeedMs;
    //Initial kickoff time passed in
    private final long currentTimeMs;
    //delay source by millisec
    private final int sourceDelay;
    //camera file location
    private final String fileLocation;

    private static final int NBR_OF_CUBES = 1000;
    private static final long MAX_SEQ_CNT = 36000; //20 mins * 60 secs/min * 30 frames/sec
    private static final int SERVING_SPEED_FREQ_MILLIS = 33; //event generated in msec every 1000 / (30 frames per sec)
    private static final String CUBE = "cu";
    private static final String CAM = "cam";
    private static final String ROI = "roi";
    private static final int NBR_OF_CAMERA_TUPLES = 19;
    private static final int SOURCE_DELAY = 15000;
    private static final String FILE_LOCATION = "file:///tmp";


    private volatile boolean running = true;

    // state
    // number of emitted events
    private long eventCnt = 0;

    public CheckpointedInputMetadataSource() {
        this(MAX_SEQ_CNT, SERVING_SPEED_FREQ_MILLIS, System.currentTimeMillis(), NBR_OF_CUBES, NBR_OF_CAMERA_TUPLES, SOURCE_DELAY, FILE_LOCATION);
    }

    public CheckpointedInputMetadataSource(long maxSeqCnt, int servingSpeedMs) {
        this(maxSeqCnt, servingSpeedMs, System.currentTimeMillis(), NBR_OF_CUBES, NBR_OF_CAMERA_TUPLES, SOURCE_DELAY, FILE_LOCATION);
    }

    public CheckpointedInputMetadataSource(long currentTimeMs) {
        this(MAX_SEQ_CNT, SERVING_SPEED_FREQ_MILLIS, currentTimeMs, NBR_OF_CUBES, NBR_OF_CAMERA_TUPLES, SOURCE_DELAY, FILE_LOCATION);
    }

    public CheckpointedInputMetadataSource(long maxSeqCnt, int servingSpeedMs, long currentTimeMs, int nbrCubes,
                                           int nbrCameraTuples, int sourceDelay, String fileLocation) {
        if (maxSeqCnt < 0) {
            throw new IllegalArgumentException("Max sequence count must be positive");
        } else if (servingSpeedMs < 0) {
            throw new IllegalArgumentException("Serving speed in millisec must be positive");
        } else if (currentTimeMs < 0) {
            throw new IllegalArgumentException("Current time in millisec must be positive");
        } else if (nbrCubes < 0) {
            throw new IllegalArgumentException("Number of cubes must be positive");
        } else if (nbrCameraTuples < 0) {
            throw new IllegalArgumentException("Number of camera tuples per cube must be positive");
        } else if (sourceDelay < 0) {
            throw new IllegalArgumentException("SourceDelay must be positive");
        }
        this.maxSeqCnt = maxSeqCnt;
        this.currentTimeMs = currentTimeMs;
        this.servingSpeedMs = servingSpeedMs;
        this.nbrCubes = nbrCubes;
        this.nbrCameraTuples = nbrCameraTuples;
        this.sourceDelay = sourceDelay;
        this.fileLocation = fileLocation;
    }

    @Override
    public void run(SourceContext<InputMetadata> sourceContext) throws Exception {
        final Object lock = sourceContext.getCheckpointLock();
        final InputMetadata inputMetadata = new InputMetadata();
        final InputMetadata.InputMetadataKey inputMetadataKey = new InputMetadata.InputMetadataKey();

        long seqCnt = 0;
        long cnt = 0;
        //skip emitted events using seqCnt
        while (cnt < eventCnt) {
            cnt++;
            seqCnt++;
        }
        //emit all subsequent events from seqCnt onwards
        while (running) {
            for (; seqCnt < maxSeqCnt; seqCnt++) {
                inputMetadataKey.ts = seqCnt;
                for (int cubeCnt = 1; cubeCnt <= nbrCubes ; cubeCnt++) {
                    inputMetadataKey.cube = CUBE + cubeCnt;
                    inputMetadata.setInputMetadataKey(inputMetadataKey);
                    List<CameraTuple> cameraTupleList = new ArrayList<>();
                    //not randomizing cameraTuples and instead using nbrCameraTuples within each cube
                    for (int camTupleCnt = 1; camTupleCnt <= nbrCameraTuples ; camTupleCnt++) {
                        CameraTuple cameraTuple = new CameraTuple();
                        cameraTuple.setCamera(CAM + camTupleCnt);
                        cameraTuple.setRoi(ROI + camTupleCnt);//TODO: make it an ROI(x,y,width,height) object
                        String outputFile1 = fileLocation + "/" + seqCnt + "/" + cameraTuple.getCamera() + ".jpg";
                        logger.debug("InputMetadata CameraLst - Camera Source fileLocation: {}", outputFile1);
                        cameraTuple.setCamFileLocation(outputFile1);
                        cameraTupleList.add(cameraTuple);
                    }
                    inputMetadata.setCameraLst(cameraTupleList);
                    synchronized (lock) {
                        inputMetadata.getTimingMap().put("Generated", System.currentTimeMillis());
                        eventCnt++;
                        sourceContext.collect(inputMetadata);
                    }
                    logger.debug("InputMetadataSource - Emitting each InputMetadata event {}", inputMetadata);
                }
                //next seqCnt after servingSpeedMs
                long currWorkingTime = System.currentTimeMillis();//1240 ; 1267
                long nextKickOffTime = currentTimeMs + (servingSpeedMs * (seqCnt+1) + sourceDelay);// 1234 + 33 *1= 1267; 1267 + 33= 1300
                long diffGreaterThanZero = nextKickOffTime - currWorkingTime;//1267 - 1240 = 27; 1300 - 1267 = 33
                if (diffGreaterThanZero > 0) {
                    Thread.sleep(diffGreaterThanZero);
                }
            }
            logger.debug("Reached end of InputMetadata event generation");
            break;
        }
        sourceContext.close();
        logger.debug("Stopping InputMetadata events generator");
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public List<Long> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
        //if state is not re-partitonable, return a singleton Collections
        return Collections.singletonList(eventCnt);
    }

    @Override
    public void restoreState(List<Long> state) throws Exception {
        for (Long s:state) {
            this.eventCnt = s;
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        logger.debug("InputmetadataSource-checkpoint complete for checkpointId:{}", checkpointId);
    }
}
