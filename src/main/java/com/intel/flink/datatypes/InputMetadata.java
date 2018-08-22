package com.intel.flink.datatypes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

/**
 * Mutable DTO
 */
public class InputMetadata implements Comparable<InputMetadata> {
    private static final Logger logger = LoggerFactory.getLogger(InputMetadata.class);

    public InputMetadata() {
    }

    public InputMetadata(InputMetadataKey inputMetadataKey, List<CameraTuple> cameraLst) {
        this.inputMetadataKey = inputMetadataKey;
        this.cameraLst = cameraLst;
        this.count = getCount();
    }

    public InputMetadata(long ts, String cube, List<CameraTuple> cameraLst) {
        this.inputMetadataKey = new InputMetadataKey(ts, cube);
        this.cameraLst = cameraLst;
        this.count = getCount();
    }

    /**
     * Convert input line to InputMetadata
     * ts,cube,2,cam1,cam2
     *
     * @param line input line from file
     * @return InputMetadata object
     */
    public static InputMetadata fromString(String line) {
        String[] tokens = line.split(",");

        if (tokens.length < 4) {
            throw new RuntimeException("Invalid record: " + line);
        }
        InputMetadata inputMetadata = new InputMetadata();
        try {
            long ts = Long.parseLong(tokens[0]);
            String cube = tokens[1];
            inputMetadata.inputMetadataKey = new InputMetadataKey(ts, cube);
            List<CameraTuple> cameraLst = new ArrayList<>();
            int cameraCnt = Integer.parseInt(tokens[2]);
            //TODO:cameraLst.addAll(Arrays.asList(tokens).subList(3, cameraCnt + 3));
            inputMetadata.cameraLst = cameraLst;
        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }
        return inputMetadata;
    }

    public static class InputMetadataKey {
        public long ts;
        public String cube;

        public InputMetadataKey() {
        }

        public InputMetadataKey(long ts, String cube) {
            this.ts = ts;
            this.cube = cube;
        }

        public long getTs() {
            return ts;
        }

        public void setTs(long ts) {
            this.ts = ts;
        }

        public String getCube() {
            return cube;
        }

        public void setCube(String cube) {
            this.cube = cube;
        }

        @Override
        public String toString() {
            return "InputMetadataKey{" +
                    "ts=" + ts +
                    ", cube='" + cube + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            InputMetadataKey that = (InputMetadataKey) o;
            return ts == that.ts &&
                    Objects.equals(cube, that.cube);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ts, cube);
        }

        public int compareTo(InputMetadataKey other) {
            if (other == null) {
                return 1;
            } else {
                int i = Long.compare(ts, other.ts);
                if (i != 0) {
                    return i;
                }

                i = cube != null ? cube.compareTo(other.cube) : -1;
                return i;
            }
        }
    }

    public InputMetadataKey inputMetadataKey;
    public List<CameraTuple> cameraLst;
    public long count;
    public HashMap<String, Long> timingMap = new HashMap<>();

    public InputMetadataKey getInputMetadataKey() {
        return inputMetadataKey;
    }

    public void setInputMetadataKey(InputMetadataKey inputMetadataKey) {
        this.inputMetadataKey = inputMetadataKey;
    }

    public List<CameraTuple> getCameraLst() {
        return cameraLst;
    }

    public void setCameraLst(List<CameraTuple> cameraLst) {
        this.cameraLst = cameraLst;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public HashMap<String, Long> getTimingMap() {
        return timingMap;
    }

    public void setTimingMap(HashMap<String, Long> timingMap) {
        this.timingMap = timingMap;
    }

    public long getCount() {
        if (cameraLst != null && cameraLst.size() > 0) {
            count = cameraLst.size();
        } else {
            count = 0;
        }
        return count;
    }

    @Override
    public int compareTo(InputMetadata other) {
        if (other == null) {
            return 1;
        } else {
            return inputMetadataKey != null ? inputMetadataKey.compareTo(other.inputMetadataKey) : -1;
        }
    }

    @Override
    public String toString() {
        return "InputMetadata{" +
                "inputMetadataKey=" + inputMetadataKey +
                ", cameraLst=" + cameraLst +
                ", count=" + count +
                ", timingMap=" + timingMap +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InputMetadata that = (InputMetadata) o;
        return Objects.equals(inputMetadataKey, that.inputMetadataKey) &&
                Objects.equals(cameraLst, that.cameraLst);
    }

    @Override
    public int hashCode() {

        return Objects.hash(inputMetadataKey, cameraLst);
    }
}
