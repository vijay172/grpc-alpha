package com.intel.flink.utils;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import com.intel.flink.datatypes.CameraWithCube;

/**
 * This function does not need to extract a Timestamp from each record. This can be used to generate watermarks
 * that are encoded in the input records. Punctuated watermarks means watermarks are embedded into the input record.
 */
public class CameraAssigner implements AssignerWithPunctuatedWatermarks<CameraWithCube> {

    @Override
    public Watermark checkAndGetNextWatermark(CameraWithCube cameraWithCube, long extractedTimestamp) {
        //simply emit a Watermark with every event
        return new Watermark(extractedTimestamp - 30000);
    }

    @Override
    public long extractTimestamp(CameraWithCube cameraWithCube, long l) {
        return System.currentTimeMillis();//TODO: no event timestamp here to extract from CameraWithCube
    }
}
