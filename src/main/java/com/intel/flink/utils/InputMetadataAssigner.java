package com.intel.flink.utils;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import com.intel.flink.datatypes.CameraWithCube;
import com.intel.flink.datatypes.InputMetadata;
/**
 * This function does not need to extract a Timestamp from each record. This can be used to generate watermarks
 * that are encoded in the input records. Punctuated watermarks means watermarks are embedded into the input record.
 * Flink encodes timestamps as 16-byte long values and attaches them as metadata to records.
 * Its built-in operators interpret the long value as a Unix timestamp with millisecond precision,
 * i.e., the number of milliseconds since 1970-01-01-00:00:00.000.
 * Watermarks are used to derive the current event-time at each task in an event-time application.
 * Watermarks that are very tight, i.e., close to the record timestamps, result in low processing latency
 * because a task will only briefly wait for more records to arrive before finalizing a computation. At the same time,
 * the result completeness might suffer because more records might not be included in the result and would be
 * considered as late records.
 * Inversely, very wide watermarks increase processing latency but improve result completeness.
 */
public class InputMetadataAssigner implements AssignerWithPunctuatedWatermarks<InputMetadata> {

    @Override
    public Watermark checkAndGetNextWatermark(InputMetadata inputMetadata, long extractedTimestamp) {
        //simply emit a Watermark with every event
        return new Watermark(extractedTimestamp - 30000);//TODO: is this too wide ?
    }

    @Override
    public long extractTimestamp(InputMetadata inputMetadata, long l) {
        return System.currentTimeMillis();//TODO: no event timestamp here from InputMetadata
    }
}
