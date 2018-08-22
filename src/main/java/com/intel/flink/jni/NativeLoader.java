package com.intel.flink.jni;

import com.intel.grpc.image.client.ImageRefClient;
import com.proto.image.Roi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NativeLoader {
    private static final Logger logger = LoggerFactory.getLogger(NativeLoader.class);

    private static ImageRefClient imageRefClient;

    private NativeLoader(String host, int port) {
        if (imageRefClient == null) {
            imageRefClient = new ImageRefClient(host, port);
        }
    }

    private static NativeLoader nativeLoader;

    public static NativeLoader getInstance(String host, int port) {
        if (nativeLoader == null) {
            nativeLoader = new NativeLoader(host, port);
        }
        return nativeLoader;
    }

    public static void shutDown() {
        logger.debug("shutdown()");
        if (imageRefClient != null) {
            imageRefClient.shutDown();
        }
    }

    public  String copyImage(long deadlineDuration, String inputFile, String outputFile, String options) {
        logger.info("Entered copyImage() - inputFile:{}, outputFile:{}, options:{}", inputFile, outputFile, options);

        String returnValue = imageRefClient.copyImageSync(deadlineDuration, inputFile, outputFile, options, imageRefClient.getChannel());
        logger.debug(returnValue);
        return returnValue;
    }

    public String readImage(long deadlineDuration, String inputFile, int x, int y, int width, int height, String options) {
        logger.info("readImage() - inputFile:{}, x:{},y:{},width:{},height:{}, options:{}", inputFile, x, y, width, height, options);
        Roi roi = Roi.newBuilder()
                .setX(x)
                .setY(y)
                .setWidth(width)
                .setHeight(height)
                .build();
        String returnValue = imageRefClient.readImageSync(deadlineDuration, inputFile, roi, options, imageRefClient.getChannel());
        logger.debug(returnValue);
        return returnValue;
    }

    public static void main(String[] args) {
        NativeLoader nativeLoader = NativeLoader.getInstance("localhost", 50051);
        nativeLoader.copyImage(5000, "test.txt", "tmp","test");
        nativeLoader.readImage(5000, "test.txt", 0, 0, 10, 5, "test");
        nativeLoader.shutDown();
    }
}
