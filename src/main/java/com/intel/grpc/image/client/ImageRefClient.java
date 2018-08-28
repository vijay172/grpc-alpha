package com.intel.grpc.image.client;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import com.proto.image.CopyImageRequest;
import com.proto.image.CopyImageResponse;
import com.proto.image.ImageServiceGrpc;
import com.proto.image.ReadImageRequest;
import com.proto.image.ReadImageResponse;
import com.proto.image.Roi;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ImageRefClient {
    private static final Logger logger = LoggerFactory.getLogger(ImageRefClient.class);

    private static ManagedChannel channel;
    private final String host;
    private final int port;

    public ImageRefClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.channel = getManagedChannel(host,port);
    }

    public static void main(String[] args) {
        logger.debug("Hello, I am a gRPC client");
        //input arguments
        String inputFile = args[0];//"test.txt";
        String fileLocation = args[1];//"tmp";
        String options = args[2];//"test";
        Roi roi = Roi.newBuilder()
                .setX(1)
                .setY(2)
                .setWidth(3)
                .setHeight(4)
                .build();
        long deadlineDuration = Long.parseLong(args[3]);//200 ms
        String host = args[4];//"localhost";
        int port = Integer.parseInt(args[5]);//50051;

        logger.debug("inputFile:{}, fileLocation:{}, options:{}, deadlineDuration:{}, host:{}, port:{}",
                inputFile, fileLocation, options, deadlineDuration, host, port);

        //ManagedChannel channel = getManagedChannel(host, port);
        ImageRefClient imageRefClient = new ImageRefClient(host, port);
        logger.debug("Creating Image service client stub");
        //DummyServiceGrpc.DummyServiceBlockingStub syncClient = DummyServiceGrpc.newBlockingStub(channel);
        //DummyServiceGrpc.DummyServiceFutureStub asyncClient = DummyServiceGrpc.newFutureStub(channel);
        //final ImageServiceGrpc.ImageServiceStub asyncClient = ImageServiceGrpc.newStub(channel);


        try {
            //works
            //copyImageFutureAsync(deadlineDuration, inputFile, fileLocation, host, port);
            //unary stream doesn't work
            //copyImageReqStreamAsync(deadlineDuration, inputFile, fileLocation, host, port);
            //bidi async works
            //copyImageBidiAsync(deadlineDuration, inputFile, fileLocation, host, port);
            //synchronous call
            for (int i = 0; i < 4; i++) {
                logger.debug("copyImageSync Create request with count:" + i);
                options = UUID.randomUUID().toString();
                imageRefClient.copyImageSync(deadlineDuration, inputFile + i, fileLocation + i, options, channel);
                imageRefClient.readImageSync(deadlineDuration,  fileLocation + i, roi, options, channel);
            }
            //copyImageAsync(asyncClient, deadlineDuration,latch, inputFile, fileLocation, options,channel);
        } finally {
            logger.debug("Shutting down Channel");
            channel.shutdown();
        }
    }

    private ManagedChannel getManagedChannel(String host, int port) {
        logger.debug("getManagedChannel for host:{}, port:{}", host, port);
        return ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext() //avoid ssl issues
                    .build();
    }

    public ManagedChannel getChannel() {
        return channel;
    }

    public void shutDown() {
        if (channel != null) {
            channel.shutdown();
        }
    }

    public void copyImageFutureAsync(long deadlineDuration,
                                             String inputFile, String fileLocation) {
        logger.debug("Entered copyImageFutureAsync()");
        ManagedChannel channel = getManagedChannel(host, port);

        CountDownLatch latch = new CountDownLatch(1);//TODO: unbounded ???
        try {
            //asynchronous Future client
            final ImageServiceGrpc.ImageServiceFutureStub asyncImageClient = ImageServiceGrpc.newFutureStub(channel);
            String options = UUID.randomUUID().toString();
            //set up protobuf copy image request
            CopyImageRequest copyImageRequest = CopyImageRequest.newBuilder()
                    .setInputFile(inputFile)
                    .setFileLocation(fileLocation)
                    .setOptions(options)
                    .build();
            //async call
            final ListenableFuture<CopyImageResponse> copyImageAsyncResponse =
                    asyncImageClient
                            .withDeadline(Deadline.after(deadlineDuration, TimeUnit.SECONDS)) //change to Milliseconds to simulate deadline exception
                            .copyImage(copyImageRequest);
            //Ugly API compared to node.js async handling
            Futures.addCallback(copyImageAsyncResponse,
                    new FutureCallback<CopyImageResponse>() {
                        @Override
                        public void onSuccess(@Nullable final CopyImageResponse copyImageResponse) {
                            assert copyImageResponse != null;
                            logger.debug("copyImageResponse:" + copyImageResponse.getResult());
                            latch.countDown();
                            logger.debug("Shutting down channel");
                            channel.shutdown();
                        }

                        @Override
                        public void onFailure(Throwable throwable) {
                            Status status = Status.fromThrowable(throwable);
                            if (status.getCode() == Status.Code.DEADLINE_EXCEEDED) {
                                logger.debug("Deadline has been exceeded, we don't want the response");
                            } else {
                                throwable.printStackTrace();
                            }
                            latch.countDown();
                            logger.debug("Shutting down channel");
                            channel.shutdown();
                        }
                    }, MoreExecutors.directExecutor());

            if (!Uninterruptibles.awaitUninterruptibly(latch, 1, TimeUnit.SECONDS)) {
                throw new RuntimeException("copyImageFutureAsync - latch timeout!");
            }
        } catch (StatusRuntimeException e) { //catch exceptions from gRPC server
            if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                logger.debug("Deadline has been exceeded, we don't want the response");
            } else {
                logger.error("StatusRuntimeException:" + e.getStatus());
                //TODO: build retry logic
                e.printStackTrace();
            }
        }
    }

    public void copyImageReqStreamAsync(long deadlineDuration,
                                                String inputFile, String fileLocation,
                                               String host, int port) {
        logger.debug("Entered copyImageReqStreamAsync()");
        ManagedChannel channel = getManagedChannel(host, port);
        int count = 4;
        CountDownLatch latch = new CountDownLatch(count);//TODO: unbounded count
        Map<String, ArrayList<String>> requestMap = new HashMap<>();
        try {
            //asynchronous client
            final ImageServiceGrpc.ImageServiceStub asyncImageClient = ImageServiceGrpc.newStub(channel);
            String options;
            final StreamObserver<CopyImageRequest> requestCopyObserver = asyncImageClient
                    .withDeadline(Deadline.after(deadlineDuration, TimeUnit.SECONDS)) //change to Milliseconds to simulate deadline exception
                    .copyImageReqStream(new StreamObserver<CopyImageResponse>() {
                        @Override
                        public void onNext(CopyImageResponse value) {
                            logger.debug("Response from server:" + value.getResult());
                            //TODO: match requestId from options with requestMap

                        }

                        @Override
                        public void onError(Throwable t) {
                            latch.countDown();
                            logger.error("copyImageReqStreamAsync Error thrown:" + t);//TODO: maybe retry ?
                            if (t instanceof StatusRuntimeException) {
                                StatusRuntimeException e = (StatusRuntimeException) t;
                                if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                                    logger.debug("copyImageReqStreamAsync Deadline has been exceeded, we don't want the response");
                                } else {
                                    e.printStackTrace();
                                }
                            }
                        }

                        @Override
                        public void onCompleted() {
                            logger.debug("copyImageReqStreamAsync Server is done sending data");
                            latch.countDown();
                        }
                    });
            //setup request
            for (int i = 0; i < count; i++) {
                logger.debug("copyImageReqStreamAsync Create request with count:" + i);
                options = UUID.randomUUID().toString();
                ArrayList<String> dataLst = new ArrayList<>();
                dataLst.add(inputFile + i);
                dataLst.add(fileLocation + i);
                dataLst.add(options);
                requestMap.put(options, dataLst);
                requestCopyObserver.onNext(
                        CopyImageRequest.newBuilder()
                                .setInputFile(inputFile + i)
                                .setFileLocation(fileLocation + i)
                                .setOptions(options)
                                .build());
            }
            //done with request - else keep it open
            requestCopyObserver.onCompleted();

            if (!Uninterruptibles.awaitUninterruptibly(latch, 500, TimeUnit.SECONDS)) { //TODO: Latch timeout as parm
                throw new RuntimeException("copyImageReqStreamAsync - latch timeout!");
            }
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                logger.debug("Deadline has been exceeded, we don't want the response");
            } else {
                logger.error("StatusRuntimeException:" + e.getStatus());
                //TODO: build retry logic
                e.printStackTrace();
            }
        } finally {
            //TODO: maybe close channel here
            //channel.shutdown();
        }
    }


    public void copyImageBidiAsync(long deadlineDuration,
                                           String inputFile, String fileLocation,
                                          String host, int port) {
        logger.debug("Entered copyImageBidiAsync()");
        ManagedChannel channel = getManagedChannel(host, port);
        int count = 4;
        CountDownLatch latch = new CountDownLatch(count);//TODO: unbounded ???
        Map<String, ArrayList<String>> requestMap = new HashMap<>();
        final ImageServiceGrpc.ImageServiceStub asyncImageClient = ImageServiceGrpc.newStub(channel);
        try {
            String options;
            //how to handle copyImage response
            final StreamObserver<CopyImageRequest> requestCopyObserver = asyncImageClient
                    .withDeadline(Deadline.after(deadlineDuration, TimeUnit.SECONDS)) //change to Milliseconds to simulate deadline exception
                    .copyImageBidi(new StreamObserver<CopyImageResponse>() {
                        @Override
                        public void onNext(CopyImageResponse value) {
                            logger.debug("copyImageBidiAsync Response from server:" + value.getResult());
                        }

                        @Override
                        public void onError(Throwable t) {
                            latch.countDown();
                            logger.error("copyImageBidiAsync Error thrown:" + t);//TODO: maybe retry ?
                            if (t instanceof StatusRuntimeException) {
                                StatusRuntimeException e = (StatusRuntimeException) t;
                                if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                                    logger.debug("copyImageBidiAsync Deadline has been exceeded, we don't want the response");
                                } else {
                                    e.printStackTrace();
                                }
                            }
                        }

                        @Override
                        public void onCompleted() {
                            logger.debug("copyImageBidiAsync Server is done sending data");
                            latch.countDown();
                        }
                    });

            //setup request
            for (int i = 0; i < count; i++) {
                logger.debug("copyImageBidiAsync Create request with count:" + i);
                options = UUID.randomUUID().toString();
                ArrayList<String> dataLst = new ArrayList<>();
                dataLst.add(inputFile + i);
                dataLst.add(fileLocation + i);
                dataLst.add(options);
                requestMap.put(options, dataLst);
                requestCopyObserver.onNext(
                        CopyImageRequest.newBuilder()
                                .setInputFile(inputFile + i)
                                .setFileLocation(fileLocation + i)
                                .setOptions(options)
                                .build());
            }

            requestCopyObserver.onCompleted();

            try {
                latch.await(500, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("copyImageBidiAsync Latch Interrupt exception:" + e);
                e.printStackTrace();
            }
        } finally {
            logger.debug("copyImageBidiAsync Shutting down channel");
            channel.shutdown();
        }
    }


    public String copyImageSync(long deadlineDuration,
                                       String inputFile, String fileLocation, String options, ManagedChannel channel) {
        logger.debug("Entered copyImageSync()");
        if (channel == null) {
            channel = getManagedChannel(host, port);
        }
        try {
            //blocking synchronous client
            final ImageServiceGrpc.ImageServiceBlockingStub imageClient = ImageServiceGrpc.newBlockingStub(channel);
            //set up protobuf copy image request
            CopyImageRequest copyImageRequest = CopyImageRequest.newBuilder()
                    .setInputFile(inputFile)
                    .setFileLocation(fileLocation)
                    .setOptions(options)
                    .build();
            CopyImageResponse copyImageResponse = imageClient.withDeadline(Deadline.after(deadlineDuration, TimeUnit.SECONDS))
                    .copyImage(copyImageRequest);
            logger.debug("copyImageResponse:" + copyImageResponse.getResult());
            return copyImageResponse.getResult();
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                logger.error("Deadline has been exceeded, we don't want the response");
            } else {
                logger.error("StatusRuntimeException:" + e.getStatus());
                e.printStackTrace();
            }
        } finally {
            /*logger.debug("Shutting down channel");
            channel.shutdown();*/
        }
        return null;
    }

    public String readImageSync(long deadlineDuration,
                                String fileLocation, Roi roi, String options, ManagedChannel channel) {
        logger.debug("Entered readImageSync()");
        if (channel == null) {
            channel = getManagedChannel(host, port);
        }
        try {
            //blocking synchronous client
            final ImageServiceGrpc.ImageServiceBlockingStub imageClient = ImageServiceGrpc.newBlockingStub(channel);
            //set up protobuf read image request
            ReadImageRequest readImageRequest = ReadImageRequest.newBuilder()
                    .setFileLocation(fileLocation)
                    .setRoi(roi)
                    .setOptions(options)
                    .build();

            ReadImageResponse readImageResponse = imageClient.withDeadline(Deadline.after(deadlineDuration, TimeUnit.SECONDS)) //TODO: determine deadline Duration
                    .readImage(readImageRequest);
            logger.debug("readImageResponse:" + readImageResponse.getResult());
            return readImageResponse.getResult();
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                logger.debug("Deadline has been exceeded, we don't want the response");
            } else {
                logger.error("StatusRuntimeException:" + e.getStatus());//Status{code=UNAVAILABLE,
                e.printStackTrace();
            }
        } finally {
            /*logger.debug("Shutting down channel");
            channel.shutdown();*/
        }
        return null;
    }

}
