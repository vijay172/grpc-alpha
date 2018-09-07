#!/bin/bash
cd /Users/vkbalakr/work/flink-examples/grpc-fd19-alpha/
mvn clean package

# Create a temporary filename in /tmp directory
jar_files=$(mktemp)
# Create classpath string of dependencies from the local repository to a file
mvn -Dmdep.outputFile=$jar_files dependency:build-classpath
classpath_values=$(cat $jar_files)
#echo "classpath_values:" $classpath_values
#worked
java -jar target/intel-grpc-fd19-alpha-1.0.jar --maxSeqCnt 1000 --parallelCam 2 --nbrCameras 3 --nbrCameraTuples 1 --parallelCube 1 --nbrCubes 1 --timeout 100000 --shutdownWaitTS 100000 --nThreads 2 --nCapacity 100 --inputFile file:///Users/vkbalakr/Downloads/test-img.jpg --outputFile file:///tmp --options test --outputPath /tmp/demo --local true --host localhost --port 50051 --action all --checkpoint true