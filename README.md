#flink-grpc-alpha
https://github.com/vijay172/grpc-alpha
This is the Flink Client code that uses the generated gRPC stub code inside ImageRefClient.
This client code assumes there is a gRPC server running at localhost:<port> as passed in hrough args.

scripts/start_demo_app.sh has examples of all args passed into the main class.

#Install grpc protoc generated jar in local maven repo
Copy latest version of protoc generated files from jar into src/main/resources/lib/grpc-java-course1-1.0-SNAPSHOT.jar 

Go to root directory

mvn deploy:deploy-file -DgroupId=com.intel.grpc -DartifactId=Image -Dversion=0.0.1 -Durl=file:./local-maven-repo/ -DrepositoryId=local-maven-repo -DupdateReleaseInfo=true -Dfile=./src/main/resources/lib/grpc-java-course1-1.0-SNAPSHOT.jar

This needs to happen before Building the code.
#Build
Go to root directory

mvn clean package

# Run

Assumptions:

Grpc server is already running in another process in the Container.

cd scripts

./start-demo-app.sh

Notes:
Starting start-demo-app.sh with additional parms
#does both copy and read Image operations
--action all
#does only copy Image
--action copy
#does only read IMage - assumes copy has bene done beforehand
--action read