#flink-grpc-alpha
https://github.com/vijay172/grpc-alpha
#Install grpc protoc generated jar in local maven repo
Copy latest version of protoc generated files from jar into src/main/resources/lib/grpc-java-course1-1.0-SNAPSHOT.jar 

Go to root directory

mvn deploy:deploy-file -DgroupId=com.intel.grpc -DartifactId=Image -Dversion=0.0.1 -Durl=file:./local-maven-repo/ -DrepositoryId=local-maven-repo -DupdateReleaseInfo=true -Dfile=./src/main/resources/lib/grpc-java-course1-1.0-SNAPSHOT.jar

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
#does both copy and read
--action all
#does only copy
--action copy
#does only read
--action read