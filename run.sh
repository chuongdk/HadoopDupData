echo "Delte target/*.jar..........................."
rm target/*.jar 
echo "Building .jar ............................................"
mvn package
echo "Running .................................................."
hdfs dfs -rm -r /user/chuong/output
hadoop jar target/DupData-0.0.1-SNAPSHOT.jar  girafon.DupData.App input/retail.dat 3 output
echo "DONE"
