rm -r ./averageprice_classes
mkdir averageprice_classes
rm ac.jar
javac -d averageprice_classes AveragePrice.java
jar cvf ac.jar -C  averageprice_classes/ .
hdfs dfs -rm -r /dft-output2
hadoop jar ac.jar AveragePrice /dft  /dft-output2

hdfs dfs -cat /dft-output2/part-r-00000