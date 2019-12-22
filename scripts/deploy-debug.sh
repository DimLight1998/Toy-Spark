sbt assembly

for x in dbvps-{1,2,3}; do
    scp config.json $x:Toy-Spark
    scp scripts/run-debug.sh $x:Toy-Spark
done

scp target/scala-2.12/Toy-Spark-assembly-0.2.jar dbvps-1:Toy-Spark
ssh dbvps-1 scp Toy-Spark/Toy-Spark-assembly-0.2.jar 172.21.0.4:Toy-Spark
ssh dbvps-1 scp Toy-Spark/Toy-Spark-assembly-0.2.jar 172.21.0.33:Toy-Spark
