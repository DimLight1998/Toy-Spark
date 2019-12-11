sbt assembly

for x in dbvps-{1,2,3}; do
    scp target/scala-2.12/Toy-Spark-assembly-0.2.jar $x:Toy-Spark
    scp config.json $x:Toy-Spark
done
