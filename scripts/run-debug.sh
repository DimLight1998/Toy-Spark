java -Djava.util.logging.SimpleFormatter.format='%4$s: %5$s%6$s%n' -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=3389 -jar Toy-Spark-assembly-0.2.jar $@