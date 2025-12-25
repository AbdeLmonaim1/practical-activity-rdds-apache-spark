package ma.enset.wordsApplication;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordsApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Words Application")
                //We commented this line be cause now we deploy our app in docker container
//                .setMaster("local[*]")
                ;
        JavaSparkContext sc = new JavaSparkContext(conf);
        //Each line in the file is an element in the RDD
//        JavaRDD<String> readLines = sc.textFile("words.txt");
        //now if the words.txt in the hdfs in hadoop cluster
        JavaRDD<String> readLines = sc.textFile("hdfs://namenode:8020/words.txt");
        JavaRDD<String> wordsSplitedRDD = readLines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairWordsRDD = wordsSplitedRDD.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> numberOccurenceForEachWord = pairWordsRDD.reduceByKey((v1, v2) -> v1 + v2);
        numberOccurenceForEachWord.collect().forEach(System.out::println);
        //We put right now the words.txt in HDFS in hadoop container by using thoses commands docker cp .\words.txt practical-activity-rdds-namenode-1:/opt/hadoop, and hdfs dfs -put words.txt / inside of container
        //Now we want to deployed application in spark cluster which executed in Docker container
        //So we need to generate a .jar file of the application an put it in the spark master container
        //by using this command docker cp .\practical-activity-rdds-1.0-SNAPSHOT.jar spark-master:/opt/spark/work-dir
        // /opt/spark/bin/spark-submit --master spark://spark-master:7077 --class ma.enset.wordsApplication practical-activity-rdds-1.0-SNAPSHOT.jar : for deploying the app in spark cluster
    }
}
