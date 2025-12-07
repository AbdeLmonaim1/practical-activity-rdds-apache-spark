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
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //Each line in the file is an element in the RDD
        JavaRDD<String> readLines = sc.textFile("words.txt");
        JavaRDD<String> wordsSplitedRDD = readLines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairWordsRDD = wordsSplitedRDD.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> numberOccurenceForEachWord = pairWordsRDD.reduceByKey((v1, v2) -> v1 + v2);
        numberOccurenceForEachWord.collect().forEach(System.out::println);
    }
}
