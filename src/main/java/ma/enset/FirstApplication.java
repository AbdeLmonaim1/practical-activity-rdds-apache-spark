package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class FirstApplication {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                // Name App is important for deployment on cluster
                .setAppName("RDD ACTIVITY")
                //Execute spark application locally as a simple java application, but if you want to execute it with more than one thread use local[*]
                // [*] means that Spark will decide the number of threads based on the number of cores available in your machine
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //Create our 1st RDD
        //RDD by parallelize a list of elements
        JavaRDD<Double> rddMarks = sc.parallelize(Arrays.asList(new Double[]{
                12.5, 15.0, 14.5, 18.0, 19.5, 9.5, 8.0, 16.0
        }));
        //1st transformation on RDD : map => that generates a new RDD by applying a transformation function on each element of the source rddMarks
        JavaRDD<Double> newRddMarksPlusOne = rddMarks.map(mark -> mark + 1);
        //2nd transformation on RDD : filter => that generates a new RDD by applying a filtering function on each element of the source rddMarks
        JavaRDD<Double> newRDDMarksGratherThan12 = newRddMarksPlusOne.filter(mark -> mark >= 12).map(mark -> mark);
        //Action on RDD : collect => that returns all the elements of the RDD to the driver program as a list
        List<Double> results = newRDDMarksGratherThan12.collect();
        results.forEach(System.out::println);
    }
}