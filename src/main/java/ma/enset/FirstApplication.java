package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

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
        JavaRDD<Double> newRddMarksPlusOne = rddMarks.map(mark -> {
            System.out.println("I'm in map transformation");
            return mark + 1;
        });
        //2nd transformation on RDD : filter => that generates a new RDD by applying a filtering function on each element of the source rddMarks
        JavaRDD<Double> newRDDMarksGratherThan12 = newRddMarksPlusOne.filter(mark -> mark >= 12).map(mark -> mark);
        //Action on RDD : collect => that returns all the elements of the RDD to the driver program as a list
        //By default RDDs are non-persistent, so if you want to persist it in memory you need to use persist() or cache()
        //Memory_ONLY() means that the RDD will be stored in memory only, if there is not enough memory it will recompute the partitions that are not in memory
        //when apply persist() there is no need to re-execute all things again for the second action (collect)
        newRDDMarksGratherThan12.persist(StorageLevel.MEMORY_ONLY());
        List<Double> results = newRDDMarksGratherThan12.collect();
        results.forEach(System.out::println);
        //just for showing the concept of persistence and non-persistence in RDDs
        //"I'm in map transformation" will be printed two time when  we call collect() 2 times because the RDD non-peristent by default
        //So RDD will destroyed after the first action (collect) because it's non-persistent, so you need like to run the transformations again for the second action (collect)
        List<Double> results1 = newRDDMarksGratherThan12.collect();
        results1.forEach(System.out::println);
    }
}