package ma.enset.ventes;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.Arrays;
@Slf4j
public class VentesApplication {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Ventes Application")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> readLines = sc.textFile("ventes.txt");
        JavaPairRDD<String, Double> ventesParVilles = readLines.mapToPair(line -> {
            String[] parts = line.split(" ");
            String Ville = parts[1];
            Double prix = Double.parseDouble(parts[3]);
            return new Tuple2<>(Ville, prix);
        });
        log.info("ventes par ville {}", ventesParVilles);
        //calculer le total par ville
        JavaPairRDD<String, Double> totalParVille = ventesParVilles.reduceByKey((v1, v2) -> v1 + v2);
        // Affichage des résultats triés par ville
        totalParVille
                .sortByKey()
                .collect()
                .forEach(tuple ->
                        System.out.println(tuple._1() + " : " + tuple._2() + " DH")
                );
    }
}
