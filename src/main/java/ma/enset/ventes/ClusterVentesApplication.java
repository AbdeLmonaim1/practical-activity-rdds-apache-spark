package ma.enset.ventes;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

@Slf4j
public class ClusterVentesApplication {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Ventes Application")
                ;
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> readLines = sc.textFile("hdfs://namenode:8020/ventes.txt");
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
        JavaPairRDD<String, Double> ventesParVilleEtAnnee = readLines.mapToPair(line -> {
            String[] parts = line.split(" ");
            String date = parts[0];
            String annee = date.split("-")[0];
            String ville = parts[1];
            Double prix = Double.parseDouble(parts[3]);
            String cle = ville + "-" + annee;
            return new Tuple2<>(cle, prix);
        });
        JavaPairRDD<String, Double> totalParVilleEtAnnee =
                ventesParVilleEtAnnee.reduceByKey((v1, v2) -> v1 + v2);
        totalParVilleEtAnnee
                .sortByKey()
                .collect()
                .forEach(tuple -> {
                    String[] parts = tuple._1().split("-");
                    System.out.println("Ville: " + parts[0] +
                            " | Année: " + parts[1] +
                            " | Total: " + tuple._2() + " DH");
                });
        sc.close();
    }
}
