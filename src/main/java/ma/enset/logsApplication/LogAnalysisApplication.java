package ma.enset.logsApplication;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class LogAnalysisApplication {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Log Analysis Application")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.println("1. Chargement du fichier de logs...");
        JavaRDD<String> logLines = sc.textFile("access.log");
        // 2. EXTRACTION DES CHAMPS
        System.out.println("2. Parsing des lignes de logs...\n");

        JavaRDD<LogEntry> logs = logLines
                .map(LogEntry::parseLogLine)
                .filter(entry -> entry != null);
        // Cache pour optimiser les performances (utilisation multiple)
        logs.cache();
        long totalRequests = logs.count();
        long totalErrors = logs.filter(LogEntry::isError).count();
        double errorPercentage = (totalErrors * 100.0) / totalRequests;
        System.out.println("Nombre total de requêtes : " + totalRequests);
        System.out.println("Nombre total d'erreurs (code >= 400) : " + totalErrors);
        System.out.printf("Pourcentage d'erreurs : %.2f%%\n\n", errorPercentage);
        JavaPairRDD<String, Integer> ipCounts = logs
                .mapToPair(log -> new Tuple2<>(log.getIp(), 1))
                .reduceByKey((a, b) -> a + b);
        List<Tuple2<String, Integer>> top5IPs = ipCounts
                .mapToPair(Tuple2::swap) // Inverser pour trier par valeur
                .sortByKey(false) // Tri décroissant
                .mapToPair(Tuple2::swap) // Remettre dans l'ordre original
                .take(5);
        int rank = 1;
        for (Tuple2<String, Integer> entry : top5IPs) {
            System.out.println(rank + ". " + entry._1() + " : " + entry._2() + " requêtes");
            rank++;
        }
        System.out.println();
        JavaPairRDD<String, Integer> resourceCounts = logs
                .mapToPair(log -> new Tuple2<>(log.getResource(), 1))
                .reduceByKey((a, b) -> a + b);

        List<Tuple2<String, Integer>> top5Resources = resourceCounts
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .mapToPair(Tuple2::swap)
                .take(5);

        rank = 1;
        for (Tuple2<String, Integer> entry : top5Resources) {
            System.out.println(rank + ". " + entry._1() + " : " + entry._2() + " requêtes");
            rank++;
        }
        System.out.println();

        JavaPairRDD<Integer, Integer> httpCodeCounts = logs
                .mapToPair(log -> new Tuple2<>(log.getHttpCode(), 1))
                .reduceByKey((a, b) -> a + b);

        List<Tuple2<Integer, Integer>> httpCodeDistribution = httpCodeCounts
                .sortByKey()
                .collect();

        for (Tuple2<Integer, Integer> entry : httpCodeDistribution) {
            double percentage = (entry._2() * 100.0) / totalRequests;
            System.out.printf("Code %d : %d requêtes (%.2f%%)\n",
                    entry._1(), entry._2(), percentage);
        }
        System.out.println();

        // Taille totale des réponses
        long totalSize = logs
                .map(LogEntry::getResponseSize)
                .reduce((a, b) -> a + b);

        System.out.println("Taille totale des réponses : " + totalSize + " octets");
        System.out.printf("Taille moyenne des réponses : %.2f octets\n",
                (double) totalSize / totalRequests);

        // Répartition par méthode HTTP
        System.out.println("\nRépartition par méthode HTTP :");
        JavaPairRDD<String, Integer> methodCounts = logs
                .mapToPair(log -> new Tuple2<>(log.getMethod(), 1))
                .reduceByKey((a, b) -> a + b);

        methodCounts.sortByKey().collect().forEach(entry ->
                System.out.println("  " + entry._1() + " : " + entry._2() + " requêtes")
        );
        // Fermeture du contexte Spark
        sc.close();
    }
}
