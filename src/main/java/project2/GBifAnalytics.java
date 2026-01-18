package project2;

import com.mongodb.client.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.*;

import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Sorts.*;

public class GBifAnalytics {

    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> gbifCollection;

    public GBifAnalytics() {
        this.mongoClient = MongoClients.create("mongodb://localhost:27017");
        this.database = mongoClient.getDatabase("biodiv");
        this.gbifCollection = database.getCollection("gbif");
    }

    // Task 2.2.f: Find 10 communities with highest number of different species
    public void mostDiverseCommunities() {
        System.out.println("Top 10 Most Diverse Communities:");
        System.out.println("=================================");

        // Create aggregation pipeline using Bson instead of Document
        List<Bson> pipeline = Arrays.asList(
                // Group by community name and collect unique species
                group("$GEN", addToSet("speciesCount", "$species")),
                // Add field to count the number of unique species
                new Document("$addFields", new Document("numSpecies", new Document("$size", "$speciesCount"))),
                // Project only the fields we need
                new Document("$project", new Document()
                        .append("communityName", "$_id")
                        .append("numSpecies", 1)
                        .append("_id", 0)
                ),
                // Sort by number of species descending
                sort(descending("numSpecies")),
                // Limit to top 10
                limit(10)
        );

        // Execute aggregation
        int rank = 1;
        for (Document doc : gbifCollection.aggregate(pipeline)) {
            String communityName = doc.getString("communityName");
            Integer numSpecies = doc.getInteger("numSpecies");
            System.out.printf("%d. %s: %d species%n", rank++, communityName, numSpecies);
        }

        /*
         * MongoDB Shell (JavaScript) query:
         *
         * db.gbif.aggregate([
         *   {
         *     $group: {
         *       _id: "$GEN",
         *       speciesCount: { $addToSet: "$species" }
         *     }
         *   },
         *   {
         *     $project: {
         *       communityName: "$_id",
         *       numSpecies: { $size: "$speciesCount" },
         *       _id: 0
         *     }
         *   },
         *   { $sort: { numSpecies: -1 } },
         *   { $limit: 10 }
         * ])
         */
    }

    // Task 2.2.g: Find most similar communities using Jaccard Similarity
    public void mostSimilarCommunities() {
        System.out.println("\n10 Most Similar Community Pairs (Jaccard Similarity):");
        System.out.println("=====================================================");

        // Set up Spark session with MongoDB access
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
//                .config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.driver.port", "0")  // Let Spark pick a random port
                .config("spark.ui.enabled", "false")  // Disable Spark UI to avoid port conflicts
                .config("spark.network.timeout", "800s")
                .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017")
                .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017")
                .getOrCreate();

        // Create aggregation pipeline to get top 10 communities and their species
        String aggregationPipeline = "[" +
                "{ $group: { _id: '$GEN', species: { $addToSet: '$species' } } }," +
                "{ $project: { community: '$_id', species: 1, numSpecies: { $size: '$species' }, _id: 0 } }," +
                "{ $sort: { numSpecies: -1 } }," +
                "{ $limit: 10 }" +
                "]";
        System.out.println("aggregationPipeline");
        // Read from MongoDB with aggregation pipeline using MongoDB Connector 10.x syntax
        Dataset<Row> communities = spark.read()
                .format("mongodb")
                .option("connection.uri", "mongodb://localhost:27017")
                .option("database", "biodiv")
                .option("collection", "gbif")
                .option("aggregation.pipeline", aggregationPipeline)
                .load();

        // Collect communities data
        List<Row> communityList = communities.collectAsList();

        // Calculate Jaccard similarity for all pairs
        List<CommunityPair> similarities = new ArrayList<>();

        for (int i = 0; i < communityList.size(); i++) {
            for (int j = i + 1; j < communityList.size(); j++) {
                Row comm1 = communityList.get(i);
                Row comm2 = communityList.get(j);

                String name1 = comm1.getString(comm1.fieldIndex("community"));
                String name2 = comm2.getString(comm2.fieldIndex("community"));

                // Get species lists - handle both List and WrappedArray from Spark
                Set<String> species1Set = extractSpeciesSet(comm1, "species");
                Set<String> species2Set = extractSpeciesSet(comm2, "species");

                // Calculate Jaccard similarity
                double similarity = calculateJaccardSimilarity(species1Set, species2Set);

                similarities.add(new CommunityPair(name1, name2, similarity));
            }
        }

        // Sort by similarity descending and take top 10
        similarities.sort((a, b) -> Double.compare(b.similarity, a.similarity));

        for (int i = 0; i < Math.min(10, similarities.size()); i++) {
            CommunityPair pair = similarities.get(i);
            System.out.printf("%d. %s <-> %s: %.4f%n",
                    i + 1, pair.community1, pair.community2, pair.similarity);
        }

        spark.stop();
    }

    // Helper method to extract species set from Row (handles both List and WrappedArray)
    private Set<String> extractSpeciesSet(Row row, String fieldName) {
        Set<String> speciesSet = new HashSet<>();
        try {
            int fieldIndex = row.fieldIndex(fieldName);
            Object speciesObj = row.get(fieldIndex);

            if (speciesObj instanceof List) {
                @SuppressWarnings("unchecked")
                List<String> speciesList = (List<String>) speciesObj;
                speciesSet.addAll(speciesList);
            } else if (speciesObj instanceof scala.collection.Seq) {
                // Handle Scala WrappedArray
                scala.collection.Seq<?> seq = (scala.collection.Seq<?>) speciesObj;
                scala.collection.Iterator<?> iterator = seq.iterator();
                while (iterator.hasNext()) {
                    Object item = iterator.next();
                    if (item != null) {
                        speciesSet.add(item.toString());
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error extracting species: " + e.getMessage());
        }
        return speciesSet;
    }

    // Helper method to calculate Jaccard Similarity
    private double calculateJaccardSimilarity(Set<String> set1, Set<String> set2) {
        // Intersection
        Set<String> intersection = new HashSet<>(set1);
        intersection.retainAll(set2);

        // Union
        Set<String> union = new HashSet<>(set1);
        union.addAll(set2);

        if (union.isEmpty()) {
            return 0.0;
        }

        return (double) intersection.size() / union.size();
    }

    // Helper class for storing community pairs and their similarity
    private static class CommunityPair {
        String community1;
        String community2;
        double similarity;

        CommunityPair(String c1, String c2, double sim) {
            this.community1 = c1;
            this.community2 = c2;
            this.similarity = sim;
        }
    }

    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    public static void main(String[] args) {
        GBifAnalytics analytics = new GBifAnalytics();

        // Task f: Most diverse communities
        analytics.mostDiverseCommunities();

        // Task g: Most similar communities
        analytics.mostSimilarCommunities();

        analytics.close();
    }
}