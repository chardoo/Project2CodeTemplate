package project2;

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FlightsAnalysisRedux {

    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> airlinesCollection;

    public FlightsAnalysisRedux() {
        this.mongoClient = MongoClients.create("mongodb://localhost:27017");
        this.database = mongoClient.getDatabase("airlinesDatabase");
        this.airlinesCollection = database.getCollection("airlinesCollection");
    }

    // Task 2.1.b: Insert data from airlines.json
    public void insertData(String filePath) {
        try {
            // Try multiple possible paths
            BufferedReader reader = null;
            String[] possiblePaths = {
                    filePath,
                    "src/main/resources/" + filePath.replace("resources/", ""),
                    "src/main/" + filePath,
                    filePath.replace("resources/", "")
            };

            for (String path : possiblePaths) {
                try {
                    reader = new BufferedReader(new FileReader(path));
                    System.out.println("Reading from: " + path);
                    break;
                } catch (IOException e) {
                    // Try next path
                }
            }

            if (reader == null) {
                System.err.println("Could not find file. Tried paths:");
                for (String path : possiblePaths) {
                    System.err.println("  - " + path);
                }
                return;
            }

            String line;
            List<Document> documents = new ArrayList<>();

            while ((line = reader.readLine()) != null) {
                Document doc = Document.parse(line);
                documents.add(doc);
            }

            reader.close();

            // Clear collection and insert new data
            airlinesCollection.drop();
            airlinesCollection.insertMany(documents);

            System.out.println("Inserted " + documents.size() + " documents into airlinesCollection");

        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Task 2.1.c: Query cancelled flights in Boston
    public void queryCancelledFlights() {
        System.out.println("\nCancelled Flights Query Results:");
        System.out.println("=================================");

        // Create filter: cancelled > 100 AND airport.code = "BOS"
        Bson filter = Filters.and(
                Filters.gt("statistics.flights.cancelled", 100),
                Filters.eq("airport.code", "DCA")
        );

        // Create sort: ascending by airline name, descending by cancelled flights
        Bson sort = Sorts.orderBy(
                Sorts.ascending("airline.name"),
                Sorts.descending("statistics.flights.cancelled")
        );

        // Execute query
        for (Document doc : airlinesCollection.find(filter).sort(sort)) {
            String airlineName = doc.getEmbedded(List.of("carrier", "name"), String.class);
            String airportCode = doc.getEmbedded(List.of("airport", "code"), String.class);
            Integer cancelled = doc.getEmbedded(List.of("statistics", "flights", "cancelled"), Integer.class);

            System.out.println("Airline: " + airlineName +
                    ", Airport: " + airportCode +
                    ", Cancelled: " + cancelled);
        }


    }

    // Task 2.1.d: Query with index
    public void queryIndexedCancelledFlights() {
        System.out.println("\nIndexed Query Performance Test:");
        System.out.println("================================");

        // Measure query time WITHOUT index
        long startWithoutIndex = System.nanoTime();

        Bson filter = Filters.and(
                Filters.gt("statistics.flights.cancelled", 100),
                Filters.eq("airport.code", "DCA")
        );
        Bson sort = Sorts.orderBy(
                Sorts.ascending("airline.name"),
                Sorts.descending("statistics.flights.cancelled")
        );

        long countWithoutIndex = airlinesCollection.find(filter).sort(sort).into(new ArrayList<>()).size();
        long endWithoutIndex = System.nanoTime();
        long timeWithoutIndex = endWithoutIndex - startWithoutIndex;

        // Create compound index
        airlinesCollection.createIndex(
                Indexes.compoundIndex(
                        Indexes.ascending("airport.code"),
                        Indexes.ascending("statistics.flights.cancelled"),
                        Indexes.ascending("airline.name")
                )
        );

        // Measure query time WITH index
        long startWithIndex = System.nanoTime();
        long countWithIndex = airlinesCollection.find(filter).sort(sort).into(new ArrayList<>()).size();
        long endWithIndex = System.nanoTime();
        long timeWithIndex = endWithIndex - startWithIndex;

        System.out.println("Results found: " + countWithIndex);
        System.out.println("Time without index: " + timeWithoutIndex / 1_000_000.0 + " ms");
        System.out.println("Time with index: " + timeWithIndex / 1_000_000.0 + " ms");
        System.out.println("Speedup: " + (double) timeWithoutIndex / timeWithIndex + "x");


    }

    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    public static void main(String[] args) {
        FlightsAnalysisRedux analysis = new FlightsAnalysisRedux();

        // Task b: Insert data
        analysis.insertData("resources/airlinesLines.json");

        // Task c: Query cancelled flights
        analysis.queryCancelledFlights();

        // Task d: Query with index
        analysis.queryIndexedCancelledFlights();

        analysis.close();
    }
}