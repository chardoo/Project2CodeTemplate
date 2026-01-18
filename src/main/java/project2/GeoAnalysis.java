package project2;

import com.mongodb.client.*;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GeoAnalysis {

    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> marburgLocations;

    public GeoAnalysis() {
        this.mongoClient = MongoClients.create("mongodb://localhost:27017");
        this.database = mongoClient.getDatabase("geoDatabase");
        this.marburgLocations = database.getCollection("marburgLocations");
    }

    // Task 2.1.e: Insert Point geometries and create geo2dsphere index
    public void insertData(String filePath) {
        try {
            // Try multiple possible paths
            String content = null;
            String[] possiblePaths = {
                    filePath,
                    "src/main/resources/" + filePath.replace("resources/", ""),
                    "src/main/" + filePath,
                    filePath.replace("resources/", "")
            };

            for (String path : possiblePaths) {
                try {
                    content = new String(Files.readAllBytes(Paths.get(path)));
//                    System.out.println("Reading from: " + content);
                    System.out.println("Reading from: " + path);
                    break;
                } catch (IOException e) {
                    // Try next path
                }
            }

            if (content == null) {
                System.err.println("Could not find file. Tried paths:");
                for (String path : possiblePaths) {
                    System.err.println("  - " + path);
                }
                return;
            }

            // Check what we're dealing with
            String trimmedContent = content.trim();
            System.out.println("File starts with: " + trimmedContent.charAt(0));

            List<Document> pointDocuments = new ArrayList<>();

            if (trimmedContent.startsWith("[")) {
                // It's a JSON array directly (FeatureCollection as array)
                JSONArray featuresArray = new JSONArray(content);

                // Process each feature in the array
                for (int i = 0; i < featuresArray.length(); i++) {
                    JSONObject feature = featuresArray.getJSONObject(i);

                    if (feature.has("geometry")) {
                        JSONObject geometry = feature.getJSONObject("geometry");
                        String type = geometry.getString("type");

                        if ("Point".equals(type)) {
                            Document doc = Document.parse(feature.toString());
                            pointDocuments.add(doc);
                        }
                    }
                }

            } else if (trimmedContent.startsWith("{")) {
                // Original code path - it's a JSON object with "features" property
                JSONObject geoJson = new JSONObject(content);
                JSONArray features = geoJson.getJSONArray("features");

                for (int i = 0; i < features.length(); i++) {
                    JSONObject feature = features.getJSONObject(i);

                    if (feature.has("geometry")) {
                        JSONObject geometry = feature.getJSONObject("geometry");
                        String type = geometry.getString("type");

                        if ("Point".equals(type)) {
                            Document doc = Document.parse(feature.toString());
                            pointDocuments.add(doc);
                        }
                    }
                }
            } else {
                System.err.println("Invalid GeoJSON format. Must start with '[' or '{'");
                return;
            }

            // Clear existing collection
            marburgLocations.drop();

            // Insert all point documents
            if (!pointDocuments.isEmpty()) {
                marburgLocations.insertMany(pointDocuments);
                System.out.println("Inserted " + pointDocuments.size() + " Point geometries");
            }

            // Create geo2dsphere index on geometry field
            marburgLocations.createIndex(Indexes.geo2dsphere("geometry"));
            System.out.println("Created geo2dsphere index on geometry field");

        } catch (Exception e) {
            System.err.println("Error reading GeoJSON file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Task 2.1.e: Get coordinates of Fachbereich Mathematik und Informatik
    public List<Double> getFB() {
        // Query for the specific location
        Bson filter = Filters.regex("properties.name", ".*Fachbereich.*Mathematik.*Informatik.*", "i");

        Document result = marburgLocations.find(filter).first();

        if (result != null) {
            Document geometry = result.get("geometry", Document.class);
            @SuppressWarnings("unchecked")
            List<Double> coordinates = (List<Double>) geometry.get("coordinates");

            System.out.println("Fachbereich Mathematik und Informatik coordinates: " + coordinates);
            return coordinates;
        } else {
            System.out.println("Fachbereich Mathematik und Informatik not found");
            return null;
        }
    }

    // Task 2.1.f: Find 10 closest restaurants using geospatial aggregation
    public void findRestaurants() {
        List<Double> fbCoordinates = getFB();

        if (fbCoordinates == null) {
            System.err.println("Cannot find restaurants without FB coordinates");
            return;
        }

        System.out.println("\n10 Closest Restaurants:");
        System.out.println("=======================");

        // Use $near with $geometry (works without index, but slower)
        Bson geoFilter = Filters.near(
                "geometry",
                new org.bson.Document("type", "Point")
                        .append("coordinates", fbCoordinates),
                null,  // maxDistance
                null   // minDistance
        );

        Bson amenityFilter = Filters.eq("properties.amenity", "restaurant");
        Bson combinedFilter = Filters.and(geoFilter, amenityFilter);

        // Execute query
        int rank = 1;
        for (Document doc : marburgLocations.find(combinedFilter).limit(10)) {
            Document geometry = doc.get("geometry", Document.class);
            @SuppressWarnings("unchecked")
            List<Double> coords = (List<Double>) geometry.get("coordinates");

            Document properties = doc.get("properties", Document.class);
            String name = properties != null ? properties.getString("name") : "Unnamed Restaurant";

            if (name == null) {
                name = "Unnamed Restaurant";
            }

            // Calculate distance manually (simple Euclidean approximation)
            double distance = calculateDistance(fbCoordinates, coords);

            System.out.printf("%d. %s - Distance: %.2f meters%n", rank++, name, distance);
        }
    }

    // Helper method to calculate approximate distance
    private double calculateDistance(List<Double> point1, List<Double> point2) {
        double lon1 = point1.get(0);
        double lat1 = point1.get(1);
        double lon2 = point2.get(0);
        double lat2 = point2.get(1);

        // Haversine formula for spherical distance
        double R = 6371000; // Earth's radius in meters
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);

        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                        Math.sin(dLon / 2) * Math.sin(dLon / 2);

        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return R * c;
    }

    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    public static void main(String[] args) {
        GeoAnalysis analysis = new GeoAnalysis();

        // Task e: Insert data and get FB coordinates
        analysis.insertData("marburg.geojson");
        analysis.getFB();

        // Task f: Find closest restaurants
        analysis.findRestaurants();

        analysis.close();
    }
}