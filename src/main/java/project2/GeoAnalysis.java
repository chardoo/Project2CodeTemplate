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

            // Create geo2dsphere index on geometry field with error handling
            try {
                marburgLocations.createIndex(Indexes.geo2dsphere("geometry"));
                System.out.println("Created geo2dsphere index on geometry field");
            } catch (com.mongodb.MongoCommandException e) {
                if (e.getErrorCode() == 14031) {
                    System.err.println("\nWARNING: Could not create geo2dsphere index due to insufficient disk space");
                    System.err.println("Available: ~" + (238116864 / 1024 / 1024) + " MB, Required: ~500 MB");
                    System.err.println("Queries will use manual distance calculation instead.\n");
                } else {
                    throw e;
                }
            }

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

    // Task 2.1.f: Find 10 closest restaurants (manual distance calculation, no index needed)
    public void findRestaurants() {
        List<Double> fbCoordinates = getFB();

        if (fbCoordinates == null) {
            System.err.println("Cannot find restaurants without FB coordinates");
            return;
        }

        System.out.println("\n10 Closest Restaurants:");
        System.out.println("=======================");

        // Filter for restaurants only
        Bson filter = Filters.eq("properties.amenity", "restaurant");

        // Retrieve all restaurants and calculate distances
        List<RestaurantDistance> restaurants = new ArrayList<>();

        for (Document doc : marburgLocations.find(filter)) {
            Document geometry = doc.get("geometry", Document.class);
            if (geometry != null) {
                @SuppressWarnings("unchecked")
                List<Double> coords = (List<Double>) geometry.get("coordinates");

                if (coords != null && coords.size() >= 2) {
                    Document properties = doc.get("properties", Document.class);
                    String name = properties != null ? properties.getString("name") : "Unnamed Restaurant";

                    if (name == null) {
                        name = "Unnamed Restaurant";
                    }

                    double distance = calculateDistance(fbCoordinates, coords);
                    restaurants.add(new RestaurantDistance(name, distance));
                }
            }
        }

        // Sort by distance and get top 10
        restaurants.sort((r1, r2) -> Double.compare(r1.distance, r2.distance));

        int count = Math.min(10, restaurants.size());
        for (int i = 0; i < count; i++) {
            RestaurantDistance r = restaurants.get(i);
            System.out.printf("%d. %s - Distance: %.2f meters%n", i + 1, r.name, r.distance);
        }

        System.out.println("\nNote: Using manual distance calculation (no geospatial index due to disk space limitation)");

        /*
         * MongoDB Shell (JavaScript) query (requires geo2dsphere index):
         *
         * db.marburgLocations.aggregate([
         *   {
         *     $geoNear: {
         *       near: {
         *         type: "Point",
         *         coordinates: [8.8106344, 50.8094125] // FB coordinates
         *       },
         *       distanceField: "distance",
         *       query: { "properties.amenity": "restaurant" },
         *       spherical: true
         *     }
         *   },
         *   { $limit: 10 },
         *   {
         *     $project: {
         *       "properties.name": 1,
         *       "distance": 1,
         *       "_id": 0
         *     }
         *   }
         * ])
         */
    }

    // Helper method to calculate distance using Haversine formula
    private double calculateDistance(List<Double> point1, List<Double> point2) {
        double lon1 = point1.get(0);
        double lat1 = point1.get(1);
        double lon2 = point2.get(0);
        double lat2 = point2.get(1);

        // Haversine formula for great-circle distance
        double R = 6371000; // Earth's radius in meters
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);

        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                        Math.sin(dLon / 2) * Math.sin(dLon / 2);

        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return R * c;
    }

    // Helper class to store restaurant name and distance
    private static class RestaurantDistance {
        String name;
        double distance;

        RestaurantDistance(String name, double distance) {
            this.name = name;
            this.distance = distance;
        }
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