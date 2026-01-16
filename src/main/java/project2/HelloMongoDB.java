package project2;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class HelloMongoDB {
    public static void main(String[] args) {
        // Create MongoClient to connect to local MongoDB cluster
        try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017")) {

            // Create or get database
            MongoDatabase database = mongoClient.getDatabase("testDatabase");

            // Create or get collection
            MongoCollection<Document> collection = database.getCollection("groupMembers");

            // Clear collection for fresh start
            collection.drop();

            // Insert group member names as documents
            Document member1 = new Document("name", "Alice Schmidt")
                    .append("role", "Team Lead");
            Document member2 = new Document("name", "Bob Mueller")
                    .append("role", "Developer");
            Document member3 = new Document("name", "Clara Weber")
                    .append("role", "Analyst");

            collection.insertOne(member1);
            collection.insertOne(member2);
            collection.insertOne(member3);

            // Query and display all documents
            System.out.println("Group Members:");
            System.out.println("--------------");
            for (Document doc : collection.find()) {
                System.out.println(doc.toJson());
            }

        } catch (Exception e) {
            System.err.println("Error connecting to MongoDB: " + e.getMessage());
            e.printStackTrace();
        }
    }
}