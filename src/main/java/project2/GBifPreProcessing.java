package project2;

import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.locationtech.jts.geom.Geometry;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class GBifPreProcessing {

    // Task 2.2.a: Create Sedona session with MongoDB configuration
    public SparkSession getSedonaMongoSession() {
        SparkSession session = SparkSession.builder()
                .appName("GBif Preprocessing")
                .master("local[*]")
                .config("spark.serializer", KryoSerializer.class.getName())
                .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
                // MongoDB Connector 10.x configuration
                .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017")
                .config("spark.mongodb.read.database", "biodiv")
                .config("spark.mongodb.read.collection", "gbif")
                .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017")
                .config("spark.mongodb.write.database", "biodiv")
                .config("spark.mongodb.write.collection", "gbif")
                .getOrCreate();

        // Register Sedona SQL functions
        SedonaSQLRegistrator.registerAll(session);

        return session;
    }

    // Task 2.2.b: Read GBIF GeoJSON file
    public Dataset<Row> readGbif(SparkSession spark, String filePath) {
        // Read GeoJSON file
        Dataset<Row> rawData = spark.read()
                .format("json")
                .option("multiline", "true")
                .load(filePath);

        // Explode features array and extract properties and geometry
        Dataset<Row> features = rawData
                .select(explode(col("features")).as("feature"))
                .select(
                        col("feature.properties.*"),
                        col("feature.geometry").as("geometry")
                );

        // Convert geometry to Sedona geometry type
        features.createOrReplaceTempView("features_temp");

        Dataset<Row> result = spark.sql(
                        "SELECT *, ST_GeomFromGeoJSON(to_json(geometry)) as geometry_sedona " +
                                "FROM features_temp"
                ).drop("geometry")
                .withColumnRenamed("geometry_sedona", "geometry");

        return result;
    }

    // Task 2.2.c: Read communities shapefile
    public Dataset<Row> readCommunities(SparkSession spark, String filePath) {
        // Read shapefile using Sedona
        // Note: The path should be to the .shp file
        spark.read()
                .format("shapefile")
                .load(filePath)
                .createOrReplaceTempView("shapefile_temp");

        // Select all columns and ensure geometry is properly named
        Dataset<Row> communities = spark.sql("SELECT * FROM shapefile_temp");

        return communities;
    }

    // Task 2.2.d: Spatial join GBIF observations with communities
    public Dataset<Row> spatiallyJoin(Dataset<Row> gbifData, Dataset<Row> communities) {
        // Register DataFrames as temporary views
        gbifData.createOrReplaceTempView("gbif");
        communities.createOrReplaceTempView("communities");

        // Get column names to avoid duplicates
        String[] gbifCols = gbifData.columns();
        String[] communityCols = communities.columns();

        // Build SELECT clause for gbif columns
        StringBuilder gbifSelect = new StringBuilder();
        for (String col : gbifCols) {
            if (!col.equals("geometry")) {
                gbifSelect.append("gbif.").append(col).append(" as gbif_").append(col).append(", ");
            }
        }

        // Build SELECT clause for community columns (excluding geometry)
        StringBuilder communitySelect = new StringBuilder();
        for (String col : communityCols) {
            if (!col.equals("geometry")) {
                communitySelect.append("communities.").append(col).append(" as ").append(col).append(", ");
            }
        }

        // Keep only gbif geometry
        String selectClause = gbifSelect.toString() + communitySelect.toString() + "gbif.geometry";

        // Perform spatial join using ST_Contains
        SparkSession spark = gbifData.sparkSession();
        Dataset<Row> joined = spark.sql(
                "SELECT " + selectClause + " " +
                        "FROM gbif, communities " +
                        "WHERE ST_Contains(communities.geometry, gbif.geometry)"
        );

        // Rename gbif_ prefixed columns back to original names
        Dataset<Row> result = joined;
        for (String col : gbifCols) {
            if (!col.equals("geometry") && joined.columns().length > 0) {
                String oldName = "gbif_" + col;
                if (Arrays.asList(joined.columns()).contains(oldName)) {
                    result = result.withColumnRenamed(oldName, col);
                }
            }
        }

        return result;
    }

    // Task 2.2.e: Write joined data to MongoDB with GeoJSON geometry
    public void writeJoinedData(Dataset<Row> joinedData) {
        // Convert Sedona geometry back to GeoJSON format
        joinedData.createOrReplaceTempView("joined_temp");
        SparkSession spark = joinedData.sparkSession();

        // Use ST_AsGeoJSON to convert geometry to GeoJSON string
        Dataset<Row> withGeoJson = spark.sql(
                "SELECT *, ST_AsGeoJSON(geometry) as geometry_json FROM joined_temp"
        ).drop("geometry");

        // Define the schema for GeoJSON geometry
        StructType geoJsonSchema = new StructType()
                .add("type", DataTypes.StringType)
                .add("coordinates", DataTypes.createArrayType(DataTypes.DoubleType));

        // Parse the GeoJSON string into a proper structure
        Dataset<Row> finalData = withGeoJson.withColumn(
                "geometry",
                from_json(col("geometry_json"), geoJsonSchema)
        ).drop("geometry_json");

        // Write to MongoDB using MongoDB Connector 10.x format
        finalData.write()
                .format("mongodb")
                .mode(SaveMode.Overwrite)
                .option("connection.uri", "mongodb://localhost:27017")
                .option("database", "biodiv")
                .option("collection", "gbif")
                .save();

        System.out.println("Successfully wrote " + finalData.count() + " records to MongoDB");
    }

    public static void main(String[] args) {
        GBifPreProcessing preprocessing = new GBifPreProcessing();

        // Task a: Create Sedona session with MongoDB config
        SparkSession spark = preprocessing.getSedonaMongoSession();

        try {
            // Task b: Read GBIF data
            Dataset<Row> gbifData = preprocessing.readGbif(spark, "resources/flora_germany.geojson");
            System.out.println("GBIF data schema:");
            gbifData.printSchema();
            System.out.println("GBIF records: " + gbifData.count());

            // Task c: Read communities
            Dataset<Row> communities = preprocessing.readCommunities(spark, "resources/communities.shp");
            System.out.println("\nCommunities schema:");
            communities.printSchema();
            System.out.println("Communities count: " + communities.count());

            // Task d: Spatial join
            Dataset<Row> joinedData = preprocessing.spatiallyJoin(gbifData, communities);
            System.out.println("\nJoined data schema:");
            joinedData.printSchema();
            System.out.println("Joined data count: " + joinedData.count());

            // Task e: Write to MongoDB
            preprocessing.writeJoinedData(joinedData);

        } catch (Exception e) {
            System.err.println("Error during processing: " + e.getMessage());
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }
}