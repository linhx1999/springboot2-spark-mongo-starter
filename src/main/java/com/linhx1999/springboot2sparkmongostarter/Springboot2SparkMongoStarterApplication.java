package com.linhx1999.springboot2sparkmongostarter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;

@SpringBootApplication
public class Springboot2SparkMongoStarterApplication {

    public static void main(String[] args) {
        SpringApplication.run(Springboot2SparkMongoStarterApplication.class, args);
    }

    @PostConstruct
    public void init() {
        String filePath = "*.csv";

        SparkSession spark = SparkSession
                .builder()
                .config("spark.mongodb.write.connection.uri", "mongodb://localhost/test")
//                .config("spark.mongodb.read.connection.uri", "mongodb://localhost/test")
                .appName("Java Spark SQL basic example")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read().option("delimiter", ";")
                .option("header", "true").csv(filePath).cache();
        df.show();
        df.write().format("mongodb")
                .mode("overwrite")
                .option("collection", "collection_name")
                .option("idFieldList", "_id")
                .option("operationType", "insert")
                .save();
    }
}
