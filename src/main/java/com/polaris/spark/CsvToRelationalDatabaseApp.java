package com.polaris.spark;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


public class CsvToRelationalDatabaseApp {


    public static void main(String[] args) {
        CsvToRelationalDatabaseApp app = new CsvToRelationalDatabaseApp();
        app.start();
    }


    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("CSV to DB")
                .master("local")
                .getOrCreate();

        Dataset<Row> dataset = spark.read()
                .format("csv")
                .option("header", "true")
                .load("data/authors.csv");

        dataset.show();

        /*
        withColumn:
        Returns a new Dataset by adding a column or replacing the existing column that has the same name.
        this method introduces a projection internally. Therefore, calling it multiple times, for instance, via loops in order to add multiple columns can generate big plans which can cause performance issues and even StackOverflowException.
        To avoid this, use select with the multiple columns at once.
         */
        dataset = dataset.withColumn(
                "name",
                concat(dataset.col("lname"), lit(", "), dataset.col("fname")));

        dataset.show();

        String dbConnectionUrl = "jdbc:postgresql://localhost/spark_labs";

        Properties prop = new Properties();
        prop.setProperty("driver", "org.postgresql.Driver");
        prop.setProperty("user", "postgres");
        prop.setProperty("password", "");


        dataset.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl, "authors_j02", prop);


        System.out.println("Process complete");
    }
}
