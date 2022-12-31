package com.polaris.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.expr;


public class TransformationAndActionApp {

    public static void main(String[] args) {
        TransformationAndActionApp app = new TransformationAndActionApp();
        String mode = "noop";
        if (args.length != 0) {
            mode = args[0];
        }
        app.start(mode);
    }


    private void start(String mode) {
        long startTime = System.currentTimeMillis();

        SparkSession spark = SparkSession.builder()
                .appName("Analysing Catalyst's behavior")
                .master("local")
                .getOrCreate();
        long sessionCreationTime = System.currentTimeMillis();

        System.out.println("1. Creating a session ........... " + (sessionCreationTime - startTime));

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .load(
                        "data/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv");

        Dataset<Row> initialDf = df;

        long initialDatasetLoadTime = System.currentTimeMillis();

        System.out.println("2. Loading initial dataset ...... " + (initialDatasetLoadTime - sessionCreationTime));

        // Step 3 - Build a bigger dataset
        for (int i = 0; i < 10; i++) {
            df = df.union(initialDf);
        }
        long unionTime = System.currentTimeMillis();

        System.out.println("3. Building full dataset ........ " + (unionTime - initialDatasetLoadTime));

        // Step 4 - Cleanup. preparation

        df = df.withColumnRenamed("Lower Confidence Limit", "lcl");
        df = df.withColumnRenamed("Upper Confidence Limit", "ucl");

        long cleanupTime = System.currentTimeMillis();

        System.out.println("4. Clean-up ..................... " + (cleanupTime - unionTime));

        // Step 5 - Transformation
        if (mode.compareToIgnoreCase("noop") != 0) {
            df = df
                    .withColumn("avg", expr("(lcl+ucl)/2"))
                    .withColumn("lcl2", df.col("lcl"))
                    .withColumn("ucl2", df.col("ucl"));
            if (mode.compareToIgnoreCase("full") == 0) {
                df = df
                        .drop(df.col("avg"))
                        .drop(df.col("lcl2"))
                        .drop(df.col("ucl2"));
            }
        }
        long transformationTime = System.currentTimeMillis();

        System.out.println("5. Transformations  ............. " + (transformationTime - cleanupTime));

        // Step 6 - Action
    /*
    Returns an array that contains all rows in this Dataset.
    Running collect requires moving all the data into the application's driver process,
    and doing so on a very large dataset can crash the driver process with OutOfMemoryError.
     */
        df.collect();

        long actionTime = System.currentTimeMillis();
        System.out.println("6. Final action ................. " + (actionTime - transformationTime));
        df.show();

        System.out.println();
        System.out.println("# of records .................... " + df.count());
    }
}
