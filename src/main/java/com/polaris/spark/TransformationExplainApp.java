package com.polaris.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.expr;


public class TransformationExplainApp {


    public static void main(String[] args) {
        TransformationExplainApp app = new TransformationExplainApp();
        app.start();
    }


    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("Showing execution plan")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .load(
                        "data/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv");
        Dataset<Row> df0 = df;
        df0.show();
        System.out.println(df0.count());

        df = df.union(df0);
        df.show();
        System.out.println(df.count());


        df = df.withColumnRenamed("Lower Confidence Limit", "lcl");
        df = df.withColumnRenamed("Upper Confidence Limit", "ucl");
        df.show();

        df = df
                .withColumn("avg", expr("(lcl+ucl)/2"))
                .withColumn("lcl2", df.col("lcl"))
                .withColumn("ucl2", df.col("ucl"));
        df.show();

        df.explain("simple");
        System.out.println("******************************************************************");
        df.explain("extended");
        System.out.println("******************************************************************");

        df.explain("codegen");
        System.out.println("******************************************************************");

        df.explain("cost");
        System.out.println("******************************************************************");

        df.explain("formatted");

    }
}
