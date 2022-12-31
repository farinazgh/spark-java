package com.polaris.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class ArrayToDatasetToDataframeApp {

    public static void main(String[] args) {
        ArrayToDatasetToDataframeApp app = new ArrayToDatasetToDataframeApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Array to dataframe")
                .master("local")
                .getOrCreate();

        String[] stringList = new String[]{"Rouly", "Tag", "Nini", "Betty", "Duggee", "Youpi"};
        List<String> data = Arrays.asList(stringList);
        // spark.createDataset
        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
        ds.show();
        ds.printSchema();

        Dataset<Row> df = ds.toDF();
        df.show();
        df.printSchema();
    }
}
