package com.polaris.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;


public class ArrayToDatasetApp {

    public static void main(String[] args) {
        ArrayToDatasetApp app = new ArrayToDatasetApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Array to Dataset<String>")
                .master("local")
                .getOrCreate();

        String[] stringList = new String[]{"Rouly", "Tag", "Nini", "Betty", "Duggee", "Youpi"};
        List<String> data = Arrays.asList(stringList);
        // spark.createDataset
        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
        ds.show();
        ds.printSchema();
    }
}
