package com.example.sparkapp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkWordCount {

    public static void main(String[] args) {
        // Create a SparkConf object
        SparkConf conf = new SparkConf()
                .setAppName("WordCount")
                .setMaster("local[*]"); // Run Spark locally using all available cores

        // Create a Java Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read input data as an RDD of lines
        JavaRDD<String> lines = sc.textFile("data/StringSmall1.txt");

        // Split each line into words
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        // Map each word to a (word, 1) tuple
        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));

        // Reduce by key to count occurrences of each word
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey((a, b) -> a + b);

        // Print the word counts
        wordCounts.collect().forEach(System.out::println);

        // Stop the Spark context
        sc.close();
    }
}