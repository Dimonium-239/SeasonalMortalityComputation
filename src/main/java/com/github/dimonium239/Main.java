package com.github.dimonium239;

import org.apache.spark.sql.SparkSession;

import java.io.FileWriter;
import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        String inputPath = "/opt/input/estat_demo_r_mwk_ts.csv";
        String outputPath = "/tmp/output";
        clearBenchmarkFile(outputPath);
        runBenchmark("local[1]", inputPath, outputPath, "Lokalny (1 wątek)");
        int cores = Runtime.getRuntime().availableProcessors();
        runBenchmark("local[*]", inputPath, outputPath, "Lokalny (" + cores + ")");
    }

    private static void runBenchmark(String master, String inputPath, String outputPath, String label) {
        SparkSession spark = SparkSession.builder()
                .appName("Mortality Benchmark: " + label)
                .master(master)
                .getOrCreate();

        MortalityAnalysis analysis = new MortalityAnalysis(spark);

        long start = System.currentTimeMillis();
        analysis.run(inputPath, outputPath);
        long end = System.currentTimeMillis();
        long duration = end - start;

        System.out.printf("Tryb: %s, Czas: %d ms%n", label, duration);

        saveBenchmark(outputPath, label, duration);
        spark.stop();
    }

    private static void clearBenchmarkFile(String outputPath) {
        try (FileWriter fw = new FileWriter(outputPath + "/benchmark.txt", false)) {
            // Open in overwrite mode (append = false), writing empty string clears file
            fw.write("");
            System.out.println("Plik benchmark.txt został wyczyszczony.");
        } catch (IOException e) {
            System.err.println("Nie można wyczyścić pliku benchmark.txt: " + e.getMessage());
        }
    }

    private static void saveBenchmark(String outputPath, String label, long duration) {
        String line = String.format("| %-25s | %10d ms |\n", label, duration);
        try (FileWriter fw = new FileWriter(outputPath + "/benchmark.txt", true)) {
            fw.write(line);
        } catch (IOException e) {
            System.err.println("Nie można zapisać benchmarku: " + e.getMessage());
        }
    }
}
