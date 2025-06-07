package com.github.dimonium239;

import org.apache.spark.sql.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import static org.apache.spark.sql.functions.*;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.month;
import static org.apache.spark.sql.functions.substring;

public class MortalityAnalysis {
    private final SparkSession spark;

    public MortalityAnalysis(SparkSession spark) {
        this.spark = spark;
    }

    public void run(String inputPath, String outputDir) {
        Dataset<Row> raw = loadData(inputPath);
        Dataset<Row> longFormat = reshapeData(raw);
        Dataset<Row> parsed = parseWeeks(longFormat);
        Dataset<Row> aggregated = aggregateMonthly(parsed);
        Dataset<Row> seasonality = computeSeasonality(aggregated);

        // Wyświetl przykładowe dane
        System.out.println("=== Miesięczna średnia liczba zgonów ===");
        aggregated.show(20, false);

        System.out.println("=== Wskaźnik sezonowości (największe różnice) ===");
        seasonality.show(20, false);

        // Zapis danych
        aggregated.write().option("header", true).mode("overwrite").csv(outputDir + "/monthly_avg");
        seasonality.write().option("header", true).mode("overwrite").csv(outputDir + "/seasonality");

        // Raport tekstowy
        generateReport(seasonality, outputDir + "/raport.txt");
    }

    private Dataset<Row> loadData(String path) {
        return spark.read()
                .option("header", true)
                .option("delimiter", ",")
                .option("inferSchema", true)
                .csv(path)
                .withColumnRenamed("geo\\TIME_PERIOD", "country");
    }

    private Dataset<Row> reshapeData(Dataset<Row> raw) {
        String[] columns = raw.columns();
        StringBuilder exprBuilder = new StringBuilder();
        int count = 0;

        for (String colName : columns) {
            if (!colName.equals("freq") && !colName.equals("sex") && !colName.equals("unit") && !colName.equals("country")) {
                exprBuilder.append("'").append(colName).append("', `").append(colName).append("`, ");
                count++;
            }
        }

        String stackExpr = "stack(" + count + ", " + exprBuilder.substring(0, exprBuilder.length() - 2) + ") as (week, deaths)";

        return raw.selectExpr("country", stackExpr)
                .filter("deaths IS NOT NULL");
    }

    private Dataset<Row> parseWeeks(Dataset<Row> longDF) {
        return longDF
                .withColumn("year", substring(col("week"), 1, 4))
                .withColumn("week_num", substring(col("week"), 7, 2).cast("int"))
                .withColumn("date", expr("date_add(to_date(concat(year, '-01-01')), (week_num - 1) * 7)"))
                .withColumn("month", month(col("date")))
                .withColumn("deaths", col("deaths").cast("int"));
    }

    private Dataset<Row> aggregateMonthly(Dataset<Row> parsedDF) {
        return parsedDF.groupBy("country", "month")
                .agg(avg("deaths").alias("avg_deaths"))
                .orderBy("country", "month");
    }

    private Dataset<Row> computeSeasonality(Dataset<Row> monthlyAgg) {
        return monthlyAgg.groupBy("country")
                .agg(
                        max("avg_deaths").alias("max_avg"),
                        min("avg_deaths").alias("min_avg")
                )
                .withColumn("seasonality_index", col("max_avg").minus(col("min_avg")))
                .orderBy(col("seasonality_index").desc());
    }

    private void generateReport(Dataset<Row> seasonality, String outputPath) {
        List<Row> rows = seasonality.collectAsList();

        StringBuilder report = new StringBuilder();
        report.append("=== RAPORT SEZONOWOŚCI DLA KAŻDEGO KRAJU ===\n\n");

        for (Row row : rows) {
            String country = row.getAs("country");
            Double maxVal = row.getAs("max_avg");
            Double minVal = row.getAs("min_avg");
            Double index = row.getAs("seasonality_index");

            String line = String.format(
                    "Kraj: %s\n  Średnia liczba zgonów zimą (max): %d\n  Średnia liczba zgonów latem (min): %d\n  Różnica sezonowa: %d\n-----------------------------\n",
                    CountryMapper.isoAlpha2ToCountryName().get(country),
                    maxVal == null ? null : Math.round(maxVal),
                    minVal == null ? null :Math.round(minVal),
                    index == null ? null : Math.round(index)
            );
            System.out.print(line);
            report.append(line);
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {
            writer.write(report.toString());
            System.out.println("Raport zapisany do: " + outputPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


