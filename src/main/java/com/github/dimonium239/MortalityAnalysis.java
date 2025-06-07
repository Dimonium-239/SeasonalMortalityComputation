package com.github.dimonium239;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.mutable.Seq;

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

        System.out.println("=== Miesięczna średnia liczba zgonów ===");
        aggregated.show(20, false);

        Dataset<Row> seasonality = computeSeasonalityBySeason(aggregated);

        System.out.println("=== Sezonowość wg sezonów ===");
        seasonality.show(20, false);

        aggregated.write().option("header", true).mode("overwrite").csv(outputDir + "/monthly_avg");
        seasonality.write().option("header", true).mode("overwrite").csv(outputDir + "/seasonality_by_season");

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

    // Metoda wyliczająca sezonowość na podstawie sezonów
    private Dataset<Row> computeSeasonalityBySeason(Dataset<Row> monthlyAgg) {
        // Dodajemy kolumnę season na podstawie miesiąca
        Dataset<Row> withSeason = monthlyAgg.withColumn("season", expr(
                "CASE " +
                        "WHEN month IN (12, 1, 2) THEN 'Winter' " +
                        "WHEN month IN (3, 4, 5) THEN 'Spring' " +
                        "WHEN month IN (6, 7, 8) THEN 'Summer' " +
                        "WHEN month IN (9, 10, 11) THEN 'Autumn' " +
                        "END"));

        // Grupujemy po kraju i sezonie, liczymy średnią zgonów w sezonie
        Dataset<Row> seasonalAvg = withSeason.groupBy("country", "season")
                .agg(avg("avg_deaths").alias("season_avg_deaths"));

        // Teraz pivotujemy by mieć wiersz na kraj i kolumny dla sezonów
        Dataset<Row> pivot = seasonalAvg.groupBy("country")
                .pivot("season", java.util.Arrays.asList("Winter", "Spring", "Summer", "Autumn"))
                .agg(first("season_avg_deaths"));

        // Obliczamy max, min i ich nazwy sezonów oraz indeks sezonowości
        // Najpierw array z sezonami i wartościami dla łatwego porównania
        Dataset<Row> withMaxMin = pivot.withColumn("season_values", array(
                struct(lit("Winter").alias("season"), col("Winter").alias("value")),
                struct(lit("Spring").alias("season"), col("Spring").alias("value")),
                struct(lit("Summer").alias("season"), col("Summer").alias("value")),
                struct(lit("Autumn").alias("season"), col("Autumn").alias("value"))
        ));

        spark.udf().register("getMaxSeason", (UDF1<Seq<Row>, String>) arr -> {
            Row maxRow = null;
            for (int i = 0; i < arr.size(); i++) {
                Row r = arr.apply(i);
                if (r.isNullAt(1)) continue;
                if (maxRow == null || r.getDouble(1) > maxRow.getDouble(1)) maxRow = r;
            }
            return maxRow == null ? null : maxRow.getString(0);
        }, DataTypes.StringType);

        spark.udf().register("getMinSeason", (UDF1<Seq<Row>, String>) arr -> {
            Row minRow = null;
            for (int i = 0; i < arr.size(); i++) {
                Row r = arr.apply(i);
                if (r.isNullAt(1)) continue;
                if (minRow == null || r.getDouble(1) < minRow.getDouble(1)) minRow = r;
            }
            return minRow == null ? null : minRow.getString(0);
        }, DataTypes.StringType);

        Dataset<Row> withSeasons = withMaxMin
                .withColumn("max_season", callUDF("getMaxSeason", col("season_values")))
                .withColumn("min_season", callUDF("getMinSeason", col("season_values")))
                .withColumn("max_value", expr("filter(season_values, x -> x.season = max_season)[0].value"))
                .withColumn("min_value", expr("filter(season_values, x -> x.season = min_season)[0].value"))
                .withColumn("seasonality_index", col("max_value").minus(col("min_value")))
                .orderBy(col("seasonality_index").desc());

        return withSeasons.select("country", "max_season", "max_value", "min_season", "min_value", "seasonality_index");
    }

    private void generateReport(Dataset<Row> seasonality, String outputPath) {
        List<Row> rows = seasonality.collectAsList();

        StringBuilder report = new StringBuilder();
        report.append("=== RAPORT SEZONOWOŚCI DLA KAŻDEGO KRAJU ===\n\n");

        for (Row row : rows) {
            String country = row.getAs("country");
            String maxSeason = row.getAs("max_season");
            Double maxVal = row.getAs("max_value");
            String minSeason = row.getAs("min_season");
            Double minVal = row.getAs("min_value");
            Double index = row.getAs("seasonality_index");

            String line = String.format(
                    "Kraj: %s\n  Sezon z najwyższą średnią zgonów: %s (%d)\n  Sezon z najniższą średnią zgonów: %s (%d)\n  Różnica sezonowa: %d\n-----------------------------\n",
                    CountryMapper.isoAlpha2ToCountryName().getOrDefault(country, country),
                    maxSeason, maxVal == null ? 0 : Math.round(maxVal),
                    minSeason, minVal == null ? 0 : Math.round(minVal),
                    index == null ? 0 : Math.round(index)
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



