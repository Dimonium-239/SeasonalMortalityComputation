package com.github.dimonium239;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.DateTickUnit;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.time.Day;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import scala.collection.mutable.Seq;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.month;
import static org.apache.spark.sql.functions.substring;
import static org.jfree.chart.axis.DateTickUnitType.MONTH;

public class MortalityAnalysis {

    private final SparkSession spark;

    public MortalityAnalysis(SparkSession spark) {
        this.spark = spark;
    }

    // Główna metoda uruchamiająca cały pipeline analityczny
    public void run(String inputPath, String outputDir) {
        Dataset<Row> raw = loadData(inputPath);                          // Wczytanie danych z pliku CSV
        Dataset<Row> longFormat = reshapeData(raw);                      // Zamiana z formatu szerokiego na długi
        Dataset<Row> parsed = parseWeeks(longFormat);                    // Parsowanie tygodnia na datę i miesiąc
        Dataset<Row> aggregated = aggregateMonthly(parsed);              // Agregacja do miesięcznej średniej

        System.out.println("=== Miesięczna średnia liczba zgonów ===");
        aggregated.show(20, false);                                      // Podgląd wyników

        Dataset<Row> seasonality = computeSeasonalityBySeason(aggregated); // Analiza sezonowości

        System.out.println("=== Sezonowość wg sezonów ===");
        seasonality.show(20, false);                                     // Podgląd sezonowości

        // Zapis wyników do plików CSV
        aggregated.write().option("header", true).mode("overwrite").csv(outputDir + "/monthly_avg");
        seasonality.write().option("header", true).mode("overwrite").csv(outputDir + "/seasonality_by_season");

        // Generowanie pliku tekstowego z raportem
        generateReport(seasonality, outputDir + "/raport.txt");
        generateTimeSeriesChart(parsed, outputDir);
    }

    // Wczytanie danych CSV i zmiana nazwy kolumny z kodem kraju
    private Dataset<Row> loadData(String path) {
        return spark.read()
                .option("header", true)
                .option("delimiter", ",")
                .option("inferSchema", true)
                .csv(path)
                .withColumnRenamed("geo\\TIME_PERIOD", "country");
    }

    // Zamiana danych z formatu szerokiego (kolumny = tygodnie) na długi (kolumny: kraj, tydzień, zgony)
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

        // Tworzymy wyrażenie stack() do zamiany kolumn na wiersze
        String stackExpr = "stack(" + count + ", " + exprBuilder.substring(0, exprBuilder.length() - 2) + ") as (week, deaths)";

        return raw.selectExpr("country", stackExpr)
                .filter("deaths IS NOT NULL"); // Usuwamy puste wartości
    }

    // Parsowanie tygodnia ISO (np. 2020W02) na rok, numer tygodnia, datę i miesiąc
    private Dataset<Row> parseWeeks(Dataset<Row> longDF) {
        return longDF
                .withColumn("year", substring(col("week"), 1, 4))
                .withColumn("week_num", substring(col("week"), 7, 2).cast("int"))
                .withColumn("date", expr("date_add(to_date(concat(year, '-01-01')), (week_num - 1) * 7)"))
                .withColumn("month", month(col("date")))
                .withColumn("deaths", col("deaths").cast("int"));
    }

    // Agregacja miesięczna: średnia liczba zgonów na kraj i miesiąc
    private Dataset<Row> aggregateMonthly(Dataset<Row> parsedDF) {
        return parsedDF.groupBy("country", "month")
                .agg(avg("deaths").alias("avg_deaths"))
                .orderBy("country", "month");
    }

    // Analiza sezonowości – średnia liczba zgonów w porach roku
    private Dataset<Row> computeSeasonalityBySeason(Dataset<Row> monthlyAgg) {
        // Dodanie kolumny sezon na podstawie miesiąca
        Dataset<Row> withSeason = monthlyAgg.withColumn("season", expr(
                "CASE " +
                        "WHEN month IN (12, 1, 2) THEN 'Winter' " +
                        "WHEN month IN (3, 4, 5) THEN 'Spring' " +
                        "WHEN month IN (6, 7, 8) THEN 'Summer' " +
                        "WHEN month IN (9, 10, 11) THEN 'Autumn' " +
                        "END"));

        // Agregacja średnich zgonów per sezon i kraj
        Dataset<Row> seasonalAvg = withSeason.groupBy("country", "season")
                .agg(avg("avg_deaths").alias("season_avg_deaths"));

        // Pivot – każdy kraj ma wiersz, a sezony są kolumnami
        Dataset<Row> pivot = seasonalAvg.groupBy("country")
                .pivot("season", java.util.Arrays.asList("Winter", "Spring", "Summer", "Autumn"))
                .agg(first("season_avg_deaths"));

        // Stworzenie arraya struktur (season, value) do dalszego porównania
        Dataset<Row> withMaxMin = pivot.withColumn("season_values", array(
                struct(lit("Winter").alias("season"), col("Winter").alias("value")),
                struct(lit("Spring").alias("season"), col("Spring").alias("value")),
                struct(lit("Summer").alias("season"), col("Summer").alias("value")),
                struct(lit("Autumn").alias("season"), col("Autumn").alias("value"))
        ));

        // Rejestracja UDF do znalezienia sezonu z max i min wartością
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

        // Obliczenia: max/min sezon, wartość, indeks sezonowości, flaga anomalii
        Dataset<Row> withSeasons = withMaxMin
                .withColumn("max_season", callUDF("getMaxSeason", col("season_values")))
                .withColumn("min_season", callUDF("getMinSeason", col("season_values")))
                .withColumn("max_value", expr("filter(season_values, x -> x.season = max_season)[0].value"))
                .withColumn("min_value", expr("filter(season_values, x -> x.season = min_season)[0].value"))
                .withColumn("seasonality_index", col("max_value").minus(col("min_value")))
                .withColumn("is_anomaly", col("seasonality_index").gt(1500))
                .orderBy(col("seasonality_index").desc());

        // Zwrot końcowej tabeli
        return withSeasons.select("country", "max_season", "max_value", "min_season", "min_value", "seasonality_index", "is_anomaly");
    }

    // Generowanie tekstowego raportu sezonowości
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

            // Format czytelny dla człowieka
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

        // Zapis do pliku
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {
            writer.write(report.toString());
            System.out.println("Raport zapisany do: " + outputPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void generateTimeSeriesChart(Dataset<Row> parsedDF, String outputPath) {
        try {
            // Dane do listy
            List<Row> rows = parsedDF
                    .select("country", "date", "deaths")
                    .orderBy("country", "date")
                    .collectAsList();

            // Oddzielne serie dla EU, PL, DE i reszty
            TimeSeries euSeries = null;
            TimeSeries plSeries = null;
            TimeSeries frSeries = null;
            TimeSeries deSeries = null;
            Map<String, TimeSeries> otherCountriesSeries = new HashMap<>();

            for (Row row : rows) {
                String country = row.getAs("country");
                java.sql.Date sqlDate = row.getAs("date");
                Integer deaths = row.getAs("deaths");

                if (country == null || sqlDate == null || deaths == null) continue;

                LocalDate localDate = sqlDate.toLocalDate();
                Day day = new Day(localDate.getDayOfMonth(), localDate.getMonthValue(), localDate.getYear());

                if (country.equalsIgnoreCase("EU") || country.equalsIgnoreCase("EU27_2020")) {
                    if (euSeries == null) {
                        euSeries = new TimeSeries("EU");
                    }
                    euSeries.addOrUpdate(day, deaths);
                } else if(country.equalsIgnoreCase("PL")) {
                    if (plSeries == null) {
                        plSeries = new TimeSeries("PL");
                    }
                    plSeries.addOrUpdate(day, deaths);
                } else if(country.equalsIgnoreCase("FR")) {
                    if (frSeries == null) {
                        frSeries = new TimeSeries("FR");
                    }
                    frSeries.addOrUpdate(day, deaths);
                } else if(country.equalsIgnoreCase("DE")) {
                    if (deSeries == null) {
                        deSeries = new TimeSeries("DE");
                    }
                    deSeries.addOrUpdate(day, deaths);
                } else {
                    otherCountriesSeries.putIfAbsent(country, new TimeSeries(country));
                    otherCountriesSeries.get(country).addOrUpdate(day, deaths);
                }
            }

            // Wykres dla EU, jeśli dane istnieją
            if (euSeries != null && !euSeries.isEmpty()) {
                TimeSeriesCollection euDataset = new TimeSeriesCollection();
                euDataset.addSeries(euSeries);
                generateChart(euDataset, "Zgony w czasie - EU", outputPath + "/zgony_EU.png");
            }

            // Wykres dla PL, jeśli dane istnieją
            if (plSeries != null && !plSeries.isEmpty()) {
                TimeSeriesCollection plDataset = new TimeSeriesCollection();
                plDataset.addSeries(plSeries);
                generateChart(plDataset, "Zgony w czasie - PL", outputPath + "/zgony_PL.png");
            }

            // Wykres dla FR, jeśli dane istnieją
            if (frSeries != null && !frSeries.isEmpty()) {
                TimeSeriesCollection frDataset = new TimeSeriesCollection();
                frDataset.addSeries(frSeries);
                generateChart(frDataset, "Zgony w czasie - FR", outputPath + "/zgony_FR.png");
            }

            // Wykres dla DE, jeśli dane istnieją
            if (deSeries != null && !deSeries.isEmpty()) {
                TimeSeriesCollection deDataset = new TimeSeriesCollection();
                deDataset.addSeries(deSeries);
                generateChart(deDataset, "Zgony w czasie - DE", outputPath + "/zgony_DE.png");
            }

            // Wykres dla pozostałych krajów
            if (!otherCountriesSeries.isEmpty()) {
                TimeSeriesCollection othersDataset = new TimeSeriesCollection();
                for (TimeSeries ts : otherCountriesSeries.values()) {
                    if (!ts.isEmpty()) {
                        othersDataset.addSeries(ts);
                    }
                }
                generateChart(othersDataset, "Zgony w czasie - inne państwa", outputPath + "/zgony_reszta.png");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    private void generateChart(TimeSeriesCollection dataset, String title, String outputPath) throws IOException {
        JFreeChart chart = ChartFactory.createTimeSeriesChart(
                title,
                "Data",
                "Liczba zgonów",
                dataset,
                true, true, false
        );

        XYPlot plot = chart.getXYPlot();
        DateAxis axis = (DateAxis) plot.getDomainAxis();

        // Format daty: miesiąc i rok
        axis.setDateFormatOverride(new SimpleDateFormat("MMM yyyy", Locale.forLanguageTag("pl")));
        axis.setVerticalTickLabels(true);

        // Wymuś ticki co 3 miesiące
        axis.setTickUnit(new DateTickUnit(MONTH, 3));

        // (opcjonalnie) pochyl etykiety, żeby się nie nachodziły
        axis.setVerticalTickLabels(true);

        // Zapis
        int width = 1400;
        int height = 800;
        File chartFile = new File(outputPath);
        ChartUtils.saveChartAsPNG(chartFile, chart, width, height);
        System.out.println("Wykres zapisany do: " + chartFile.getAbsolutePath());
    }


}
