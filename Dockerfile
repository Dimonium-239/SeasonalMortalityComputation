# Używamy oficjalnego obrazu Spark od Apache
FROM apache/spark:3.5.0

COPY target/classes/estat_demo_r_mwk_ts.csv /opt/input/
COPY target/SeasonalMortalityComputation-1.0-SNAPSHOT.jar /opt/app/app.jar

# Domyślne polecenie - uruchomienie aplikacji Spark
CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", "--class", "com.github.dimonium239.Main", "/opt/app/app.jar"]
