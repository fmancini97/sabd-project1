# SABD Project 1

Authors: Francesco Mancini, Fabiano Veglianti

## Requirements

- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/) 
- [Maven](https://maven.apache.org/)

## Building and Deployment 

Use maven to build the project
```shell
mvn package
```

Project deployment is done using docker containers managed by docker compose.
```shell
cd ./docker-compose
docker-compose up --scale spark-worker=<number of replicas>
```

#### Frameworks:

 - [<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/f/f3/Apache_Spark_logo.svg/1200px-Apache_Spark_logo.svg.png" width=70px>](https://spark.apache.org/)

    - Web UI port: 4040

 - [<img src="https://uploads-ssl.webflow.com/5abbd6c80ca1b5830c921e17/5ad766e2a1a548ee4fc61cf6_hadoop%20(1).png" width=70px>](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html)

    - Web UI port: 9870

 - [<img src="https://miro.medium.com/max/400/1*b-i9e82pUCgJbsg3lpdFnA.jpeg" width=70px>](https://nifi.apache.org/)
   
    - Web UI port: 8085 

 - [<img src="https://upload.wikimedia.org/wikipedia/en/3/31/Cockroach_Labs_Logo.png" width=70px>](https://www.cockroachlabs.com/)
    
    - Web UI port: 8091 

 - [<img src="https://secure.gravatar.com/avatar/31cea69afa424609b2d83621b4d47f1d.jpg?s=80&r=g&d=mm" width=70px>](https://grafana.com/)
    - Web UI port: 3000
    - Username: admin
    - Password: sabdsabd 

## Queries on Covid-19 Opendata Vaccini

The program allows to perform three different queries on Covid-19 Opendata Vaccini.

####Query1 
Using [somministrazioni-vaccini-summary-latest.csv](https://github.com/italia/covid19-opendata-vaccini/blob/master/dati/somministrazioni-vaccini-summary-latest.csv) and
[punti-somministrazione-tipologia.csv](https://github.com/italia/covid19-opendata-vaccini/blob/master/dati/punti-somministrazione-tipologia.csv), for each month and for each area, calculate the average number of vaccinations that was carried out daily in a generic vaccination center in that area and during that month. Consider the data starting from January 1, 2021. 

####Query2
Using [somministrazioni-vaccini-latest.csv](https://github.com/italia/covid19-opendata-vaccini/blob/master/dati/somministrazioni-vaccini-latest.csv), for women, for each age group and for each month, determine the first 5 areas for which the greatest number of vaccinations on the first day of the following month. To determine the monthly ranking and predict the number of vaccinations, consider the regression line that approximates the trend of daily vaccinations. For query resolution, consider only the categories for which in the month at least two days of vaccination campaign are recorded. It is also required to calculate the ranking for each month and category starting from the data collected from February 1, 2021.

####Query3
Using [somministrazioni-vaccini-summary-latest.csv](https://github.com/italia/covid19-opendata-vaccini/blob/master/dati/somministrazioni-vaccini-summary-latest.csv), estimate the total number of vaccinations carried out on 1 June 2021 starting from 27 December 2020 considering all age groups and, using a clustering algorithm, classify the areas in K cluster considering for each area the estimate of the percentage of the vaccinated population. Compare clustering quality and performance measured in terms of processing time for the two clustering algorithms considering K from 2 to 5.

Queries execution is done running
```bash
cd ./docker-compose
./execute-queries.sh
```

Queries 1 and 2 are performed using both **Spark Core** and **Spark SQL**.

## HDFS directory tree
```
/
+-- sabd/
    +-- input/
    +-- output/
        +-- query1Result/
        +-- query2Result/
        +-- query3Result/
        +-- query3Benchmark.csv
        +-- queriesBenchmark.csv
```

## Results
Results are stored in *Results* folder.
