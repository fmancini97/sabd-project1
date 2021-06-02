#!/usr/bin/env sh
docker-compose exec spark spark-submit --class it.uniroma2.ing.dicii.sabd.QueryExecutor --master local /queries/project1-1.0-SNAPSHOT.jar
