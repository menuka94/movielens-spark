#!/bin/bash

source env.sh;
spark-submit \
	--class org.movielens.Main \
	target/scala-2.12/movielens-spark_2.12-1.0.jar;