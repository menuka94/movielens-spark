.PHONY: build
build:
	sbt package

clean:
	rm -rf target

run:
	spark-submit \
	--class org.movielens.Main \
	target/scala-2.12/movielens-spark_2.12-1.0.jar
