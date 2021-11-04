.PHONY: build
build:
	sbt package

clean:
	rm -rf target

run:
	chmod +x ./run.sh
	./run.sh
