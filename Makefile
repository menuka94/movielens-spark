.PHONY: build
build:
	sbt package

clean:
	rm -rf target

# Q = question number
# Example (to run question 3): make run Q=3
run:
	chmod +x ./run.sh
	./run.sh $(Q)
