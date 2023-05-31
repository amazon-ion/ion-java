# Amazon Ion Java CLI
A Java implementation of CLI where its design document is located in [here](https://github.com/amazon-ion/ion-test-driver#design).

The package is stored under `ion-java/ion-java-cli`.

## Setup
Build ion-java-cli.
```
./gradlew ion-java-cli:build
```

## Getting Started
Running the test driver CLI.

```
./gradlew ion-java-cli:run -q --args="process test_file.ion -f pretty -o output.ion"
```
