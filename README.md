# Kafka Streams workshop

This repository contains examples and exercises for the Kafka Streams workshop.

## Examples

Code examples from the workshop slides are available in
`src/main/java/kafkastreams/javaexamples` and
`src/main/scala/kafkastreams/scalaexamples`

## Exercises

The goal of the exercises is to get the various tests to go green by
implementing missing functionality in the empty methods.

You can find the exercises in `src/main/java/kafkastreams/javaexercises`

The corresponding tests are located in
`src/test/scala/kafkastreams/exercises`

### Solving exercises in Scala

If you want to implement the exercises using Scala instead of Java,
open `src/test/scala/kafkastreams/exercises/ExerciseBase.scala` and
include the import for the Scala exercises instead of Java.

The Scala exercises are located in
`src/main/scala/kafkastreams/scalaexercises`

## Running tests

To run the tests from Gradle:
```
./gradlew test
```

To run in IntelliJ:
- Right click on test class in Project view and select `Run 'Exercise...'`, or
- Open test class in editor and press Ctrl+Shift+F10 (on Mac)
