# Kafka Streams SDK Release Notes

## Version 1.0.0

We are excited to announce the release of Kafka Streams SDK version 1.0.0! This release comes with
several enhancements and libraries to simplify Kafka Streams application development. Kafka Streams
SDK provides a streamlined setup for configuring streams, reducing boilerplate code and making it
easier for developers to work with Kafka Streams.

### Libraries Included

- **Java 11**: The SDK is now compatible with Java 11, allowing you to take advantage of the latest
  Java features and improvements.

- **Confluent 7.5.0**: We have upgraded to Confluent Platform version 7.5.0, which includes various
  improvements and bug fixes. You can refer
  to [Confluent's release notes](https://docs.confluent.io/platform/current/release-notes/index.html)
  for more details.

- **Kafka 3.4.0**: Kafka Streams SDK now supports Kafka version 3.4.0, bringing the latest Kafka
  features and enhancements. Check out
  the [Kafka 3.4.0 release notes](https://archive.apache.org/dist/kafka/3.4.0/RELEASE_NOTES.html)
  for a comprehensive list of changes.

### Building the SDK

To build the Kafka Streams SDK, use the following Gradle commands:

```bash
./gradlew clean build
./gradlew clean sdk:build
./gradlew clean examples:build
```

### Testing

You can run tests using the following command:

```bash
./gradlew check
```

### Test Coverage

To generate test coverage reports, use the following command:

```bash
./gradlew reportTestCoverage
```

### References

For more information on Confluent Platform and Kafka, you can refer to the following resources:

- [Confluent Platform Release Notes](https://docs.confluent.io/platform/current/release-notes/index.html)
- [Apache Kafka 3.4.0 Release Notes](https://archive.apache.org/dist/kafka/3.4.0/RELEASE_NOTES.html)

We hope you find Kafka Streams SDK 1.0.0 useful for your Kafka Streams development. If you have any
questions or encounter any issues, please feel free to reach out to us. Happy streaming!

---

Note: For benchmarking information related to the included libraries, you can visit
the [Zstandard (zstd) benchmarks](http://facebook.github.io/zstd/#benchmarks) page.
