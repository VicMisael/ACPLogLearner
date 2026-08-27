FROM docker.io/library/maven:3.9.11-eclipse-temurin-17 AS builder

# Pin the upstream source so thesis experiments remain reproducible.
ARG BENCHMARKSQL_COMMIT=98f5cb4b446f4296ca5e85ad27303482a6bbd624
WORKDIR /source
RUN git init \
    && git remote add origin https://github.com/pgsql-io/benchmarksql.git \
    && git fetch --depth 1 origin "${BENCHMARKSQL_COMMIT}" \
    && git checkout --detach FETCH_HEAD \
    && mvn -DskipTests package

FROM docker.io/library/eclipse-temurin:17-jre
WORKDIR /benchmarksql

COPY --from=builder /source/target/BenchmarkSQL.jar ./BenchmarkSQL.jar
COPY --from=builder /source/target/lib ./lib
COPY --from=builder /source/target/run ./run
COPY --chmod=755 docker/benchmarksql/entrypoint.sh ./entrypoint.sh

RUN mkdir -p /benchmarksql/results

ENTRYPOINT ["/benchmarksql/entrypoint.sh"]
CMD ["build"]
