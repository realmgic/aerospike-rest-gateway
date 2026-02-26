FROM eclipse-temurin:17-jdk AS build
WORKDIR /workspace/app

# Needed for arm64 support. Open gradle issue: https://github.com/gradle/gradle/issues/18212
ENV JAVA_OPTS="-Djdk.lang.Process.launchMechanism=vfork"

COPY . /workspace/app
RUN apt-get -y update && apt-get -y install git
RUN ./gradlew clean build -x test

# Copy the non-plain jar into /build/dependency
RUN mkdir -p build/dependency && cd build/dependency && jar -xf $(ls ../libs/*.jar | grep -v "plain.jar")

FROM eclipse-temurin:17-jre
VOLUME /tmp
ARG DEPENDENCY=/workspace/app/build/dependency
COPY --from=build ${DEPENDENCY}/META-INF /app/META-INF
COPY --from=build ${DEPENDENCY}/BOOT-INF/lib /app/lib
COPY --from=build ${DEPENDENCY}/BOOT-INF/classes /app

RUN adduser --system --no-create-home --group appuser && chown -R appuser:appuser /app \
    && apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
USER appuser

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \
  CMD curl -f http://localhost:8080/actuator/health/liveness || exit 1
ENTRYPOINT ["java","-cp","app:app/lib/*","com.aerospike.restclient.AerospikeRestGatewayApplication"]
