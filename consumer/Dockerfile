FROM maven:3.8-jdk-11 as mavenBuild
COPY pom.xml pom.xml
COPY src src
RUN ["mvn", "package"]

FROM openjdk:11
COPY --from=mavenBuild ./target/*.jar ./
ENV JAVA_OPTS ""
CMD [ "bash", "-c", "java ${JAVA_OPTS} -jar *.jar"]