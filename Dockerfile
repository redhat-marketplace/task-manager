FROM adoptopenjdk:14.0.2_8-jdk-openj9-0.21.0 AS build
COPY . /home/gradle/src
WORKDIR /home/gradle/src
RUN ./gradlew build

FROM adoptopenjdk:14.0.2_8-jre-openj9-0.21.0
EXPOSE 8080
RUN adduser --disabled-password --gecos 'nestor,nestor,nestor,nestor,nestor' nestor
ARG JAR_FILE=build/libs/*.jar
COPY --from=build --chown=nestor:nestor /home/gradle/src/${JAR_FILE} /home/nestor/app.jar
USER nestor
ENTRYPOINT ["java", "-Djava.security.egd=file:/dev/./urandom","-jar","/home/nestor/app.jar"]