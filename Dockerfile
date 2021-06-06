FROM adoptopenjdk/openjdk15:alpine
RUN apk --no-cache add curl
COPY target/rawdata-converter-app-xml-*.jar rawdata-converter-app-xml.jar
COPY target/classes/logback*.xml /conf/
EXPOSE 8080
CMD ["java", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005", "-Dcom.sun.management.jmxremote", "-Dmicronaut.bootstrap.context=true", "-Xmx1g", "-jar", "rawdata-converter-app-xml.jar"]
