# Use an official Java runtime as a parent image
FROM openjdk:17-jdk-slim

# Set the working directory in the container
WORKDIR /dataloader

# Copy the jar file from the build directory to the container
COPY ${JAR_FILE} dataloader-${project.version}.jar

# Run the jar file
ENTRYPOINT ["java", "-jar", "app.jar"]