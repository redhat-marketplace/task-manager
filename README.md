# Task Manager
The Task Manager is a multi-tenant task sequencing, scheduling and execution service that compliments an application's core function by taking the overhead of tracking and managing jobs of any kind. It has the capability to recover from failures that may be expected in a distributed cloud microservice environment using an optimistic decaying algorithm. It provides transparent notifications on any events that occur while running a job and gives in-context details to assist with troubleshooting and automatic recovery of errors. It supports out of the box notifications through slack, pager duty and emails which allows task owners to stay on top of the status of execution by defining relevant rules.

## Development setup
### Java
The application runs using AdoptOpenJDK version 14.0.2_8 on the OpenJ9 VM. These steps below help set up to run in the same environment.

#### MacOS and Linux
On MacOS/Linux to manage the environment, SDKMAN! is recommended. Read more about it and follow the instructions here https://sdkman.io/ to setup SDKMAN! Then use it to install and use the necessary JDK.

This will install java but you need not set it to the current or default version.
`sdk install java 14.0.2-open`

Run the following command to set the newly installed version as the current version for the terminal session.
`sdk use java 14.0.2-open`

If you used SDKMAN! to install java then the install path typically be `$HOME/.sdkman/candidates/java/14.0.2.j9-adpt` on MacOS.

#### Windows
You can go to https://adoptopenjdk.net/ and download the installer.

### Build and run
Run the build using `./gradlew build`

Run the Task Manager with `./gradlew bootRun`

Verify the application runs by executing `curl localhost:8080/actuator/health`

### Docker setup, build and running
Install Docker Desktop https://docs.docker.com/get-docker/

Build the container `docker build -t ibmcom/nestor:0.0.1 .`

Run the container `docker run --publish 8080:8080 --name nestor ibmcom/nestor:0.0.1`

Verify the application runs by execution `curl localhost:8080/actuator/health`

Once done, you can remove the container if you wish `docker rm --force nestor`
