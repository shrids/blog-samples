# Steps to run the Sample.
### 1. Start Pravega.
There are multiple ways to run Pravega and it is documented at  [Running Pravega](https://github.com/pravega/pravega/blob/master/documentation/src/docs/deployment/deployment.md).
Below are the steps quickly spin up Pravega as a single process and in-memory.

In standalone mode, the Pravega server is accessible from clients through the `localhost` interface only. 
Controller REST APIs, however, are accessible from remote hosts/machines.

You can launch a standalone mode server using the following options:

1. From [source code](#from-source-code)
2. From [installation package](#from-installation-package)
3. From [Docker image](#from-docker-image)

#### From Source Code

Checkout the source code:

```
$ git clone https://github.com/pravega/pravega.git
$ cd pravega
```

Build the Pravega standalone mode distribution:

```
./gradlew startStandalone
```

#### From Installation Package

Download the Pravega release from the [GitHub Releases](https://github.com/pravega/pravega/releases).

```
$ tar xfvz pravega-<version>.tgz
```
Download and extract either tarball or zip files. Follow the instructions provided for the tar files (same can be applied for zip file) to launch all the components of Pravega on your local machine.

Run Pravega Standalone:

```
$ pravega-<version>/bin/pravega-standalone
```

#### From Docker Image

The below command will download and run Pravega from the container image on docker hub.

**Note:** We must replace the `<ip>` with the IP of our machine to connect to Pravega from our local machine. Optionally we can replace `latest` with the version of Pravega as per the requirement.

```
docker run -it -e HOST_IP=<ip> -p 9090:9090 -p 12345:12345 pravega/pravega:latest standalone
```

###2. Run the sample 
The below command can be used to run the sample via command line.
```
./gradlew run
```