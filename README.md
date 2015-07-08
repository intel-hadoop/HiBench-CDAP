# HiBench-CDAP
[HiBench](https://github.com/intel-hadoop/HiBench) benchmark suite migration to CDAP.

##Getting Started

### Building HiBench-CDAP
HiBench-CDAP is built with [Apache Maven](http://maven.apache.org/).
To build the workload application, go to the root directory of a certain application and run:

> mvn clean package


### Deploy an application
Go to your CDAP Web UI and add an application with the .jar you built.

### Start HiBench-CDAP Web UI
1. Start all services of the application you added in the CDAP Web UI.

2. Open the BenchUI service with url:

   > http://[host]:10000/v3/namespaces/[namespace]/apps/[app]/services/BenchUI/methods/UI

* A typical setup would be
  * host: localhost
  * namespace: default
  * app: "WordCount" or "DFSIO"

### Run the Application

Example: Word Count

1.	Set the workload size and click “generate”

2.	When complete, select the processing engine and input the number of iterations, click “execute”
 * Currently only Hadoop MR is available

3.	Wait to get your benchmark result.

4.	Download the input text file or count result file by clicking button ![save button](/resources/save.png)

![example page](/resources/wordCountPage.PNG)

## Development Environment

### SDK

Java: 1.7.0_79

CDAP SDK: 2.8.0

### Operating System:

Development Environment: Windows 7

CDAP Enviroment: Ubuntu 14.04 

### Developing Tool:

Application Development: Intellij IDEA 14.1.4

Web UI Development: WebStorm 10.0.0
