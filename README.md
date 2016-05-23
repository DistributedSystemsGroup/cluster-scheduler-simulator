# Cluster scheduler simulator overview

This simulator is forked from Cluster-Scheduler-Simulator by [Google](https://github.com/google/cluster-scheduler-simulator) and contains the features introduces by [Liang-Chi Hsieh](https://github.com/viirya) in his [fork](https://github.com/viirya/cluster-scheduler-simulator).

This version of the simulator has gone trough a lot of fixes, refactoring and improvements. The original document can be located [here](README_GOOGLE.md). We recommend to read it before using this version.

## Downloading, building, and running

To download this project you can clone this Git repository. Once cloned there are several ways to run it:

1. follow instructions in the same section [here](README_GOOGLE.md)

2. if you have a Swarm or Docker deployment at hand, it is possible to launch it with the following command:


        $ bash bin/launch_on_container.sh


If you use option 2. make sure to update the variables inside the above script and to deploy the Docker Image [here](docker).

## Simulators implemented

Currently the scheduler architectures supported are:

1. Monolithic by [Google](https://github.com/google/cluster-scheduler-simulator/blob/master/src/main/scala/MonolithicSimulation.scala)
2. Mesos by [Google](https://github.com/google/cluster-scheduler-simulator/blob/master/src/main/scala/MesosSimulation.scala)
3. Omega by [Google](https://github.com/google/cluster-scheduler-simulator/blob/master/src/main/scala/OmegaSimulation.scala)
4. Spark by [Liang-Chi Hsieh](https://github.com/viirya/cluster-scheduler-simulator/blob/spark_support/src/main/scala/SparkSimulation.scala)

Be aware that the scheduler simulators original code might be different after the refactoring and improvements, but the logic is the same.

## Metrics recorded

Below there is a table that summarize the metric used for each scheduler simulator.

|                                   | Monolithic  |  Mesos  | Omega  | Spark  |
|:-:|:-:|:-:|:-:|:-:|:-:|
| Avg Cell CPU Utilization          | &#10004;  | &#10004;  | &#10004;  |   |
| Avg Cell RAM Utilization          | &#10004;  | &#10004;  | &#10004;  |   |
| Avg Cell CPU Locked               | &#10004;  | &#10004;  | &#10004;  |   |
| Avg Cell RAM Locked               | &#10004;  | &#10004;  | &#10004;  |   |
| Busy Time Fraction                | &#10004;  | &#10004;  | &#10004;  |   |
| Conflict Fraction                 |           |           | &#10004;  |   |
| Daily Busy Fraction Median        | &#10004;  | &#10004;  | &#10004;  |   |
| Daily Conflict Fraction Median    |           |           | &#10004;  |   |
| Failed Task Allocation            | &#10004;  | &#10004;  | &#10004;  |   |
| Jobs Unscheduled                  | &#10004;  | &#10004;  | &#10004;  |   |
| Number Jobs Timed Out             | &#10004;  | &#10004;  | &#10004;  |   |
| Number Pending Jobs at the End    | &#10004;  | &#10004;  | &#10004;  |   |
| Retried Transactions              |           |           | &#10004;  |   |
| Task Conflict Fraction            |           |           | &#10004;  |   |
| Wait Time Before First Task       | &#10004;  | &#10004;  | &#10004;  |   |
| Wait Time Before All Tasks        | &#10004;  | &#10004;  | &#10004;  |   |
