/**
  * Copyright (c) 2013, Regents of the University of California
  * All rights reserved.
  *
  * Redistribution and use in source and binary forms, with or without
  * modification, are permitted provided that the following conditions are met:
  *
  * Redistributions of source code must retain the above copyright notice, this
  * list of conditions and the following disclaimer.  Redistributions in binary
  * form must reproduce the above copyright notice, this list of conditions and the
  * following disclaimer in the documentation and/or other materials provided with
  * the distribution.  Neither the name of the University of California, Berkeley
  * nor the names of its contributors may be used to endorse or promote products
  * derived from this software without specific prior written permission.  THIS
  * SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
  * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
  * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
  * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
  * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
  * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
  * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
  * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
  */

package ClusterSchedulingSimulation

import java.io._
import java.util.concurrent.Callable

import ClusterSchedulingSimulation.ClusterSimulationProtos.ExperimentResultSet.ExperimentEnv
import ClusterSchedulingSimulation.ClusterSimulationProtos.ExperimentResultSet.ExperimentEnv.ExperimentResult
import ClusterSchedulingSimulation.ClusterSimulationProtos._
import org.apache.log4j.Logger

import scala.collection.mutable.ListBuffer

/**
  * An experiment represents a series of runs of a simulator,
  * across ranges of parameters. Exactly one of {L, C, Lambda}
  * can be swept over per experiment, i.e. only one of
  * avgJobInterarrivalTimeRange, constantThinkTimeRange, and
  * perTaskThinkTimeRange can have size greater than one in a
  * single Experiment instance.
  */
class Experiment(
                  name: String,
                  // Workloads setup.
                  workloadToSweepOver: String,
                  avgJobInterarrivalTimeRange: Option[Seq[Double]] = None,
                  workloadDescs: Seq[WorkloadDesc],
                  // Schedulers setup.
                  schedulerWorkloadsToSweepOver: Map[String, Seq[String]],
                  constantThinkTimeRange: Seq[Double],
                  perTaskThinkTimeRange: Seq[Double],
                  blackListPercentRange: Seq[Double],
                  // Workload -> scheduler mapping setup.
                  schedulerWorkloadMap: Map[String, Seq[String]],
                  // Simulator setup.
                  simulatorDesc: ClusterSimulatorDesc,
                  logging: Boolean = false,
                  outputDirectory: String = "experiment_results",
                  // Map from workloadName -> max % of cellState this prefill workload
                  // can account for. Any prefill workload generator with workloadName
                  // that is not contained in any of these maps will have no prefill
                  // generated for this experiment, and any with name that is in multiple
                  // of these maps will use the first limit that actually kicks in.
                  prefillCpuLimits: Map[String, Double] = Map(),
                  prefillMemLimits: Map[String, Double] = Map(),
                  // Default simulations to 10 minute timeout.
                  simulationTimeout: Double = 60.0*10.0) extends Runnable {
  val logger = Logger.getLogger(this.getClass.getName)

  prefillCpuLimits.values.foreach(l => assert(l >= 0.0 && l <= 1.0))
  prefillMemLimits.values.foreach(l => assert(l >= 0.0 && l <= 1.0))

  //  var parametersSweepingOver = 0
  //  avgJobInterarrivalTimeRange.foreach{opt: Seq[Double] => {
  //    if (opt.length > 1) {
  //      parametersSweepingOver += 1
  //    }
  //  }}
  //  if (constantThinkTimeRange.length > 1) {parametersSweepingOver += 1}
  //  if (perTaskThinkTimeRange.length > 1) {parametersSweepingOver += 1}
  //  assert(parametersSweepingOver <= 1)

  override
  def toString = name

  def run() {
    // Create the output directory if it doesn't exist.
    new File(outputDirectory).mkdirs()
    val output =
      new java.io.FileOutputStream("%s/%s-%.0f.protobuf"
        .format(outputDirectory,
          name,
          simulatorDesc.runTime))

    var allRuns: ListBuffer[ExperimentRun] = ListBuffer()
    val allExperimentEnv = collection.mutable.HashMap[Int, ExperimentEnv.Builder]()
    val allExperimentResult = collection.mutable.HashMap[Int, Array[ExperimentResult]]()
    val experimentResultSet = ExperimentResultSet.newBuilder()


    logger.debug("Running Experiment %s with RunTime: %.2fs and Timeout: %.2fs".format(name, simulatorDesc.runTime, simulationTimeout))
    // Parameter sweep over workloadDescs
    var workloadDescId = 0
    var run_id = 0
    workloadDescs.foreach(workloadDesc => {
      logger.debug("Set workloadDesc = %s %s".format(workloadDesc.cell, workloadDesc.assignmentPolicy))

      // Save Experiment level stats into protobuf results.
      val experimentEnv = ExperimentResultSet.ExperimentEnv.newBuilder()
      experimentEnv.setCellName(workloadDesc.cell)
      experimentEnv.setWorkloadSplitType(workloadDesc.assignmentPolicy)
      experimentEnv.setIsPrefilled(
        workloadDesc.prefillWorkloadGenerators.nonEmpty)
      experimentEnv.setRunTime(simulatorDesc.runTime)
      allExperimentEnv.put(workloadDescId, experimentEnv)

      // Generate preFill workloads. The simulator doesn't modify
      // these workloads like it does the workloads that are played during
      // the simulation.
      var prefillWorkloads = List[Workload]()
      workloadDesc.prefillWorkloadGenerators
        .filter(wlGen => {
          prefillCpuLimits.contains(wlGen.workloadName) ||
            prefillMemLimits.contains(wlGen.workloadName)
        }).foreach(wlGen => {
        val cpusMaxOpt = prefillCpuLimits.get(wlGen.workloadName).map(i => {
          i * workloadDesc.cellStateDesc.numMachines *
            workloadDesc.cellStateDesc.cpusPerMachine
        })

        val memMaxOpt = prefillMemLimits.get(wlGen.workloadName).map(i => {
          i * workloadDesc.cellStateDesc.numMachines *
            workloadDesc.cellStateDesc.memPerMachine
        })
        logger.debug(("Creating a new prefill workload from " +
          "%s with maxCPU %s and maxMem %s")
          .format(wlGen.workloadName, cpusMaxOpt, memMaxOpt))
        val newWorkload = wlGen.newWorkload(simulatorDesc.runTime,
          maxCpus = cpusMaxOpt,
          maxMem = memMaxOpt)
        for(job <- newWorkload.getJobs) {
          assert(job.submitted == 0.0)
        }
        prefillWorkloads ::= newWorkload
      })

      // Parameter sweep over lambda.
      // If we have a range for lambda, loop over it, else
      // we just loop over a list holding a single element: None
      val jobInterarrivalRange = avgJobInterarrivalTimeRange match {
        case Some(paramsRange) => paramsRange.map(Some(_))
        case None => List(None)
      }

      logger.debug("Set up avgJobInterarrivalTimeRange: %s".format(jobInterarrivalRange))
      jobInterarrivalRange.foreach(avgJobInterarrivalTime => {
        if (avgJobInterarrivalTime.isEmpty) {
          logger.debug("Since we're not in a lambda sweep, not overwriting lambda.")
        } else {
          logger.debug("Curr avgJobInterarrivalTime: %s".format(avgJobInterarrivalTime))
        }

        // Set up a list of workloads
        val commonWorkloadSet = ListBuffer[Workload]()
        var newAvgJobInterarrivalTime: Option[Double] = None
        workloadDesc.workloadGenerators.foreach(workloadGenerator => {
          if (workloadToSweepOver.equals(
            workloadGenerator.workloadName)) {
            // Only update the workload interarrival time if this is the
            // workload we are supposed to sweep over. If this is not a
            // lambda parameter sweep then updatedAvgJobInterarrivalTime
            // will remain None after this line is executed.
            newAvgJobInterarrivalTime = avgJobInterarrivalTime
          }
          logger.debug("Generating new Workload %s for window %f seconds long."
            .format(workloadGenerator.workloadName, simulatorDesc.runTime))
          val newWorkload =
            workloadGenerator
              .newWorkload(timeWindow = simulatorDesc.runTime,
                updatedAvgJobInterarrivalTime = newAvgJobInterarrivalTime)
          commonWorkloadSet.append(newWorkload)
        })

        // Parameter sweep over L.
        perTaskThinkTimeRange.foreach(perTaskThinkTime => {
          logger.debug("Set perTaskThinkTime = %f".format(perTaskThinkTime))

          // Parameter sweep over C.
          constantThinkTimeRange.foreach(constantThinkTime => {
            logger.debug("Set constantThinkTime = %f".format(constantThinkTime))

            // Parameter sweep over BlackListPercent (of cellstate).
            blackListPercentRange.foreach(blackListPercent => {
              logger.debug("Set blackListPercent = %f".format(blackListPercent))

              val copyOfPrefillWorkloads = ListBuffer[Workload]()
              prefillWorkloads.foreach(prefillWorkload => {
                copyOfPrefillWorkloads.append(prefillWorkload.copy)
              })

              // Make a copy of the workloads that this run of the simulator
              // will modify by using them to track statistics.
              val workloads = ListBuffer[Workload]()
              commonWorkloadSet.foreach(workload => {
                workloads.append(workload.copy)
              })

              logger.debug("Setting up run %d".format(run_id + 1))
              allRuns += new ExperimentRun(
                run_id,
                name = new String(name),
                workloadToSweepOver = new String(workloadToSweepOver),
                avgJobInterarrivalTime = avgJobInterarrivalTime,
                workloadDescId = workloadDescId,
                schedulerWorkloadsToSweepOver = Map[String, Seq[String]]() ++ schedulerWorkloadsToSweepOver,
                constantThinkTime = constantThinkTime,
                perTaskThinkTime = perTaskThinkTime,
                simulatorRunTime = simulatorDesc.runTime,
                simulationTimeout = simulationTimeout,
                workloads =  workloads,
                simulator = simulatorDesc.newSimulator(constantThinkTime,
                  perTaskThinkTime,
                  blackListPercent,
                  Map[String, Seq[String]]() ++ schedulerWorkloadsToSweepOver,
                  Map[String, Seq[String]]() ++ schedulerWorkloadMap,
                  workloadDesc.cellStateDesc,
                  workloads,
                  copyOfPrefillWorkloads,
                  logging)
              )
              run_id += 1
            }) // blackListPercent
          }) // C
        }) // L
      }) // lambda
      workloadDescId += 1
    }) // WorkloadDescs
    val numTotalRun = allRuns.length
    var numFinishedRuns = 0
    allExperimentEnv.foreach{ case (id, experimentEnv) =>
      allExperimentResult.put(id, new Array[ExperimentResult](numTotalRun))
    }

    /**
      * Start the runs we've set up.
      */
    val numThreads = Math.min(numTotalRun, Runtime.getRuntime.availableProcessors() - 1)
    val threadSleep = 5
    val pool = java.util
      .concurrent
      .Executors
      .newFixedThreadPool(numThreads)
    var futures = ListBuffer[java.util.concurrent.Future[(Int, Int, ExperimentResult)]]()
    logger.info(("Starting %d runs with the following options:\n" +
      "\t - threads:     %d\n").format(numTotalRun, numThreads))
    val startTime = System.currentTimeMillis()
    allRuns.foreach(run => {
      futures += pool.submit(run)
    })
    // Let go of pointers to Run because each Run will use quite a lot of memory.
    allRuns = ListBuffer()
    pool.shutdown()
    while(futures.nonEmpty) {
      Thread.sleep(threadSleep * 1000)
      val (completed, running) = futures.partition(_.isDone)
      logger.debug("futures: %d, completed: %d, running: %d".format(futures.length, completed.length, running.length))
      if (completed.nonEmpty) {
        val elapsedTime: Double = (System.currentTimeMillis() - startTime) / 1000.0
        numFinishedRuns += completed.length
        // Let's calculate the estimated time left for this experiment
        val etl = (elapsedTime/numFinishedRuns) * ((numTotalRun - numFinishedRuns) / numThreads.toDouble)
        logger.info("%d more runs just finished running. In total, %d of %d have finished. ETL: %.2fs (%.2fm)"
          .format(completed.length, numFinishedRuns, numTotalRun, etl, etl / 60.0))
        completed.foreach(x => {
          val callableResult = x.get()
          val workloadDescId = callableResult._1
          if (workloadDescId != -1){
            // We have to use another structure because insert an experimentResult
            // in a specific position is not working with the protobuf
            // (an IndexOutOfBoundException is triggered)
            val experimentEnv = allExperimentResult.get(workloadDescId).get
            experimentEnv(callableResult._2) =  callableResult._3
          }
        })
      }
      futures = running
    }
    logger.info("Done all (%d) runs".format(numTotalRun))
    allExperimentEnv.foreach{ case (id, experimentEnv) =>
      val experimentResults = allExperimentResult.get(id).get
      experimentResults.foreach(experimentResult => {
        experimentEnv.addExperimentResult(experimentResult)
      })
      logger.debug("Workload %d has %d experimentResults.".format(id, experimentEnv.getExperimentResultCount))
      experimentResultSet.addExperimentEnv(id, experimentEnv)
    }
    experimentResultSet.build().writeTo(output)
    output.close()
  }
}

class ExperimentRun(
                     id: Int,
                     name: String,
                     // Workloads setup.
                     workloadToSweepOver: String,
                     avgJobInterarrivalTime: Option[Double],
                     workloadDescId: Int,
                     // Schedulers setup.
                     schedulerWorkloadsToSweepOver: Map[String, Seq[String]],
                     constantThinkTime: Double,
                     perTaskThinkTime: Double,
                     // Simulator setup.
                     simulatorRunTime: Double,
                     // Default simulations to 10 minute timeout.
                     simulationTimeout: Double = 60.0*10.0,
                     workloads: ListBuffer[Workload],
                     simulator: ClusterSimulator
                   ) extends Callable[(Int, Int, ExperimentResult)]{
  val logger = Logger.getLogger(this.getClass.getName)

  def recordStats( simulator: ClusterSimulator,
                   workloads: Seq[Workload],
                   avgJobInterarrivalTime: Option[Double],
                   constantThinkTime: Double,
                   perTaskThinkTime: Double,
                   simulatorRunTime: Double,
                   workloadToSweepOver: String,
                   schedulerWorkloadsToSweepOver: Map[String, Seq[String]]): ExperimentResult ={
    /**
      * Capture statistics into a protocolbuffer.
      */
    val experimentResult =
      ExperimentResultSet.ExperimentEnv.ExperimentResult.newBuilder()

    experimentResult.setCellStateAvgCpuUtilization(
      simulator.avgCpuUtilization) // / simulator.cellState.totalCpus)
    experimentResult.setCellStateAvgMemUtilization(
      simulator.avgMemUtilization)// / simulator.cellState.totalMem)

    experimentResult.setCellStateAvgCpuLocked(
      simulator.avgCpuLocked / simulator.cellState.totalCpus)
    experimentResult.setCellStateAvgMemLocked(
      simulator.avgMemLocked / simulator.cellState.totalMem)

    // Save repeated stats about workloads.
    workloads.foreach(workload => {
      val workloadStats = ExperimentResultSet.
        ExperimentEnv.
        ExperimentResult.
        WorkloadStats.newBuilder()
      workloadStats.setWorkloadName(workload.name)
      workloadStats.setNumJobs(workload.numJobs)
      workloadStats.setNumJobsScheduled(
        workload.getJobs.count(_.numSchedulingAttempts > 0))
      workloadStats.setJobThinkTimes90Percentile(
        workload.jobUsefulThinkTimesPercentile(0.9))
      workloadStats.setAvgJobQueueTimesTillFirstScheduled(
        workload.avgJobQueueTimeTillFirstScheduled)
      workloadStats.setAvgJobQueueTimesTillFullyScheduled(
        workload.avgJobQueueTimeTillFullyScheduled)
      workloadStats.setJobQueueTimeTillFirstScheduled90Percentile(
        workload.jobQueueTimeTillFirstScheduledPercentile(0.9))
      workloadStats.setJobQueueTimeTillFullyScheduled90Percentile(
        workload.jobQueueTimeTillFullyScheduledPercentile(0.9))
      workloadStats.setNumSchedulingAttempts90Percentile(
        workload.numSchedulingAttemptsPercentile(0.9))
      workloadStats.setNumSchedulingAttempts99Percentile(
        workload.numSchedulingAttemptsPercentile(0.99))
      workloadStats.setNumTaskSchedulingAttempts90Percentile(
        workload.numTaskSchedulingAttemptsPercentile(0.9))
      workloadStats.setNumTaskSchedulingAttempts99Percentile(
        workload.numTaskSchedulingAttemptsPercentile(0.99))

      experimentResult.addWorkloadStats(workloadStats)
    })
    // Record workload specific details about the parameter sweeps.
    experimentResult.setSweepWorkload(workloadToSweepOver)
    experimentResult.setAvgJobInterarrivalTime(
      avgJobInterarrivalTime.getOrElse(
        workloads.filter(_.name == workloadToSweepOver)
          .head.avgJobInterarrivalTime))

    // Save repeated stats about schedulers.
    simulator.schedulers.values.foreach(scheduler => {
      val schedulerStats =
        ExperimentResultSet.
          ExperimentEnv.
          ExperimentResult.
          SchedulerStats.newBuilder()
      schedulerStats.setSchedulerName(scheduler.name)
      schedulerStats.setUsefulBusyTime(
        scheduler.totalUsefulTimeScheduling)
      schedulerStats.setWastedBusyTime(
        scheduler.totalWastedTimeScheduling)
      // Per scheduler metrics bucketed by day.
      // Use floor since days are zero-indexed. For example, if the
      // simulator only runs for 1/2 day, we should only have one
      // bucket (day 0), so our range should be 0 to 0. In this example
      // we would get floor(runTime / 86400) = floor(0.5) = 0.
      val daysRan = math.floor(simulatorRunTime/86400.0).toInt
      logger.debug("Computing daily stats for days 0 through %d."
        .format(daysRan))
      (0 to daysRan).foreach {
        day: Int => {
          val perDayStats =
            ExperimentResultSet.
              ExperimentEnv.
              ExperimentResult.
              SchedulerStats.
              PerDayStats.newBuilder()
          perDayStats.setDayNum(day)
          // Busy and wasted time bucketed by day.
          perDayStats.setUsefulBusyTime(
            scheduler.dailyUsefulTimeScheduling.getOrElse(day, 0.0))
          logger.debug(("Writing dailyUsefulScheduling(day = %d) = %f for " +
            "scheduler %s")
            .format(day,
              scheduler
                .dailyUsefulTimeScheduling
                .getOrElse(day, 0.0),
              scheduler.name))
          perDayStats.setWastedBusyTime(
            scheduler.dailyWastedTimeScheduling.getOrElse(day, 0.0))
          // Counters bucketed by day.
          perDayStats.setNumSuccessfulTransactions(
            scheduler.dailySuccessTransactions.getOrElse[Int](day, 0))
          perDayStats.setNumFailedTransactions(
            scheduler.dailyFailedTransactions.getOrElse[Int](day, 0))

          schedulerStats.addPerDayStats(perDayStats)
        }}

      assert(scheduler.perWorkloadUsefulTimeScheduling.size ==
        scheduler.perWorkloadWastedTimeScheduling.size,
        "the maps held by Scheduler to track per workload " +
          "useful and wasted time should be the same size " +
          "(Scheduler.addJob() should ensure this).")
      scheduler.perWorkloadUsefulTimeScheduling.foreach{
        case (workloadName, workloadUsefulBusyTime) =>
          val perWorkloadBusyTime =
            ExperimentResultSet.
              ExperimentEnv.
              ExperimentResult.
              SchedulerStats.
              PerWorkloadBusyTime.newBuilder()
          perWorkloadBusyTime.setWorkloadName(workloadName)
          perWorkloadBusyTime.setUsefulBusyTime(workloadUsefulBusyTime)
          perWorkloadBusyTime.setWastedBusyTime(
            scheduler.perWorkloadWastedTimeScheduling(workloadName))

          schedulerStats.addPerWorkloadBusyTime(perWorkloadBusyTime)
      }
      // Counts of sched-level job transaction successes, failures,
      // and retries.
      schedulerStats.setNumSuccessfulTransactions(
        scheduler.numSuccessfulTransactions)
      schedulerStats.setNumFailedTransactions(
        scheduler.numFailedTransactions)
      schedulerStats.setNumNoResourcesFoundSchedulingAttempts(
        scheduler.numNoResourcesFoundSchedulingAttempts)
      schedulerStats.setNumRetriedTransactions(
        scheduler.numRetriedTransactions)
      schedulerStats.setNumJobsTimedOutScheduling(
        scheduler.numJobsTimedOutScheduling)
      // Counts of task transaction successes and failures.
      schedulerStats.setNumSuccessfulTaskTransactions(
        scheduler.numSuccessfulTaskTransactions)
      schedulerStats.setNumFailedTaskTransactions(
        scheduler.numFailedTaskTransactions)

      schedulerStats.setIsMultiPath(scheduler.isMultiPath)
      schedulerStats.setNumJobsLeftInQueue(scheduler.jobQueueSize)
      schedulerStats.setFailedFindVictimAttempts(
        scheduler.failedFindVictimAttempts)

      experimentResult.addSchedulerStats(schedulerStats)
    })
    // Record scheduler specific details about the parameter sweeps.
    schedulerWorkloadsToSweepOver
      .foreach{case (schedName, workloadNames1) =>
        workloadNames1.foreach(workloadName => {
          val schedulerWorkload =
            ExperimentResultSet.
              ExperimentEnv.
              ExperimentResult.
              SchedulerWorkload.newBuilder()
          schedulerWorkload.setSchedulerName(schedName)
          schedulerWorkload.setWorkloadName(workloadName)
          experimentResult.addSweepSchedulerWorkload(schedulerWorkload)
        })
      }

    experimentResult.setConstantThinkTime(constantThinkTime)
    experimentResult.setPerTaskThinkTime(perTaskThinkTime)
    experimentResult.build()
    //    /**
    //      * TODO(andyk): Once protocol buffer support is finished,
    //      *              remove this.
    //      */
    //    val numSchedulingAttemptsMax =
    //      sortedWorkloads.map(workload => {
    //        workload.getJobs.map(_.numSchedulingAttempts).max
    //      }).mkString(" ")
    //
    //    val numTaskSchedulingAttemptsMax =
    //      sortedWorkloads.map(workload => {
    //        workload.getJobs.map(_.numTaskSchedulingAttempts).max
    //      }).mkString(" ")
  }

  def call(): (Int, Int, ExperimentResult) = {
    logger.info("Starting %s - %d".format(name, id + 1))

    val startTime = System.currentTimeMillis()
    val success: Boolean = simulator.run(Some(simulatorRunTime),
      Some(simulationTimeout))
    if (success) {
      // Simulation did not time out, so record stats.
      // Save our results as a protocol buffer.
      val experimentResult = recordStats(
        simulator = simulator,
        workloads = workloads,
        avgJobInterarrivalTime = avgJobInterarrivalTime,
        constantThinkTime = constantThinkTime,
        perTaskThinkTime = perTaskThinkTime,
        simulatorRunTime = simulatorRunTime,
        workloadToSweepOver = workloadToSweepOver,
        schedulerWorkloadsToSweepOver = schedulerWorkloadsToSweepOver
      )
      logger.info("Done %s - %d. Real RunTime: %.2fs"
        .format(name, id + 1, (System.currentTimeMillis() - startTime) / 1000.0))
      (workloadDescId, id, experimentResult)
    } else {
      logger.info("Done %s - %d. Sim timed out."
        .format(name, id))
      (-1, -1, null)
    }
  }
}
