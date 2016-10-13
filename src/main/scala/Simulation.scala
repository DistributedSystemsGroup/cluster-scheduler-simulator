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

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.channels.FileChannel

import ClusterSchedulingSimulation._
import ClusterSchedulingSimulation.Workloads._
import schedulers.{ZoePreemptionSimulatorDesc, _}
import ca.zmatrix.utils._
import org.apache.log4j.Logger

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object Simulation{
  val logger = Logger.getLogger(this.getClass.getName)
  def main(args: Array[String]) {
    val helpString = "Usage: bin/sbt run [--thread-pool-size INT_NUM_THREADS] [--random-seed INT_SEED_VALUE]"
    if (args.length > 0) {
      if (args.head.equals("--help") || args.head.equals("-h")) {
        println(helpString)
        System.exit(0)
      }
    }
    val availableProcessors = Runtime.getRuntime.availableProcessors()
    val pp = new ParseParms(helpString)
    pp.parm("--thread-pool-size", availableProcessors.toString).rex("^\\d*") // optional_arg
    //    pp.parm("--random-seed", "%d".format(util.Random.nextLong)).rex("^\\d*") // optional_arg
    pp.parm("--random-seed", "0").rex("^\\d*") // optional_arg

    var inputArgs = Map[String, String]()
    val result = pp.validate(args.toList)
    if(!result._1) {
      println(result._2)
      sys.error("Exiting due to invalid input.")
    } else {
      inputArgs = result._3
    }

    var numThreads = Math.min(inputArgs("--thread-pool-size").toInt, availableProcessors)
    val randomSeed:Long = inputArgs("--random-seed").toLong

    logger.info("RUNNING CLUSTER SIMULATOR EXPERIMENTS")


    /**
      * Start Config Variables
      */
    val runMonolithic = false
    val runSpark = false
    val runNewSpark = false
    val runMesos = false
    val runOmega = false
    val runZoe = true
    val runZoePreemption = false

    val globalRunTime = 86400.0 * 120 //86400.0 // 1 Day
    val threadSleep = 5

    /**
      * Set up parameter sweeps.
      */
    //
    // Full ConstantRange is 91 values.
    val fullConstantRange: List[Double] = (0.001 to 0.01 by 0.0005).toList :::
      (0.015 to 0.1 by 0.005).toList :::
      (0.15 to 1.0 by 0.05).toList :::
      (1.5 to 10.0 by 0.5).toList :::
      (15.0 to 100.0 by 5.0).toList // :::
    // (150.0 to 1000.0 by 50.0).toList

    // Full PerTaskRange is 55 values.
    val fullPerTaskRange: List[Double] = (0.001 to 0.01 by 0.0005).toList :::
      (0.015 to 0.1 by 0.005).toList :::
      (0.15 to 1.0 by 0.05).toList // :::
    // (1.5 to 10 by 0.5).toList

    // Full lambda is 20 values.
    val fullLambdaRange: List[Double] = (0.01 to 0.11 by 0.01).toList :::
      (0.15 to 1.0 by 0.1).toList // :::
    // (1.5 to 10.0 by 1.0).toList

    val fullPickinessRange: List[Double] = (0.00 to 0.75 by 0.05).toList
//
//
//    val medConstantRange: List[Double] = 0.01 :: 0.05 :: 0.1 :: 0.5 ::
//                                         1.0 :: 5.0 :: 10.0:: 50.0 ::
//                                         100.0 :: Nil
//    val medPerTaskRange: List[Double] = 0.001 :: 0.005 :: 0.01 :: 0.05 ::
//                                        0.1 :: 0.5 :: 1.0 :: Nil
//
//    val medLambdaRange: List[Double] = 0.01 :: 0.05 :: 0.1 :: 0.5 :: Nil
//
//    val smallConstantRange: List[Double] = (0.1 to 100.0 by 99.9).toList
//    val smallPerTaskRange: List[Double] = (0.1 to 1.0 by 0.9).toList
//    val smallLambdaRange: List[Double] = (0.001 to 10.0 by 9.999).toList

//    val constantRange = 0.1 :: 1.0 :: 10.0 :: Nil
    // val constantRange = medConstantRange
    val constantRange = fullConstantRange

    //    val perTaskRange = 0.005 :: Nil
    // val perTaskRange = medPerTaskRange
    val perTaskRange = fullPerTaskRange

    val pickinessRange = fullPickinessRange
//    val lambdaRange = fullLambdaRange
    val lambdaRange = 1.0 :: Nil
//    val lambdaRange = 0.01 :: 0.02 :: Nil

//    val interArrivalScaleRange = 0.009 :: 0.01 :: 0.02 :: 0.1 :: 0.2 :: 1.0 :: Nil
    val interArrivalScaleRange = lambdaRange.map(1/_)

    val prefillCpuLim = Map("PrefillBatchService" -> 0.6)

    val doLogging = false

    val timeout = None // In seconds.

    val sweepC = false
    val sweepL = false
    val sweepCL = false
    val sweepPickiness = false
    val sweepLambda = true

    val allocationModes = List[AllocationModes.Value](AllocationModes.All) //, AllocationModes.Incremental)
    val policyModes = List[PolicyModes.Value](
//      PolicyModes.Fifo,
//      PolicyModes.MyFifo,
//      PolicyModes.PriorityFifo,
      PolicyModes.SJF,
      PolicyModes.MySJF,
//      PolicyModes.LJF,
//      PolicyModes.HRRN,
      PolicyModes.SRPT,
      PolicyModes.MySRPT,
      PolicyModes.Size,
      PolicyModes.MySize//,
//      PolicyModes.MySizeError,
//      PolicyModes.MySize2,
//      PolicyModes.MySize3,
//      PolicyModes.MySize4,
//      PolicyModes.MySize5,
//      PolicyModes.MySize6,
//      PolicyModes.MySize7,
//      PolicyModes.MySize8
    )

    val formatter = new java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
    /**
      * End Config Variables
      */

    var sweepDimensions = collection.mutable.ListBuffer[String]()
    if (sweepC)
      sweepDimensions += "C"
    if (sweepL)
      sweepDimensions += "L"
    if (sweepCL)
      sweepDimensions += "CL"
    if (sweepPickiness)
      sweepDimensions += "Pickiness"
    if (sweepLambda)
      sweepDimensions += "Lambda"

    val dateTimeStamp = formatter.format(new java.util.Date)


    /**
      * Choose which "experiment environments" (i.e. WorkloadDescs)
      * we want to use.
      */
    logger.info("Loading Traces...")
    var allWorkloadDescs = List[WorkloadDesc]()
    // allWorkloadDescs ::= exampleWorkloadDesc

    // allWorkloadDescs ::= exampleWorkloadPrefillDesc

    // Prefills jobs based on prefill trace, draws job and task stats from
    // exponential distributions.
    // allWorkloadDescs ::= exampleInterarrivalTimeTraceWorkloadPrefillDesc

    // Prefills jobs based on prefill trace. Loads Job stats (interarrival
    // time, num tasks, duration) from traces, and task stats from
    // exponential distributions.
    // allWorkloadDescs ::= exampleTraceWorkloadPrefillDesc

    // Prefills jobs based on prefill trace. Loads Job stats (interarrival
    // time, num tasks, duration) and task stats (cpusPerTask, memPerTask)
    // from traces.
    allWorkloadDescs ::= eurecomCellTraceAllWorkloadPrefillDesc
//    allWorkloadDescs ::= tenEurecomCellTraceAllWorkloadPrefillDesc
//    allWorkloadDescs ::= fiveEurecomCellTraceAllWorkloadPrefillDesc

    var allExperiments: List[Experiment] = List()
    val wlDescs = allWorkloadDescs
    // Make the experiment_results dir if it doesn't exist
    val experDir = new java.io.File("experiment_results")
    if (!experDir.exists) {
      logger.info("Creating the 'experiment_results' dir.")
      experDir.mkdir()
    }
    val outputDirName = "%s/%s-%s-%s-%.0f"
      .format(
        experDir.toString,
        dateTimeStamp,
        "vary_" + sweepDimensions.mkString("_"),
        wlDescs.map(value => {
          value.cell + value.assignmentPolicy +
                (if (value.prefillWorkloadGenerators.nonEmpty) {
                  "_prefilled"
                } else {""})
        }).mkString("_"),
        globalRunTime)
    logger.info("outputDirName is %s".format(outputDirName))


    logger.info("Setting up Schedulers...")
    // Monolithic
    if(runMonolithic){
      logger.info("\tMonolithic")
      /**
        * Set up SchedulerDesc-s.
        */
      val monolithicSchedulerDesc = new SchedulerDesc(
        name = "Monolithic".intern(),
        constantThinkTimes = Map("Batch" -> 0.01, "Service" -> 0.01),
        perTaskThinkTimes = Map("Batch" -> 0.005, "Service" -> 0.01))

      /**
        * Set up workload-to-scheduler mappings.
        */
      val monolithicSchedulerWorkloadMap =
        Map[String, Seq[String]]("Batch" -> Seq("Monolithic"),
          "Service" -> Seq("Monolithic"))

      /**
        * Set up a simulatorDesc-s.
        */
      val monolithicSimulatorDescs: ListBuffer[MonolithicSimulatorDesc] = ListBuffer[MonolithicSimulatorDesc]()
      allocationModes.foreach(allocationMode =>{
        monolithicSimulatorDescs += new MonolithicSimulatorDesc(Array(monolithicSchedulerDesc), globalRunTime, allocationMode)
      })

      /**
        * Set up a run of experiments.
        */
      // Loop over both a single and multi path Monolithic scheduler.
      // Emulate a single path scheduler by making the parameter sweep
      // apply to both the "Service" and "Batch" workload types for it.
      val singlePathSetup = ("single", Map("Monolithic" -> List("Service")))
      val multiPathSetup =
        ("multi", Map("Monolithic" -> List("Service", "Batch")))
      List(singlePathSetup, multiPathSetup).foreach {
        case (multiOrSingle, schedulerWorkloadsMap) =>
          monolithicSimulatorDescs.foreach(monolithicSimulatorDesc => {
            if (sweepC) {
              allExperiments ::= new Experiment(
                name = "google-monolithic-%s_path-vary_c-allocation_%s"
                  .format(multiOrSingle, monolithicSimulatorDesc.allocationMode),
                workloadToSweepOver = "Service",
                workloadDescs = wlDescs,
                schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
                constantThinkTimeRange = constantRange,
                perTaskThinkTimeRange = 0.005 :: Nil,
                blackListPercentRange = 0.0 :: Nil,
                schedulerWorkloadMap = monolithicSchedulerWorkloadMap,
                simulatorDesc = monolithicSimulatorDesc,
                logging = doLogging,
                outputDirectory = outputDirName,
                prefillCpuLimits = prefillCpuLim,
                simulationTimeout = timeout)
            }

            if (sweepCL) {
              allExperiments ::= new Experiment(
                name = "google-monolithic-%s_path-vary_cl-allocation_%s"
                  .format(multiOrSingle, monolithicSimulatorDesc.allocationMode),
                workloadToSweepOver = "Service",
                workloadDescs = wlDescs,
                schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
                constantThinkTimeRange = constantRange,
                perTaskThinkTimeRange = perTaskRange,
                blackListPercentRange = 0.0 :: Nil,
                schedulerWorkloadMap = monolithicSchedulerWorkloadMap,
                simulatorDesc = monolithicSimulatorDesc,
                logging = doLogging,
                outputDirectory = outputDirName,
                prefillCpuLimits = prefillCpuLim,
                simulationTimeout = timeout)
            }

            if (sweepL) {
              allExperiments ::= new Experiment(
                name = "google-monolithic-%s_path-vary_l-allocation_%s"
                  .format(multiOrSingle, monolithicSimulatorDesc.allocationMode),
                workloadToSweepOver = "Service",
                workloadDescs = wlDescs,
                schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
                constantThinkTimeRange = 0.1 :: Nil,
                perTaskThinkTimeRange = perTaskRange,
                blackListPercentRange = 0.0 :: Nil,
                schedulerWorkloadMap = monolithicSchedulerWorkloadMap,
                simulatorDesc = monolithicSimulatorDesc,
                logging = doLogging,
                outputDirectory = outputDirName,
                prefillCpuLimits = prefillCpuLim,
                simulationTimeout = timeout)
            }

            if (sweepPickiness) {
              allExperiments ::= new Experiment(
                name = "google-monolithic-%s_path-vary_pickiness-allocation_%s"
                  .format(multiOrSingle, monolithicSimulatorDesc.allocationMode),
                workloadToSweepOver = "Service",
                workloadDescs = wlDescs,
                schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
                constantThinkTimeRange = 0.1 :: Nil,
                perTaskThinkTimeRange = 0.005 :: Nil,
                blackListPercentRange = pickinessRange,
                schedulerWorkloadMap = monolithicSchedulerWorkloadMap,
                simulatorDesc = monolithicSimulatorDesc,
                logging = doLogging,
                outputDirectory = outputDirName,
                prefillCpuLimits = prefillCpuLim,
                simulationTimeout = timeout)
            }

            if (sweepLambda) {
              allExperiments ::= new Experiment(
                name = "google-monolithic-%s_path-vary_lambda-allocation_%s"
                  .format(multiOrSingle, monolithicSimulatorDesc.allocationMode),
                workloadToSweepOver = "Service",
                workloadDescs = wlDescs,
                schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
                avgJobInterarrivalTimeRange = Some(interArrivalScaleRange),
                constantThinkTimeRange = 0.1 :: Nil,
                perTaskThinkTimeRange = 0.005 :: Nil,
                blackListPercentRange = 0.0 :: Nil,
                schedulerWorkloadMap = monolithicSchedulerWorkloadMap,
                simulatorDesc = monolithicSimulatorDesc,
                logging = doLogging,
                outputDirectory = outputDirName,
                prefillCpuLimits = prefillCpuLim,
                simulationTimeout = timeout)
            }
          })
      }
    }

    // Spark
    if(runSpark){
      logger.info("\tSpark")
      /**
        * Set up SchedulerDesc-s.
        */
      val sparkSchedulerDesc = new SchedulerDesc(
        name = "Spark".intern(),
        constantThinkTimes = Map("Batch" -> 0.01, "Service" -> 0.01),
        perTaskThinkTimes = Map("Batch" -> 0.005, "Service" -> 0.01))

      /**
        * Set up workload-to-scheduler mappings.
        */
      val sparkSchedulerWorkloadMap =
        Map[String, Seq[String]]("Batch" -> Seq("Spark"),
          "Service" -> Seq("Spark"))

      /**
        * Set up a simulatorDesc-s.
        */
      val sparkSimulatorDesc =
        new SparkSimulatorDesc(Array(sparkSchedulerDesc),
          globalRunTime)

      /**
        * Set up a run of experiments.
        */
      // Loop over both a single and multi path Monolithic scheduler.
      // Emulate a single path scheduler by making the parameter sweep
      // apply to both the "Service" and "Batch" workload types for it.
      val singlePathSetup = ("single", Map("Spark" -> List("Service")))
      //val multiPathSetup =
      //    ("multi", Map("Spark" -> List("Service", "Batch")))
      List(singlePathSetup).foreach {
        case (multiOrSingle, schedulerWorkloadsMap) =>
          if (sweepC) {
            allExperiments ::= new Experiment(
              name = "google-spark-%s_path-vary_c-allocation_%s"
                .format(multiOrSingle, sparkSimulatorDesc.allocationMode),
              workloadToSweepOver = "Service",
              workloadDescs = wlDescs,
              schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
              constantThinkTimeRange = constantRange,
              perTaskThinkTimeRange = 0.005 :: Nil,
              blackListPercentRange = 0.0 :: Nil,
              schedulerWorkloadMap = sparkSchedulerWorkloadMap,
              simulatorDesc = sparkSimulatorDesc,
              logging = doLogging,
              outputDirectory = outputDirName,
              prefillCpuLimits = prefillCpuLim,
              simulationTimeout = timeout)
          }

          if (sweepCL) {
            allExperiments ::= new Experiment(
              name = "google-spark-%s_path-vary_cl-allocation_%s"
                .format(multiOrSingle, sparkSimulatorDesc.allocationMode),
              workloadToSweepOver = "Service",
              workloadDescs = wlDescs,
              schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
              constantThinkTimeRange = constantRange,
              perTaskThinkTimeRange = perTaskRange,
              blackListPercentRange = 0.0 :: Nil,
              schedulerWorkloadMap = sparkSchedulerWorkloadMap,
              simulatorDesc = sparkSimulatorDesc,
              logging = doLogging,
              outputDirectory = outputDirName,
              prefillCpuLimits = prefillCpuLim,
              simulationTimeout = timeout)
          }

          if (sweepL) {
            allExperiments ::= new Experiment(
              name = "google-spark-%s_path-vary_l-allocation_%s"
                .format(multiOrSingle, sparkSimulatorDesc.allocationMode),
              workloadToSweepOver = "Service",
              workloadDescs = wlDescs,
              schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
              constantThinkTimeRange = 0.1 :: Nil,
              perTaskThinkTimeRange = perTaskRange,
              blackListPercentRange = 0.0 :: Nil,
              schedulerWorkloadMap = sparkSchedulerWorkloadMap,
              simulatorDesc = sparkSimulatorDesc,
              logging = doLogging,
              outputDirectory = outputDirName,
              prefillCpuLimits = prefillCpuLim,
              simulationTimeout = timeout)
          }

          if (sweepPickiness) {
            allExperiments ::= new Experiment(
              name = "google-spark-%s_path-vary_pickiness-allocation_%s"
                .format(multiOrSingle, sparkSimulatorDesc.allocationMode),
              workloadToSweepOver = "Service",
              workloadDescs = wlDescs,
              schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
              constantThinkTimeRange = 0.1 :: Nil,
              perTaskThinkTimeRange = 0.005 :: Nil,
              blackListPercentRange = pickinessRange,
              schedulerWorkloadMap = sparkSchedulerWorkloadMap,
              simulatorDesc = sparkSimulatorDesc,
              logging = doLogging,
              outputDirectory = outputDirName,
              prefillCpuLimits = prefillCpuLim,
              simulationTimeout = timeout)
          }

          if (sweepLambda) {
            allExperiments ::= new Experiment(
              name = "google-spark-%s_path-vary_lambda-allocation_%s"
                .format(multiOrSingle, sparkSimulatorDesc.allocationMode),
              workloadToSweepOver = "Service",
              workloadDescs = wlDescs,
              schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
              avgJobInterarrivalTimeRange = Some(interArrivalScaleRange),
              constantThinkTimeRange = 0.1 :: Nil,
              perTaskThinkTimeRange = 0.005 :: Nil,
              blackListPercentRange = 0.0 :: Nil,
              schedulerWorkloadMap = sparkSchedulerWorkloadMap,
              simulatorDesc = sparkSimulatorDesc,
              logging = doLogging,
              outputDirectory = outputDirName,
              prefillCpuLimits = prefillCpuLim,
              simulationTimeout = timeout)
          }
      }
    }

    // New Spark
    if(runNewSpark){
      logger.info("\tNew Spark")
      /**
        * Set up SchedulerDesc-s.
        */
      val newsparkSchedulerDesc = new SchedulerDesc(
        name = "NewSpark".intern(),
        constantThinkTimes = Map("Batch" -> 0.01, "Service" -> 0.01),
        perTaskThinkTimes = Map("Batch" -> 0.005, "Service" -> 0.01))

      /**
        * Set up workload-to-scheduler mappings.
        */
      val newsparkSchedulerWorkloadMap =
        Map[String, Seq[String]]("Batch" -> Seq("NewSpark"),
          "Service" -> Seq("NewSpark"))

      /**
        * Set up a simulatorDesc-s.
        */
      val newsparkSimulatorDesc =
        new NewSparkSimulatorDesc(Array(newsparkSchedulerDesc),
          globalRunTime)

      /**
        * Set up a run of experiments.
        */
      // Loop over both a single and multi path Monolithic scheduler.
      // Emulate a single path scheduler by making the parameter sweep
      // apply to both the "Service" and "Batch" workload types for it.
      val singlePathSetup = ("single", Map("NewSpark" -> List("Service")))
      //val multiPathSetup =
      //    ("multi", Map("Spark" -> List("Service", "Batch")))
      List(singlePathSetup).foreach {
        case (multiOrSingle, schedulerWorkloadsMap) =>
          if (sweepC) {
            allExperiments ::= new Experiment(
              name = "google-newspark-%s_path-vary_c-allocation_%s"
                .format(multiOrSingle, newsparkSimulatorDesc.allocationMode),
              workloadToSweepOver = "Service",
              workloadDescs = wlDescs,
              schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
              constantThinkTimeRange = constantRange,
              perTaskThinkTimeRange = 0.005 :: Nil,
              blackListPercentRange = 0.0 :: Nil,
              schedulerWorkloadMap = newsparkSchedulerWorkloadMap,
              simulatorDesc = newsparkSimulatorDesc,
              logging = doLogging,
              outputDirectory = outputDirName,
              prefillCpuLimits = prefillCpuLim,
              simulationTimeout = timeout)
          }

          if (sweepCL) {
            allExperiments ::= new Experiment(
              name = "google-newspark-%s_path-vary_cl-allocation_%s"
                .format(multiOrSingle, newsparkSimulatorDesc.allocationMode),
              workloadToSweepOver = "Service",
              workloadDescs = wlDescs,
              schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
              constantThinkTimeRange = constantRange,
              perTaskThinkTimeRange = perTaskRange,
              blackListPercentRange = 0.0 :: Nil,
              schedulerWorkloadMap = newsparkSchedulerWorkloadMap,
              simulatorDesc = newsparkSimulatorDesc,
              logging = doLogging,
              outputDirectory = outputDirName,
              prefillCpuLimits = prefillCpuLim,
              simulationTimeout = timeout)
          }

          if (sweepL) {
            allExperiments ::= new Experiment(
              name = "google-newspark-%s_path-vary_l-allocation_%s"
                .format(multiOrSingle, newsparkSimulatorDesc.allocationMode),
              workloadToSweepOver = "Service",
              workloadDescs = wlDescs,
              schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
              constantThinkTimeRange = 0.1 :: Nil,
              perTaskThinkTimeRange = perTaskRange,
              blackListPercentRange = 0.0 :: Nil,
              schedulerWorkloadMap = newsparkSchedulerWorkloadMap,
              simulatorDesc = newsparkSimulatorDesc,
              logging = doLogging,
              outputDirectory = outputDirName,
              prefillCpuLimits = prefillCpuLim,
              simulationTimeout = timeout)
          }

          if (sweepPickiness) {
            allExperiments ::= new Experiment(
              name = "google-newspark-%s_path-vary_pickiness-allocation_%s"
                .format(multiOrSingle, newsparkSimulatorDesc.allocationMode),
              workloadToSweepOver = "Service",
              workloadDescs = wlDescs,
              schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
              constantThinkTimeRange = 0.1 :: Nil,
              perTaskThinkTimeRange = 0.005 :: Nil,
              blackListPercentRange = pickinessRange,
              schedulerWorkloadMap = newsparkSchedulerWorkloadMap,
              simulatorDesc = newsparkSimulatorDesc,
              logging = doLogging,
              outputDirectory = outputDirName,
              prefillCpuLimits = prefillCpuLim,
              simulationTimeout = timeout)
          }

          if (sweepLambda) {
            allExperiments ::= new Experiment(
              name = "google-newspark-%s_path-vary_lambda-allocation_%s"
                .format(multiOrSingle, newsparkSimulatorDesc.allocationMode),
              workloadToSweepOver = "Service",
              workloadDescs = wlDescs,
              schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
              avgJobInterarrivalTimeRange = Some(interArrivalScaleRange),
              constantThinkTimeRange = 0.1 :: Nil,
              perTaskThinkTimeRange = 0.005 :: Nil,
              blackListPercentRange = 0.0 :: Nil,
              schedulerWorkloadMap = newsparkSchedulerWorkloadMap,
              simulatorDesc = newsparkSimulatorDesc,
              logging = doLogging,
              outputDirectory = outputDirName,
              prefillCpuLimits = prefillCpuLim,
              simulationTimeout = timeout)
          }
      }
    }

    // Mesos
    if (runMesos){
      logger.info("\tMesos")
      /**
        * Set up SchedulerDesc-s.
        */
      val mesosBatchSchedulerDesc = new MesosSchedulerDesc(
        name = "MesosBatch".intern(),
        constantThinkTimes = Map("Batch" -> 0.01),
        perTaskThinkTimes = Map("Batch" -> 0.005),
        schedulePartialJobs = true)

      val mesosServiceSchedulerDesc = new MesosSchedulerDesc(
        name = "MesosService".intern(),
        constantThinkTimes = Map("Service" -> 0.01),
        perTaskThinkTimes = Map("Service" -> 0.01),
        schedulePartialJobs = true)

      val mesosSchedulerDescs = Array(mesosBatchSchedulerDesc,
        mesosServiceSchedulerDesc)

      //      val mesosBatchScheduler2Desc = new MesosSchedulerDesc(
      //        name = "MesosBatch-2".intern(),
      //        constantThinkTimes = Map("Batch" -> 0.01),
      //        perTaskThinkTimes = Map("Batch" -> 0.005),
      //        schedulePartialJobs = true)
      //
      //      val mesosBatchScheduler3Desc = new MesosSchedulerDesc(
      //        name = "MesosBatch-3".intern(),
      //        constantThinkTimes = Map("Batch" -> 0.01),
      //        perTaskThinkTimes = Map("Batch" -> 0.005),
      //        schedulePartialJobs = true)
      //
      //      val mesosBatchScheduler4Desc = new MesosSchedulerDesc(
      //        name = "MesosBatch-4".intern(),
      //        constantThinkTimes = Map("Batch" -> 0.01),
      //        perTaskThinkTimes = Map("Batch" -> 0.005),
      //        schedulePartialJobs = true)
      //
      //      val mesos4BatchSchedulerDescs = Array(mesosBatchSchedulerDesc,
      //        mesosBatchScheduler2Desc,
      //        mesosBatchScheduler3Desc,
      //        mesosBatchScheduler4Desc,
      //        mesosServiceSchedulerDesc)

      /**
        * Set up workload-to-scheduler mappings.
        */
      val mesos1BatchSchedulerWorkloadMap =
        Map[String, Seq[String]]("Batch" -> Seq("MesosBatch"),
          "Service" -> Seq("MesosService"))

      /**
        * Set up a simulatorDesc-s.
        */
      val mesosSimulatorDescs: ListBuffer[MesosSimulatorDesc] = ListBuffer[MesosSimulatorDesc]()
      allocationModes.foreach(allocationMode => {
        // Mesos simulator with 1 batch schedulers
        mesosSimulatorDescs += new MesosSimulatorDesc(mesosSchedulerDescs,
            runTime = globalRunTime,
            allocatorConstantThinkTime = 0.001, allocationMode)
        // Mesos simulator with 4 batch schedulers
        //      mesosSimulatorDescs +=
        //              new MesosSimulatorDesc(mesos4BatchSchedulerDescs,
        //                runTime = globalRunTime,
        //                allocatorConstantThinkTime = 0.001, allocationMode)
      })

      /**
        * Set up a run of experiments.
        */
      // val mesosSchedulerWorkloadMap = mesos4BatchSchedulerWorkloadMap
      val mesosSchedulerWorkloadMap = mesos1BatchSchedulerWorkloadMap

      // val mesosSchedWorkloadsToSweep = Map("MesosBatch" -> List("Batch"),
      //                                      "MesosBatch-2" -> List("Batch"),
      //                                      "MesosBatch-3" -> List("Batch"),
      //                                      "MesosBatch-4" -> List("Batch"))
      val mesosSchedWorkloadsToSweep = Map("MesosService" -> List("Service"))

      // val mesosWorkloadToSweep = "Batch"
      val mesosWorkloadToSweep = "Service"

      mesosSimulatorDescs.foreach(mesosSimulatorDesc => {
        if (sweepC) {
          allExperiments ::= new Experiment(
            name = "google-mesos-single_path-vary_c-allocation_%s".format(mesosSimulatorDesc.allocationMode),
            workloadToSweepOver = mesosWorkloadToSweep,
            workloadDescs = wlDescs,
            schedulerWorkloadsToSweepOver = mesosSchedWorkloadsToSweep,
            // constantThinkTimeRange = (0.1 :: Nil),
            constantThinkTimeRange = constantRange,
            perTaskThinkTimeRange = 0.005 :: Nil,
            blackListPercentRange = 0.0 :: Nil,
            schedulerWorkloadMap = mesosSchedulerWorkloadMap,
            simulatorDesc = mesosSimulatorDesc,
            logging = doLogging,
            outputDirectory = outputDirName,
            prefillCpuLimits = prefillCpuLim,
            simulationTimeout = timeout)
        }

        if (sweepCL) {
          allExperiments ::= new Experiment(
            name = "google-mesos-single_path-vary_cl-allocation_%s".format(mesosSimulatorDesc.allocationMode),
            workloadToSweepOver = mesosWorkloadToSweep,
            workloadDescs = wlDescs,
            schedulerWorkloadsToSweepOver = mesosSchedWorkloadsToSweep,
            constantThinkTimeRange = constantRange,
            perTaskThinkTimeRange = perTaskRange,
            blackListPercentRange = 0.0 :: Nil,
            schedulerWorkloadMap = mesosSchedulerWorkloadMap,
            simulatorDesc = mesosSimulatorDesc,
            logging = doLogging,
            outputDirectory = outputDirName,
            prefillCpuLimits = prefillCpuLim,
            simulationTimeout = timeout)
        }

        if (sweepL) {
          allExperiments ::= new Experiment(
            name = "google-mesos-single_path-vary_l-allocation_%s".format(mesosSimulatorDesc.allocationMode),
            workloadToSweepOver = mesosWorkloadToSweep,
            workloadDescs = wlDescs,
            schedulerWorkloadsToSweepOver = mesosSchedWorkloadsToSweep,
            constantThinkTimeRange = 0.1 :: Nil,
            perTaskThinkTimeRange = perTaskRange,
            blackListPercentRange = 0.0 :: Nil,
            schedulerWorkloadMap = mesosSchedulerWorkloadMap,
            simulatorDesc = mesosSimulatorDesc,
            logging = doLogging,
            outputDirectory = outputDirName,
            prefillCpuLimits = prefillCpuLim,
            simulationTimeout = timeout)
        }

        if (sweepPickiness) {
          allExperiments ::= new Experiment(
            name = "google-mesos-single_path-vary_pickiness-allocation_%s".format(mesosSimulatorDesc.allocationMode),
            workloadToSweepOver = mesosWorkloadToSweep,
            workloadDescs = wlDescs,
            schedulerWorkloadsToSweepOver = mesosSchedWorkloadsToSweep,
            constantThinkTimeRange = 0.1 :: Nil,
            perTaskThinkTimeRange = 0.005 :: Nil,
            blackListPercentRange = pickinessRange,
            schedulerWorkloadMap = mesosSchedulerWorkloadMap,
            simulatorDesc = mesosSimulatorDesc,
            logging = doLogging,
            outputDirectory = outputDirName,
            prefillCpuLimits = prefillCpuLim,
            simulationTimeout = timeout)
        }

        if (sweepLambda) {
          allExperiments ::= new Experiment(
            name = "google-mesos-single_path-vary_lambda-allocation_%s".format(mesosSimulatorDesc.allocationMode),
            workloadToSweepOver = "Service",
            workloadDescs = wlDescs,
            schedulerWorkloadsToSweepOver = Map("MesosService" -> List("Service")),
            avgJobInterarrivalTimeRange = Some(interArrivalScaleRange),
            constantThinkTimeRange = 0.1 :: Nil,
            perTaskThinkTimeRange = 0.005 :: Nil,
            blackListPercentRange = 0.0 :: Nil,
            schedulerWorkloadMap = mesosSchedulerWorkloadMap,
            simulatorDesc = mesosSimulatorDesc,
            logging = doLogging,
            outputDirectory = outputDirName,
            prefillCpuLimits = prefillCpuLim,
            simulationTimeout = timeout)
        }
      })
    }

    // Omega
    if (runOmega){
      logger.info("\tOmega")
      /**
        * Set up SchedulerDesc-s.
        */
      def generateOmegaSchedulerDescs(numServiceScheds: Int,
                                      numBatchScheds: Int)
      : Array[SchedulerDesc] = {
        val schedDescs = ArrayBuffer[SchedulerDesc]()
        (1 to numBatchScheds).foreach(i => {
          schedDescs +=
            new SchedulerDesc(name = "OmegaBatch-%d".format(i).intern(),
              constantThinkTimes = Map("Batch" -> 0.01),
              perTaskThinkTimes = Map("Batch" -> 0.01))
        })
        (1 to numServiceScheds).foreach(i => {
          schedDescs +=
            new SchedulerDesc(name = "OmegaService-%d".format(i).intern(),
              constantThinkTimes = Map("Service" -> 0.01),
              perTaskThinkTimes = Map("Service" -> 0.01))
        })
        //      println("Generated schedulerDescs: " + schedDescs)
        schedDescs.toArray
      }

      /**
        * Set up workload-to-scheduler mappings.
        */

      /**
        * Set up a simulatorDesc-s.
        */

      /**
        * Set up a run of experiments.
        */
      val numOmegaServiceSchedsRange = Seq(1)
      val numOmegaBatchSchedsRange = Seq(1)

      /**
        * Returns a Map with mappings from workload to an arbitrary
        * number of schedulers. These mappings are used by the simulator
        * to decide which scheduler to send a job to when it arrives.
        * If more than one scheduler is specified for a single workload
        * name, then the jobs will be scheduled round-robin across all
        * of those schedulers.
        */
      type SchedulerWorkloadMap = Map[String, Seq[String]]
      def generateSchedulerWorkloadMap(schedulerNamePrefix: String,
                                       numServiceScheds: Int,
                                       numBatchScheds: Int)
      : SchedulerWorkloadMap = {
        //        println("Generating workload map with %d serv scheds & %d batch scheds"
        //          .format(numServiceScheds, numBatchScheds))
        val schedWorkloadMap = collection.mutable.Map[String, Seq[String]]()
        schedWorkloadMap("Service") =
          (1 to numServiceScheds).map(schedulerNamePrefix + "Service-" + _)
        schedWorkloadMap("Batch") =
          (1 to numBatchScheds).map(schedulerNamePrefix + "Batch-" + _)
        //        println("Generated schedulerWorkloadMap: " + schedWorkloadMap)
        schedWorkloadMap.toMap
      }

      /**
        * Returns a Map whose entries represent which scheduler/workload pairs
        * to apply the L/C parameter sweep to.
        */
      type SchedulerWorkloadsToSweep = Map[String, Seq[String]]
      def generateSchedulerWorkloadsToSweep(schedulerNamePrefix: String,
                                            numServiceScheds: Int,
                                            numBatchScheds: Int)
      : SchedulerWorkloadsToSweep = {
        //        println("Generating workload map with %d serv scheds & %d batch scheds"
        //          .format(numServiceScheds, numBatchScheds))
        val schedWorkloadsToSweep = collection.mutable.Map[String, Seq[String]]()
        (1 to numServiceScheds).foreach{ i: Int => {
          schedWorkloadsToSweep(schedulerNamePrefix + "Service-" + i) = Seq("Service")
        }}
        (1 to numBatchScheds).foreach{ i: Int => {
          schedWorkloadsToSweep(schedulerNamePrefix + "Batch-" + i) = Seq("Batch")
        }}
        //        println("Generated schedulerWorkloadsToSweepMap: " + schedWorkloadsToSweep)
        schedWorkloadsToSweep.toMap
      }

      val omegaSimulatorSetups =
        for (numOmegaServiceScheds <- numOmegaServiceSchedsRange;
             numOmegaBatchScheds <- numOmegaBatchSchedsRange) yield {
          // List of the different {{SimulatorDesc}}s to be run with the
          // SchedulerWorkloadMap and SchedulerWorkloadToSweep.
          val omegaSimulatorDescs = for(
            conflictMode <- Seq("sequence-numbers", "resource-fit");
            transactionMode <- Seq("all-or-nothing", "incremental");
            allocationMode <- allocationModes) yield {
            new OmegaSimulatorDesc(
              generateOmegaSchedulerDescs(numOmegaServiceScheds, numOmegaBatchScheds),
              runTime = globalRunTime,
              conflictMode,
              transactionMode, allocationMode)
          }

          val omegaSchedulerWorkloadMap =
            generateSchedulerWorkloadMap("Omega",
              numOmegaServiceScheds,
              numOmegaBatchScheds)

          val omegaSchedulerWorkloadsToSweep =
            generateSchedulerWorkloadsToSweep("Omega",
              numServiceScheds = 0,
              numOmegaBatchScheds)
          (omegaSimulatorDescs,omegaSchedulerWorkloadMap, omegaSchedulerWorkloadsToSweep)
        }

      omegaSimulatorSetups.foreach { case(simDescs, schedWLMap, schedWLToSweep) =>
        for (simDesc <- simDescs) {
          val numServiceScheds =
            simDesc.schedulerDescs.count(_.name.contains("Service"))
          val numBatchScheds =
            simDesc.schedulerDescs.count(_.name.contains("Batch"))
          if (sweepC) {
            allExperiments ::= new Experiment(
              name = "google-omega-%s-%s-%d_service-%d_batch-single_path-vary_c-allocation_%s"
                .format(simDesc.conflictMode,
                  simDesc.transactionMode,
                  numServiceScheds,
                  numBatchScheds,
                  simDesc.allocationMode
                ),
              workloadToSweepOver = "Service",
              workloadDescs = wlDescs,
              schedulerWorkloadsToSweepOver = schedWLToSweep,
              constantThinkTimeRange = constantRange,
              perTaskThinkTimeRange = 0.005 :: Nil,
              blackListPercentRange = 0.0 :: Nil,
              schedulerWorkloadMap = schedWLMap,
              simulatorDesc = simDesc,
              logging = doLogging,
              outputDirectory = outputDirName,
              prefillCpuLimits = prefillCpuLim,
              simulationTimeout = timeout)
          }

          if (sweepCL) {
            allExperiments ::= new Experiment(
              name = "google-omega-%s-%s-%d_service-%d_batch-single_path-vary_cl-allocation_%s"
                .format(simDesc.conflictMode,
                  simDesc.transactionMode,
                  numServiceScheds,
                  numBatchScheds,
                  simDesc.allocationMode),
              workloadToSweepOver = "Service",
              workloadDescs = wlDescs,
              schedulerWorkloadsToSweepOver = schedWLToSweep,
              constantThinkTimeRange = constantRange,
              perTaskThinkTimeRange = perTaskRange,
              blackListPercentRange = 0.0 :: Nil,
              schedulerWorkloadMap = schedWLMap,
              simulatorDesc = simDesc,
              logging = doLogging,
              outputDirectory = outputDirName,
              prefillCpuLimits = prefillCpuLim,
              simulationTimeout = timeout)
          }

          if (sweepL) {
            allExperiments ::= new Experiment(
              name = "google-omega-%s-%s-%d_service-%d_batch-single_path-vary_l-allocation_%s"
                .format(simDesc.conflictMode,
                  simDesc.transactionMode,
                  numServiceScheds,
                  numBatchScheds,
                  simDesc.allocationMode),
              workloadToSweepOver = "Service",
              workloadDescs = wlDescs,
              schedulerWorkloadsToSweepOver = schedWLToSweep,
              constantThinkTimeRange = 0.1 :: Nil,
              perTaskThinkTimeRange = perTaskRange,
              blackListPercentRange = 0.0 :: Nil,
              schedulerWorkloadMap = schedWLMap,
              simulatorDesc = simDesc,
              logging = doLogging,
              outputDirectory = outputDirName,
              prefillCpuLimits = prefillCpuLim,
              simulationTimeout = timeout)
          }

          if (sweepPickiness) {
            allExperiments ::= new Experiment(
              name = "google-omega-%s-%s-%d_service-%d_batch-single_path-vary_pickiness-allocation_%s"
                .format(simDesc.conflictMode,
                  simDesc.transactionMode,
                  numServiceScheds,
                  numBatchScheds,
                  simDesc.allocationMode),
              workloadToSweepOver = "Service",
              workloadDescs = wlDescs,
              schedulerWorkloadsToSweepOver = schedWLToSweep,
              constantThinkTimeRange = 0.1 :: Nil,
              perTaskThinkTimeRange = 0.005 :: Nil,
              blackListPercentRange = pickinessRange,
              schedulerWorkloadMap = schedWLMap,
              simulatorDesc = simDesc,
              logging = doLogging,
              outputDirectory = outputDirName,
              prefillCpuLimits = prefillCpuLim,
              simulationTimeout = timeout)
          }

          if (sweepLambda) {
            allExperiments ::= new Experiment(
              name = "google-omega-%s-%s-%d_service-%d_batch-single_path-vary_lambda-allocation_%s"
                .format(simDesc.conflictMode,
                  simDesc.transactionMode,
                  numServiceScheds,
                  numBatchScheds,
                  simDesc.allocationMode),
              workloadToSweepOver = "Service",
              workloadDescs = wlDescs,
              schedulerWorkloadsToSweepOver = schedWLToSweep,
              avgJobInterarrivalTimeRange = Some(interArrivalScaleRange),
              constantThinkTimeRange = 0.1 :: Nil,
              perTaskThinkTimeRange = 0.005 :: Nil,
              blackListPercentRange = 0.0 :: Nil,
              schedulerWorkloadMap = schedWLMap,
              simulatorDesc = simDesc,
              logging = doLogging,
              outputDirectory = outputDirName,
              prefillCpuLimits = prefillCpuLim,
              simulationTimeout = timeout)
          }
        }
      }
    }

    // Zoe
    if(runZoe){
      logger.info("\tZoe")
      /**
        * Set up SchedulerDesc-s.
        */
      val zoeSchedulerDesc = new SchedulerDesc(
        name = "Zoe".intern(),
        constantThinkTimes = Map("Batch" -> 0.01, "Batch-MPI" -> 0.01, "Service" -> 0.01, "Interactive" -> 0.01),
        perTaskThinkTimes = Map("Batch" -> 0.005, "Batch-MPI" -> 0.005, "Service" -> 0.01, "Interactive" -> 0.01))

      /**
        * Set up workload-to-scheduler mappings.
        */
      val zoeSchedulerWorkloadMap =
        Map[String, Seq[String]]("Batch" -> Seq("Zoe"), "Batch-MPI" -> Seq("Zoe"),
          "Service" -> Seq("Zoe"), "Interactive" -> Seq("Zoe"))

      /**
        * Set up a simulatorDesc-s.
        */
      val zoeSimulatorDescs: ListBuffer[ZoeSimulatorDesc] = ListBuffer[ZoeSimulatorDesc]()
      allocationModes.foreach(allocationMode => {
        policyModes.foreach(policyMode => {
          zoeSimulatorDescs += new ZoeSimulatorDesc(Array(zoeSchedulerDesc),
            globalRunTime, allocationMode, policyMode)
        })
      })

      /**
        * Set up a run of experiments.
        */
      // Loop over both a single and multi path Monolithic scheduler.
      // Emulate a single path scheduler by making the parameter sweep
      // apply to both the "Service" and "Batch" workload types for it.
      val singlePathSetup = ("single", Map("Zoe" -> List("Service")))
      val multiPathSetup =
        ("multi", Map("Zoe" -> List("Service", "Batch", "Batch-MPI", "Interactive")))
//      List(singlePathSetup, multiPathSetup).foreach {
      List(multiPathSetup).foreach{
        case (multiOrSingle, schedulerWorkloadsMap) =>
          zoeSimulatorDescs.foreach(zoeSimulatorDesc => {
            if (sweepC) {
              allExperiments ::= new Experiment(
                name = "zoe-%s_path-vary_c-allocation_%s-policy_%s"
                  .format(multiOrSingle, zoeSimulatorDesc.allocationMode, zoeSimulatorDesc.policyMode),
                workloadToSweepOver = "Service",
                workloadDescs = wlDescs,
                schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
                constantThinkTimeRange = constantRange,
                perTaskThinkTimeRange = 0.005 :: Nil,
                blackListPercentRange = 0.0 :: Nil,
                schedulerWorkloadMap = zoeSchedulerWorkloadMap,
                simulatorDesc = zoeSimulatorDesc,
                logging = doLogging,
                outputDirectory = outputDirName,
                prefillCpuLimits = prefillCpuLim,
                simulationTimeout = timeout)
            }

            if (sweepCL) {
              allExperiments ::= new Experiment(
                name = "zoe-%s_path-vary_cl-allocation_%s-policy_%s"
                  .format(multiOrSingle, zoeSimulatorDesc.allocationMode, zoeSimulatorDesc.policyMode),
                workloadToSweepOver = "Service",
                workloadDescs = wlDescs,
                schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
                constantThinkTimeRange = constantRange,
                perTaskThinkTimeRange = perTaskRange,
                blackListPercentRange = 0.0 :: Nil,
                schedulerWorkloadMap = zoeSchedulerWorkloadMap,
                simulatorDesc = zoeSimulatorDesc,
                logging = doLogging,
                outputDirectory = outputDirName,
                prefillCpuLimits = prefillCpuLim,
                simulationTimeout = timeout)
            }

            if (sweepL) {
              allExperiments ::= new Experiment(
                name = "zoe-%s_path-vary_l-allocation_%s-policy_%s"
                  .format(multiOrSingle, zoeSimulatorDesc.allocationMode, zoeSimulatorDesc.policyMode),
                workloadToSweepOver = "Service",
                workloadDescs = wlDescs,
                schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
                constantThinkTimeRange = 0.1 :: Nil,
                perTaskThinkTimeRange = perTaskRange,
                blackListPercentRange = 0.0 :: Nil,
                schedulerWorkloadMap = zoeSchedulerWorkloadMap,
                simulatorDesc = zoeSimulatorDesc,
                logging = doLogging,
                outputDirectory = outputDirName,
                prefillCpuLimits = prefillCpuLim,
                simulationTimeout = timeout)
            }

            if (sweepPickiness) {
              allExperiments ::= new Experiment(
                name = "zoe-%s_path-vary_pickiness-allocation_%s-policy_%s"
                  .format(multiOrSingle, zoeSimulatorDesc.allocationMode, zoeSimulatorDesc.policyMode),
                workloadToSweepOver = "Service",
                workloadDescs = wlDescs,
                schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
                constantThinkTimeRange = 0.1 :: Nil,
                perTaskThinkTimeRange = 0.005 :: Nil,
                blackListPercentRange = pickinessRange,
                schedulerWorkloadMap = zoeSchedulerWorkloadMap,
                simulatorDesc = zoeSimulatorDesc,
                logging = doLogging,
                outputDirectory = outputDirName,
                prefillCpuLimits = prefillCpuLim,
                simulationTimeout = timeout)
            }

            if (sweepLambda) {
              allExperiments ::= new Experiment(
                name = "zoe-%s_path-vary_lambda-allocation_%s-policy_%s"
                  .format(multiOrSingle, zoeSimulatorDesc.allocationMode, zoeSimulatorDesc.policyMode),
                workloadToSweepOver = "Service",
                workloadDescs = wlDescs,
                schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
                avgJobInterarrivalTimeRange = Some(interArrivalScaleRange),
//                constantThinkTimeRange = 0.1 :: Nil,
//                perTaskThinkTimeRange = 0.005 :: Nil,
                constantThinkTimeRange = 0 :: Nil,
                perTaskThinkTimeRange = 0 :: Nil,
                blackListPercentRange = 0.0 :: Nil,
                schedulerWorkloadMap = zoeSchedulerWorkloadMap,
                simulatorDesc = zoeSimulatorDesc,
                logging = doLogging,
                outputDirectory = outputDirName,
                prefillCpuLimits = prefillCpuLim,
                simulationTimeout = timeout)
            }
          })
      }
    }

    if(runZoePreemption){
      logger.info("\tZoePreemption")
      /**
        * Set up SchedulerDesc-s.
        */
      val zoePreemptionSchedulerDesc = new SchedulerDesc(
        name = "Zoe-Preemptive".intern(),
        constantThinkTimes = Map("Batch" -> 0.01, "Batch-MPI" -> 0.01, "Service" -> 0.01, "Interactive" -> 0.01),
        perTaskThinkTimes = Map("Batch" -> 0.005, "Batch-MPI" -> 0.005, "Service" -> 0.01, "Interactive" -> 0.01))

      /**
        * Set up workload-to-scheduler mappings.
        */
      val zoePreemptionSchedulerWorkloadMap =
      Map[String, Seq[String]]("Batch" -> Seq("Zoe-Preemptive"), "Batch-MPI" -> Seq("Zoe-Preemptive"),
        "Service" -> Seq("Zoe-Preemptive"), "Interactive" -> Seq("Zoe-Preemptive"))

      /**
        * Set up a simulatorDesc-s.
        */
      val zoePreemptionSimulatorDescs: ListBuffer[ZoePreemptionSimulatorDesc] = ListBuffer[ZoePreemptionSimulatorDesc]()
      allocationModes.foreach(allocationMode => {
        policyModes.foreach(policyMode => {
          zoePreemptionSimulatorDescs += new ZoePreemptionSimulatorDesc(Array(zoePreemptionSchedulerDesc),
            globalRunTime, allocationMode, policyMode)
        })
      })

      /**
        * Set up a run of experiments.
        */
      // Loop over both a single and multi path Monolithic scheduler.
      // Emulate a single path scheduler by making the parameter sweep
      // apply to both the "Service" and "Batch" workload types for it.
      val singlePathSetup = ("single", Map("Zoe-Preemptive" -> List("Service")))
      val multiPathSetup =
        ("multi", Map("Zoe-Preemptive" -> List("Service", "Batch", "Batch-MPI", "Interactive")))
      //      List(singlePathSetup, multiPathSetup).foreach {
      List(multiPathSetup).foreach{
        case (multiOrSingle, schedulerWorkloadsMap) =>
          zoePreemptionSimulatorDescs.foreach(zoePreemptionSimulatorDesc => {
            if (sweepC) {
              allExperiments ::= new Experiment(
                name = "zoe_preemptive-%s_path-vary_c-allocation_%s-policy_%s"
                  .format(multiOrSingle, zoePreemptionSimulatorDesc.allocationMode, zoePreemptionSimulatorDesc.policyMode),
                workloadToSweepOver = "Service",
                workloadDescs = wlDescs,
                schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
                constantThinkTimeRange = constantRange,
                perTaskThinkTimeRange = 0.005 :: Nil,
                blackListPercentRange = 0.0 :: Nil,
                schedulerWorkloadMap = zoePreemptionSchedulerWorkloadMap,
                simulatorDesc = zoePreemptionSimulatorDesc,
                logging = doLogging,
                outputDirectory = outputDirName,
                prefillCpuLimits = prefillCpuLim,
                simulationTimeout = timeout)
            }

            if (sweepCL) {
              allExperiments ::= new Experiment(
                name = "zoe_preemptive-%s_path-vary_cl-allocation_%s-policy_%s"
                  .format(multiOrSingle, zoePreemptionSimulatorDesc.allocationMode, zoePreemptionSimulatorDesc.policyMode),
                workloadToSweepOver = "Service",
                workloadDescs = wlDescs,
                schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
                constantThinkTimeRange = constantRange,
                perTaskThinkTimeRange = perTaskRange,
                blackListPercentRange = 0.0 :: Nil,
                schedulerWorkloadMap = zoePreemptionSchedulerWorkloadMap,
                simulatorDesc = zoePreemptionSimulatorDesc,
                logging = doLogging,
                outputDirectory = outputDirName,
                prefillCpuLimits = prefillCpuLim,
                simulationTimeout = timeout)
            }

            if (sweepL) {
              allExperiments ::= new Experiment(
                name = "zoe_preemptive-%s_path-vary_l-allocation_%s-policy_%s"
                  .format(multiOrSingle, zoePreemptionSimulatorDesc.allocationMode, zoePreemptionSimulatorDesc.policyMode),
                workloadToSweepOver = "Service",
                workloadDescs = wlDescs,
                schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
                constantThinkTimeRange = 0.1 :: Nil,
                perTaskThinkTimeRange = perTaskRange,
                blackListPercentRange = 0.0 :: Nil,
                schedulerWorkloadMap = zoePreemptionSchedulerWorkloadMap,
                simulatorDesc = zoePreemptionSimulatorDesc,
                logging = doLogging,
                outputDirectory = outputDirName,
                prefillCpuLimits = prefillCpuLim,
                simulationTimeout = timeout)
            }

            if (sweepPickiness) {
              allExperiments ::= new Experiment(
                name = "zoe_preemptive-%s_path-vary_pickiness-allocation_%s-policy_%s"
                  .format(multiOrSingle, zoePreemptionSimulatorDesc.allocationMode, zoePreemptionSimulatorDesc.policyMode),
                workloadToSweepOver = "Service",
                workloadDescs = wlDescs,
                schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
                constantThinkTimeRange = 0.1 :: Nil,
                perTaskThinkTimeRange = 0.005 :: Nil,
                blackListPercentRange = pickinessRange,
                schedulerWorkloadMap = zoePreemptionSchedulerWorkloadMap,
                simulatorDesc = zoePreemptionSimulatorDesc,
                logging = doLogging,
                outputDirectory = outputDirName,
                prefillCpuLimits = prefillCpuLim,
                simulationTimeout = timeout)
            }

            if (sweepLambda) {
              allExperiments ::= new Experiment(
                name = "zoe_preemptive-%s_path-vary_lambda-allocation_%s-policy_%s"
                  .format(multiOrSingle, zoePreemptionSimulatorDesc.allocationMode, zoePreemptionSimulatorDesc.policyMode),
                workloadToSweepOver = "Service",
                workloadDescs = wlDescs,
                schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
                avgJobInterarrivalTimeRange = Some(interArrivalScaleRange),
//                constantThinkTimeRange = 0.1 :: Nil,
//                perTaskThinkTimeRange = 0.005 :: Nil,
                constantThinkTimeRange = 0 :: Nil,
                perTaskThinkTimeRange = 0 :: Nil,
                blackListPercentRange = 0.0 :: Nil,
                schedulerWorkloadMap = zoePreemptionSchedulerWorkloadMap,
                simulatorDesc = zoePreemptionSimulatorDesc,
                logging = doLogging,
                outputDirectory = outputDirName,
                prefillCpuLimits = prefillCpuLim,
                simulationTimeout = timeout)
            }
          })
      }
    }


    /* Make a snapshot of the source file that has our settings in it */
    val settingsFileNames: List[String] = "Simulation.scala" :: "Workloads.scala" :: Nil
    settingsFileNames.foreach(settingsFileName => {
      logger.info("Making a copy of %s in %s".format(settingsFileName, outputDirName))

      val sourceFile = new File("src/main/scala/" + settingsFileName)
      val destFile = new File(outputDirName + "/" + settingsFileName + "-snapshot")
      // Create the output directory if it doesn't exist.
      new File(outputDirName).mkdirs()
      if(!destFile.exists()) {
        destFile.createNewFile()
      }
      var source: FileChannel = null
      var destination: FileChannel = null

      try {
        source = new FileInputStream(sourceFile).getChannel
        destination = new FileOutputStream(destFile).getChannel
        destination.transferFrom(source, 0, source.size());
      }
      finally {
        if(source != null) {
          source.close()
        }
        if(destination != null) {
          destination.close()
        }
      }
    })

    /**
      * Run the experiments we've set up.
      */
    val numTotalExps = allExperiments.length
    var numFinishedExps = 0

    if(numTotalExps < numThreads){
      logger.warn("The given number of threads is higher than the number of experiments to run. Adjusting it to avoid wasting resources.")
      numThreads = numTotalExps
    }
    val pool = java.util
      .concurrent
      .Executors
      .newFixedThreadPool(numThreads)
    Seed.set(randomSeed)
    logger.info(("Running %d experiments with the following options:\n" +
      "\t - threads:     %d\n" +
      "\t - random seed: %d\n").format(numTotalExps, numThreads, randomSeed))
    val startTime = System.currentTimeMillis()
    var futures = allExperiments.map(pool.submit)
    // Let go of pointers to Experiments because each Experiment will use
    // quite a lot of memory.
    allExperiments = Nil
    pool.shutdown()
    while(futures.nonEmpty) {
      Thread.sleep(threadSleep * 1000)
      val (completed, running) = futures.partition(_.isDone)
      if (completed.nonEmpty) {
        val elapsedTime: Double = (System.currentTimeMillis() - startTime) / 1000.0
        numFinishedExps += completed.length
        // Let's calculate the estimated time left for this simulation
        val numExpsLeft: Double = (numTotalExps - numFinishedExps) / numThreads.toDouble
        val etl = elapsedTime / Math.ceil(numFinishedExps / numThreads.toDouble) * numExpsLeft
        logger.info("%d more experiments just finished running. In total, %d of %d have finished. ETL: %.2fs (%.2fm)"
          .format(completed.length, numFinishedExps, numTotalExps, etl, etl / 60.0))
        //      completed.foreach(x => try x.get() catch {
        //        case e: Throwable => e.printStackTrace()
        //      })
        completed.foreach(x => x.get())
      }
      futures = running
    }
    logger.info("Done running all experiments. See output in %s."
      .format(outputDirName))
  }
}
