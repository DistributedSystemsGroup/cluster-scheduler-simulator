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

import java.io.File

/**
  * Set up workloads based on measurements from a real cluster.
  * In the Eurosys paper, we used measurements from Google clusters here.
  */
object Workloads {

  val globalNumMachines = 25
  val globalCpusPerMachine = 32
  val globalMemPerMachine = 128 //value must be in GB

  val globalMaxCoresPerJob = 20.0 // only used in NewSpark
//  val globalMaxCpusPerTask = 2
//  val globalMaxMemPerTask = 8
  // In this way we should disable the limitation introduced by these two variables
  // and completely use the input traces and their distribution
  val globalMaxCpusPerTask = globalCpusPerMachine
  val globalMaxMemPerTask = globalMemPerMachine

//  val maxTasksPerJob = ((globalNumMachines * globalCpusPerMachine * 1.5) / globalMaxCpusPerTask).toInt

  /**
    * Set up CellStateDescs that will go into WorkloadDescs. Fabricated
    * numbers are provided as an example. Enter numbers based on your
    * own clusters instead.
    */
  val eurecomCellStateDesc = new CellStateDesc(globalNumMachines,
    globalCpusPerMachine,
    globalMemPerMachine)

  val tenEurecomCellStateDesc = new CellStateDesc(globalNumMachines * 10,
    globalCpusPerMachine,
    globalMemPerMachine)

  val fiveEurecomCellStateDesc = new CellStateDesc(globalNumMachines * 5,
    globalCpusPerMachine,
    globalMemPerMachine)


  // example pre-fill workload generators.
  val prefillTraceFileName = "traces/init-cluster-state.log"
  assert(new File(prefillTraceFileName).exists(), "File " + prefillTraceFileName + " does not exist.")


  val batchServicePrefillTraceWLGenerator =
    new PrefillPbbTraceWorkloadGenerator("PrefillBatchService", prefillTraceFileName)



  // Set up example workload with jobs that have interarrival times
  // from trace-based interarrival times.
  val interarrivalTraceFileName = "traces/job-distribution-traces/" +
    "interarrival_cmb.log"
  val numTasksTraceFileName = "traces/job-distribution-traces/" +
    "csizes_cmb.log"
  val jobDurationTraceFileName = "traces/job-distribution-traces/" +
    "runtimes_cmb.log"
  assert(new File(interarrivalTraceFileName).exists(), "File " + interarrivalTraceFileName + " does not exist.")
  assert(new File(numTasksTraceFileName).exists(), "File " + numTasksTraceFileName + " does not exist.")
  assert(new File(jobDurationTraceFileName).exists(), "File " + jobDurationTraceFileName + " does not exist.")

  // A workload based on traces of interarrival times, tasks-per-job,
  // and job duration. Task shapes now based on pre-fill traces.
  val workloadGeneratorTraceAllBatch =
    new TraceAllZoeWLGenerator(
      "Batch".intern(),
      interarrivalTraceFileName,
      numTasksTraceFileName,
      jobDurationTraceFileName,
      prefillTraceFileName,
      maxCpusPerTask = globalMaxCpusPerTask,
      maxMemPerTask = globalMaxMemPerTask)

  val workloadGeneratorTraceAllService =
    new TraceAllZoeWLGenerator(
      "Service".intern(),
      interarrivalTraceFileName,
      numTasksTraceFileName,
      jobDurationTraceFileName,
      prefillTraceFileName,
      maxCpusPerTask = globalMaxCpusPerTask,
      maxMemPerTask = globalMaxMemPerTask)

//  val workloadGeneratorTraceAllBatch =
//    new UniformZoeWorkloadGenerator(
//      "Batch".intern(),
//      initJobInterarrivalTime = 1,
//      tasksPerJob = 50,
//      jobDuration = 200,
//      cpusPerTask = 2,
//      memPerTask = 16,
//      moldableTaskPercentage = 40,
//      jobsPerWorkload = 100)
//
//  val workloadGeneratorTraceAllService =
//    new UniformZoeWorkloadGenerator(
//      "Service".intern(),
//      initJobInterarrivalTime = 1,
//      tasksPerJob = 10,
//      jobDuration = 2000,
//      cpusPerTask = 2,
//      memPerTask = 16,
//      moldableTaskPercentage = 100,
//      jobsPerWorkload = 100)

  val eurecomCellTraceAllWorkloadPrefillDesc =
    WorkloadDesc(
      cell = "Eurecom",
      assignmentPolicy = "CMB_PBB",
      workloadGenerators =
        workloadGeneratorTraceAllBatch ::
          workloadGeneratorTraceAllService ::
          Nil,
      cellStateDesc = eurecomCellStateDesc ,
      prefillWorkloadGenerators =
        List(batchServicePrefillTraceWLGenerator)
    )

  val tenEurecomCellTraceAllWorkloadPrefillDesc =
    WorkloadDesc(
      cell = "10xEurecom",
      assignmentPolicy = "CMB_PBB",
      workloadGenerators =
        workloadGeneratorTraceAllBatch ::
          workloadGeneratorTraceAllService ::
          Nil,
      cellStateDesc = tenEurecomCellStateDesc,
      prefillWorkloadGenerators =
        List(batchServicePrefillTraceWLGenerator))

  val fiveEurecomCellTraceAllWorkloadPrefillDesc =
    WorkloadDesc(
      cell = "5xEurecom",
      assignmentPolicy = "CMB_PBB",
      workloadGenerators =
        workloadGeneratorTraceAllBatch ::
          workloadGeneratorTraceAllService ::
          Nil,
      cellStateDesc = fiveEurecomCellStateDesc,
      prefillWorkloadGenerators =
        List(batchServicePrefillTraceWLGenerator))
}
