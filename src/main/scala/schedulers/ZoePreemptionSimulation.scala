/**
  * Copyright (c) 2016 Eurecom
  * All rights reserved.
  **
  *Redistribution and use in source and binary forms, with or without
  *modification, are permitted provided that the following conditions are met:
  **
  *Redistributions of source code must retain the above copyright notice, this
  *list of conditions and the following disclaimer. Redistributions in binary
  *form must reproduce the above copyright notice, this list of conditions and the
  *following disclaimer in the documentation and/or other materials provided with
  *the distribution.
  **
  *Neither the name of Eurecom nor the names of its contributors may be used to
  *endorse or promote products derived from this software without specific prior
  *written permission.
  **
  *THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
  *ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  *WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
  *DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
  *FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
  *DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
  *SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
  *CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
  *OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  *OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
  */

package ClusterSchedulingSimulation.schedulers

import ClusterSchedulingSimulation.{CellState, Scheduler, _}
import org.apache.log4j.Logger

import scala.collection.mutable.ListBuffer
import scala.collection.{Iterator, mutable}

/* This class and its subclasses are used by factory method
 * ClusterSimulator.newScheduler() to determine which type of Simulator
 * to create and also to carry any extra fields that the factory needs to
 * construct the simulator.
 */
class ZoePreemptionSimulatorDesc(schedulerDescs: Seq[SchedulerDesc],
                       runTime: Double,
                       val allocationMode: AllocationModes.Value,
                       val policyMode: PolicyModes.Value)
  extends ClusterSimulatorDesc(runTime){
  override
  def newSimulator(constantThinkTime: Double,
                   perTaskThinkTime: Double,
                   blackListPercent: Double,
                   schedulerWorkloadsToSweepOver: Map[String, Seq[String]],
                   workloadToSchedulerMap: Map[String, Seq[String]],
                   cellStateDesc: CellStateDesc,
                   workloads: Seq[Workload],
                   prefillWorkloads: Seq[Workload],
                   logging: Boolean = false): ClusterSimulator = {
    val schedulers = mutable.HashMap[String, Scheduler]()
    // Create schedulers according to experiment parameters.
    schedulerDescs.foreach(schedDesc => {
      // If any of the scheduler-workload pairs we're sweeping over
      // are for this scheduler, then apply them before
      // registering it.
      val constantThinkTimes = mutable.HashMap[String, Double](
        schedDesc.constantThinkTimes.toSeq: _*)
      val perTaskThinkTimes = mutable.HashMap[String, Double](
        schedDesc.perTaskThinkTimes.toSeq: _*)
      var newBlackListPercent = 0.0
      if (schedulerWorkloadsToSweepOver
        .contains(schedDesc.name)) {
        newBlackListPercent = blackListPercent
        schedulerWorkloadsToSweepOver(schedDesc.name)
          .foreach(workloadName => {
            constantThinkTimes(workloadName) = constantThinkTime
            perTaskThinkTimes(workloadName) = perTaskThinkTime
          })
      }
      schedulers(schedDesc.name) =
        new ZoePreemptionScheduler(schedDesc.name,
          constantThinkTimes.toMap,
          perTaskThinkTimes.toMap,
          math.floor(newBlackListPercent *
            cellStateDesc.numMachines.toDouble).toInt,
          allocationMode,
          policyMode)
    })

    val cellState = new CellState(cellStateDesc.numMachines,
      cellStateDesc.cpusPerMachine,
      cellStateDesc.memPerMachine,
      conflictMode = "resource-fit",
      transactionMode = "all-or-nothing")




    new ClusterSimulator(cellState,
      schedulers.toMap,
      workloadToSchedulerMap,
      workloads,
      prefillWorkloads,
      logging,
      prefillScheduler = new ZoePreemptionPrefillScheduler(cellState))
  }
}

class ZoePreemptionPrefillScheduler(cellState: CellState)
  extends PrefillScheduler(cellState = cellState) {

  override
  def scheduleWorkloads(workloads: Seq[Workload]): Unit ={
    // Prefill jobs that exist at the beginning of the simulation.
    // Setting these up is similar to loading jobs that are part
    // of the simulation run; they need to be scheduled onto machines
    simulator.logger.info("Prefilling cell-state with %d workloads."
      .format(workloads.length))
    workloads.foreach(workload => {
      simulator.logger.info("Prefilling cell-state with %d jobs from workload %s."
        .format(workload.numJobs, workload.name))
      //var i = 0
      workload.getJobs.foreach(job => {
        //i += 1
        // println("Prefilling %d %s job id - %d."
        //         .format(i, workload.name, job.id))
        if (job.cpusPerTask > cellState.cpusPerMachine ||
          job.memPerTask > cellState.memPerMachine) {
          simulator.logger.warn(("IGNORING A JOB REQUIRING %f CPU & %f MEM PER TASK " +
            "BECAUSE machines only have %f cpu / %f mem.")
            .format(job.cpusPerTask, job.memPerTask,
              cellState.cpusPerMachine, cellState.memPerMachine))
        } else if((job.cpusPerTask * job.numTasks) > (cellState.cpusPerMachine * cellState.numMachines) ||
          (job.memPerTask * job.numTasks) > (cellState.memPerMachine * cellState.numMachines)){
          simulator.logger.warn(("IGNORING A JOB REQUIRING %f CPU & %f MEM PER %d TASK " +
            "BECAUSE THE TOTAL RESOURCES ARE NOT ENOUGH")
            .format(job.cpusPerTask, job.memPerTask, job.numTasks))
        } else {
          val claimDeltas = scheduleJob(job, cellState)
          // assert(job.numTasks == claimDeltas.length,
          //        "Prefill job failed to schedule.")
          cellState.scheduleEndEvents(claimDeltas)

          simulator.logger.info(("After prefill, common cell state now has %.2f%% (%.2f) " +
            "cpus and %.2f%% (%.2f) mem occupied.")
            .format(cellState.totalOccupiedCpus / cellState.totalCpus * 100.0,
              cellState.totalOccupiedCpus,
              cellState.totalOccupiedMem / cellState.totalMem * 100.0,
              cellState.totalOccupiedMem))
        }
      })
    })
  }
}

class ZoePreemptionScheduler(name: String,
                   constantThinkTimes: Map[String, Double],
                   perTaskThinkTimes: Map[String, Double],
                   numMachinesToBlackList: Double = 0,
                   allocationMode: AllocationModes.Value,
                   policyMode: PolicyModes.Value)
  extends Scheduler(name,
    constantThinkTimes,
    perTaskThinkTimes,
    numMachinesToBlackList,
    allocationMode) {
  val logger = Logger.getLogger(this.getClass.getName)
  logger.debug("scheduler-id-info: %d, %s, %d, %s, %s"
    .format(Thread.currentThread().getId,
      name,
      hashCode(),
      constantThinkTimes.mkString(";"),
      perTaskThinkTimes.mkString(";")))

  var pendingQueueAsList = new ListBuffer[Job]()
  override def jobQueueSize = pendingQueueAsList.count(_ != null)
//  var pendingQueueIterator = pendingQueueAsList.iterator
  var numJobsInQueue: Int = 0

  var runningQueueAsList = new ListBuffer[Job]()
  override def runningJobQueueSize: Long = runningQueueAsList.count(_ != null)
  var numRunningJobs: Int = 0

  //  var moldableQueueLength: Int = 0

  //  var elasticPendingQueue = new collection.mutable.ListBuffer[Job]()
  //  var elasticQueueIterator = elasticPendingQueue.iterator
  //  var numElasticJobsInQueue: Int = 0
  //  var schedulingElastic: Boolean = false
  //  var interruptElasticScheduling: Boolean = false
  //  var numElasticJobsSeen: Int = 0
//  var numJobsSeen: Int = 0
  var jobAttempt: Int = 0

  var privateCellState: CellState = _

  val schedulerPrefix = "[%s]".format(name)

  var previousJob: Job = _

  def getJobs(queue: ListBuffer[Job], currentTime:Double): ListBuffer[Job] ={
    val jobs = PolicyModes.getJobsWithSamePriority(queue, policyMode, currentTime)
    jobs.foreach(job => {
      removePendingJob(job)
    })
    jobs
  }

  /**
    * This function gives the next job in the queue
    *
    * @param iterator the iterator of the queue
    * @return  A tuple with:
    *          the next job in the queue or null if the end of the queue has been reached
    *          a counter with the number of elements skipped
    */
  def getNextJobInQueue(iterator: Iterator[Job]): (Job, Long) ={
    var elementsSkipped: Long = 0
    while(iterator.hasNext){
      val result = iterator.next()
      if(result != null)
        return (result, elementsSkipped)
      elementsSkipped += 1
    }
    (null, elementsSkipped)
  }

  def removePendingJob(job: Job): Unit = {
    val idx = pendingQueueAsList.indexOf(job)
    if(idx != -1 && pendingQueueAsList(idx) != null){
      numJobsInQueue -= 1
      pendingQueueAsList(idx) = null
    }

    //    pendingQueueAsList -= job
    //    moldableQueueIterator = pendingQueueAsList.iterator
    //    for (i <- 0 until currentIndex)
    //        moldableQueueIterator.next()
  }

  def removeRunningJob(job: Job): Unit = {
    val idx = runningQueueAsList.indexOf(job)
    if (idx != -1 && runningQueueAsList(idx) != null) {
      numRunningJobs -= 1
      runningQueueAsList(idx) = null
    }
  }


  //  def removeElasticJob(job: Job): Unit = {
  //    val idx = elasticPendingQueue.indexOf(job)
  //    if(idx != -1 && elasticPendingQueue(idx) != null){
  //      numElasticJobsInQueue -= 1
  //      elasticPendingQueue(idx) = null
  //    }
  ////    elasticPendingQueue -= job
  ////    elasticQueueIterator = elasticPendingQueue.iterator
  ////    for (i <- 0 until currentIndex)
  ////      elasticQueueIterator.next()
  //  }

  //  def addElasticJob(job: Job): Unit = {
  //    numElasticJobsInQueue += 1
  //    elasticPendingQueue += job
  ////    elasticPendingQueue = applyPolicy(elasticPendingQueue)
  //    elasticQueueIterator = elasticPendingQueue.iterator
  //  }

  def addPendingJob(job: Job, prepend: Boolean = false): Unit = {
    numJobsInQueue += 1
    if(!prepend)
      pendingQueueAsList += job
    else
      pendingQueueAsList.prepend(job)

    //    pendingQueueAsList = applyPolicy(pendingQueueAsList)
    //    moldableQueueIterator = pendingQueueAsList.iterator
  }

  def addRunningJob(job: Job): Unit = {
    numRunningJobs += 1
    runningQueueAsList += job
    //    pendingQueueAsList = applyPolicy(pendingQueueAsList)
    //    moldableQueueIterator = pendingQueueAsList.iterator
  }


  def isAllocationSuccessfully(claimDeltas: Seq[ClaimDelta], job: Job): Boolean = {
    allocationMode match {
      case AllocationModes.Incremental => claimDeltas.nonEmpty
      case AllocationModes.All => claimDeltas.size == job.moldableTasks
      case _ => false
    }
  }

  def syncCellState() {
    privateCellState = simulator.cellState.copy
    simulator.logger.debug(schedulerPrefix + " Scheduler %s (%d) has new private cell state %d"
      .format(name, hashCode, privateCellState.hashCode))
  }

  override
  def wakeUp(): Unit = {
    simulator.logger.trace("wakeUp method called.")
//    simulator.logger.warn("%f - Jobs in Queue: %d | Jobs Running: %d ".format(simulator.currentTime, numJobsInQueue, numRunningJobs))
    //    pendingQueueAsList = applyPolicy(pendingQueueAsList)
    // This is necessary when we order the list, because the job can be put at the begin on it
    // Caused by the async nature of this call
    //    moldableQueueIterator = pendingQueueAsList.iterator

    //    interruptElasticScheduling = true
    //    if(policyMode == PolicyModes.Fifo || policyMode == PolicyModes.PriorityFifo)
    //      numJobsSeen = 0

    scheduleNextJob()
  }

  var firstTime = true
  override
  def addJob(job: Job) = {
    simulator.logger.info(schedulerPrefix + " Enqueued job %d of workload type %s."
      .format(job.id, job.workloadName))
    super.addJob(job)

    addPendingJob(job)
//    simulator.logger.warn("%f - Added a new Job (%s) in the queue. Num Tasks: %d (%d/%d) | Cpus: %f | Mem: %f | Job Runtime: %f"
//      .format(simulator.currentTime, job.workloadName, job.numTasks, job.moldableTasks, job.elasticTasks, job.cpusPerTask, job.memPerTask, job.jobDuration ))
//    if(firstTime){
//      if (numJobsInQueue == 3){
//        wakeUp()
//        firstTime = false
//      }
//    }else
      wakeUp()
  }

  def preemptionForInelastic(jobToFit:Job,
                             jobsThatCanBePreempted: ListBuffer[JobPreemption],
                             jobsToPreempt: ListBuffer[JobPreemption]): ListBuffer[ClaimDelta] = {

    var jobsThatCanBePreempted_snapshot = new ListBuffer[JobPreemption]()
    var jobsToPreempt_snapshot = new ListBuffer[JobPreemption]()
    jobsToPreempt.foreach(job => {
      jobsToPreempt_snapshot += job.copy()
    })
    val privateCell_snapshot = privateCellState.copy

    var claimDelta_inelastic = new ListBuffer[ClaimDelta]()
    val relativeJobsToPreempt: ListBuffer[JobPreemption] = new ListBuffer[JobPreemption]()

    for(job: JobPreemption <- jobsThatCanBePreempted if claimDelta_inelastic.isEmpty){
      jobsThatCanBePreempted_snapshot += job.copy()

      if(job.elasticDeltasAfterPreemption.nonEmpty){
        var servicePreempted = false
        for(claimDelta:ClaimDelta <- job.elasticDeltasAfterPreemption if claimDelta_inelastic.isEmpty){
//          if(claimDelta_inelastic.isEmpty){
//            simulator.logger.info(schedulerPrefix + " The cell state is (%f cpus, %f mem)".format(privateCellState.availableCpus, privateCellState.availableMem))
//            simulator.logger.info(schedulerPrefix + " ClaimDelta Machine Info. id: %d / seqNum: %d".format(claimDelta.machineID, claimDelta.machineSeqNum))
          claimDelta.unApply(privateCellState)
          job.elasticDeltasAfterPreemption -= claimDelta
          job.elasticDeltasToPreempt += claimDelta
          servicePreempted = true

          claimDelta_inelastic = scheduleJob(jobToFit, privateCellState)
          if (!isAllocationSuccessfully(claimDelta_inelastic, jobToFit)) {
            claimDelta_inelastic.foreach(claimDelta => {
              claimDelta.unApply(privateCellState)
            })
            claimDelta_inelastic.clear()
          }
//          }
        }
        if(servicePreempted && !relativeJobsToPreempt.contains(job))
          relativeJobsToPreempt += job

      }
    }

//    if(claimDelta_inelastic.isEmpty){
//      for(job: JobPreemption <- jobsThatCanBePreempted){
//        if(job.inelasticDeltasAfterPreemption.nonEmpty){
//          for(claimDelta:ClaimDelta <- job.inelasticDeltasAfterPreemption){
//            claimDelta.unApply(privateCellState)
//            job.inelasticDeltasAfterPreemption -= claimDelta
//            job.inelasticDeltasToPreempt += claimDelta
//          }
//
//
//          claimDelta_inelastic = scheduleJob(jobToFit, privateCellState)
//          if (!isAllocationSuccessfully(claimDelta_inelastic, jobToFit)) {
//            claimDelta_inelastic.foreach(claimDelta => {
//              claimDelta.unApply(privateCellState)
//            })
//            claimDelta_inelastic.clear()
//          }
//          if(!relativeJobsToPreempt.contains(job))
//            relativeJobsToPreempt += job
//        }
//      }
//    }

    if(claimDelta_inelastic.isEmpty){
      relativeJobsToPreempt.clear()

      jobsToPreempt.clear()
      jobsToPreempt ++= jobsToPreempt_snapshot

      jobsThatCanBePreempted.clear()
      jobsThatCanBePreempted ++= jobsThatCanBePreempted_snapshot

      privateCellState = privateCell_snapshot
    }

    relativeJobsToPreempt.foreach(job => {
      if(!jobsToPreempt.contains(job))
        jobsToPreempt += job
    })

    claimDelta_inelastic
  }

  def preemptionForElastic(jobToFit:Job,
                             jobsThatCanBePreempted: ListBuffer[JobPreemption],
                             jobsToPreempt: ListBuffer[JobPreemption]): ListBuffer[ClaimDelta] = {

    var jobsThatCanBePreempted_snapshot = new ListBuffer[JobPreemption]()
    var jobsToPreempt_snapshot = new ListBuffer[JobPreemption]()
    jobsToPreempt.foreach(job => {
      jobsToPreempt_snapshot += job.copy()
    })
    val privateCell_snapshot = privateCellState.copy

    var claimDelta_elastic = new ListBuffer[ClaimDelta]()
    val relativeJobsToPreempt: ListBuffer[JobPreemption] = new ListBuffer[JobPreemption]()

    var elasticServicesToSchedule = jobToFit.elasticTasksUnscheduled
    var servicesPreempted = 0
    for(job: JobPreemption <- jobsThatCanBePreempted if elasticServicesToSchedule > 0){
      jobsThatCanBePreempted_snapshot += job.copy()

      if(job.elasticDeltasAfterPreemption.nonEmpty){
        var servicePreempted = false
        for(claimDelta:ClaimDelta <- job.elasticDeltasAfterPreemption if elasticServicesToSchedule > 0){
//          if(elasticServicesToSchedule > 0){
//            simulator.logger.info(schedulerPrefix + " The cell state is (%f cpus, %f mem)".format(privateCellState.availableCpus, privateCellState.availableMem))
          claimDelta.unApply(privateCellState)
          job.elasticDeltasAfterPreemption -= claimDelta
          job.elasticDeltasToPreempt += claimDelta
          servicesPreempted += 1
          servicePreempted = true

          val relativeClaimDelta_elastic = scheduleJob(jobToFit, privateCellState, elastic = true)
          if(relativeClaimDelta_elastic.nonEmpty){
            elasticServicesToSchedule -= relativeClaimDelta_elastic.size
            claimDelta_elastic ++= relativeClaimDelta_elastic
          }
//          }
        }
        if(servicePreempted && !relativeJobsToPreempt.contains(job))
          relativeJobsToPreempt += job
      }
    }

//    if(elasticServicesToSchedule > 0){
//      for(job: JobPreemption <- jobsThatCanBePreempted){
//        if(job.inelasticDeltasAfterPreemption.nonEmpty){
//          for(claimDelta:ClaimDelta <- job.inelasticDeltasAfterPreemption){
//            claimDelta.unApply(privateCellState)
//            job.inelasticDeltasAfterPreemption -= claimDelta
//            job.inelasticDeltasToPreempt += claimDelta
//          }
//
//          val relativeClaimDelta_elastic = scheduleJob(jobToFit, privateCellState, elastic = true)
//          elasticServicesToSchedule -= relativeClaimDelta_elastic.size
//          claimDelta_elastic ++= relativeClaimDelta_elastic
//          if(!relativeJobsToPreempt.contains(job))
//            relativeJobsToPreempt += job
//        }
//      }
//    }

    if(claimDelta_elastic.isEmpty){
      relativeJobsToPreempt.clear()

      jobsToPreempt.clear()
      jobsToPreempt ++= jobsToPreempt_snapshot

      jobsThatCanBePreempted.clear()
      jobsThatCanBePreempted ++= jobsThatCanBePreempted_snapshot

      privateCellState = privateCell_snapshot
      servicesPreempted = 0
    }

    relativeJobsToPreempt.foreach(job => {
      if(!jobsToPreempt.contains(job))
        jobsToPreempt += job
    })
    simulator.logger.info("[%d (%s)] preemptionForElastic was able to preempt %d to allocate %d services".format(jobToFit.id, jobToFit.workloadName, servicesPreempted, claimDelta_elastic.size))
    claimDelta_elastic
  }

  def compareMachineSeqNum(o1: ClaimDelta, o2: ClaimDelta): Int = {
    if (o1 == null && o2 == null)
      return 0
    if (o1 == null)
      return 1
    if (o2 == null)
      return -1

    if(o1.machineID == o2.machineID)
      return o1.machineSeqNum.compareTo(o2.machineSeqNum)

    o1.machineID.compareTo(o2.machineID)
  }

  def compareJobFinishTime(o1: Job, o2: Job): Int = {
    if (o1 == null && o2 == null)
      return 0
    if (o1 == null)
      return 1
    if (o2 == null)
      return -1

    o1.jobFinishedWorking.compareTo(o2.jobFinishedWorking)
  }

  /**
    * Checks to see if there is currently a job in this scheduler's job queue.
    * If there is, and this scheduler is not currently scheduling a job, then
    * pop that job off of the queue and "begin scheduling it". Scheduling a
    * job consists of setting this scheduler's state to scheduling = true, and
    * adding a finishSchedulingJobAction to the simulators event queue by
    * calling afterDelay().
    */
  def scheduleNextJob(): Unit = {
    if (!scheduling) {
      val jobCompleted: ListBuffer[Job] = new ListBuffer[Job]()
      pendingQueueAsList.foreach(job => {
        if (job != null && job.finalStatus == JobStates.Completed){
          jobCompleted += job
        }
      })
      jobCompleted.foreach(job => {
        // Remove the Job from the pending queue and running queue, if it was present.
        removePendingJob(job)
        removeRunningJob(job)
      })
      if (numJobsInQueue > 0) {
        scheduling = true

        var jobsToAttemptScheduling: ListBuffer[Job] = getJobs(pendingQueueAsList, simulator.currentTime)
        if(policyMode == PolicyModes.Fifo || policyMode == PolicyModes.hFifo)
          assert(jobsToAttemptScheduling.length == 1,
            "For Fifo like policy the jobsToAttemptScheduling length must be 1 (%d)".format(jobsToAttemptScheduling.length))
        if(PolicyModes.myPolicies.contains(policyMode)){
          if (jobsToAttemptScheduling.size > 1)
            jobsToAttemptScheduling = jobsToAttemptScheduling.sortWith(_.numTasks > _.numTasks)
        }

//        if (previousJob != null && previousJob == jobsToAttemptScheduling.head) {
//          jobAttempt += 1
//          if(jobAttempt == 2) {
//            jobAttempt = 0
//            scheduling = false
//            jobsToAttemptScheduling.foreach(job => {
//              addPendingJob(job)
//            })
//            simulator.logger.info(schedulerPrefix + " Exiting because we are trying to schedule the same job that failed before.")
//            return
//          }
//        }
//        previousJob = jobsToAttemptScheduling.head

        totalQueueSize += numJobsInQueue
        numSchedulingCalls += 1

        syncCellState()
        simulator.logger.info(schedulerPrefix + " The cell state is (%f cpus, %f mem)".format(privateCellState.availableCpus, privateCellState.availableMem))

        var jobThinkTime: Double = 0
        var allJobPrefix = "[Job"
        jobsToAttemptScheduling.foreach(job => {
          jobThinkTime += getThinkTime(job)
          allJobPrefix += " %d (%s)".format(job.id, job.workloadName)
        })
        allJobPrefix += "] "
        val elasticPrefix = "[Elastic]"

        simulator.logger.info(schedulerPrefix + allJobPrefix + "Started %f seconds of scheduling thinktime."
          .format(jobThinkTime))
        simulator.afterDelay(jobThinkTime) {
          simulator.logger.info((schedulerPrefix + allJobPrefix + "Finished %f seconds of scheduling " +
            "thinktime.").format(jobThinkTime))


          /*
           * Let's perform a simulation of the jobs, with the same priority, that could be allocated on the cluster
           */
          val jobsToLaunch: ListBuffer[(Job, ListBuffer[ClaimDelta], ListBuffer[ClaimDelta])] = new ListBuffer[(Job, ListBuffer[ClaimDelta], ListBuffer[ClaimDelta])]()
          val tmpJobsToLaunch: ListBuffer[(Job, ListBuffer[ClaimDelta], ListBuffer[ClaimDelta])] = new ListBuffer[(Job, ListBuffer[ClaimDelta], ListBuffer[ClaimDelta])]()
          val jobsCannotFit: ListBuffer[Job] = new ListBuffer[Job]()
          var stop:Boolean = false
          var clusterFreeResources: Double = privateCellState.availableCpus * privateCellState.availableMem

          val jobsToPreempt: ListBuffer[JobPreemption] = new ListBuffer[JobPreemption]()

          jobsToAttemptScheduling.foreach(job => {
            val jobPrefix = "[Job %d (%s)] ".format(job.id, job.workloadName)
            var claimDelta_inelastic = new ListBuffer[ClaimDelta]()
            var claimDelta_elastic = new ListBuffer[ClaimDelta]()

            job.numSchedulingAttempts += 1

            if(PolicyModes.myPolicies.contains(policyMode)){
              if(!stop){
                var jobsThatCanBePreempted: ListBuffer[JobPreemption] = new ListBuffer[JobPreemption]()
                for(job1: Job <- PolicyModes.getJobsWithLowerPriority(runningQueueAsList, policyMode, job, simulator.currentTime, threshold = 1).reverse){
                  if(job1.finalStatus != JobStates.Completed /*&& job1.scheduledElasticTasks > 0*/){
                    var inserted: Boolean = false
                    for(jobToPreempt: JobPreemption <- jobsToPreempt if !inserted){
                      if(jobToPreempt.job.id == job1.id && (jobToPreempt.elasticDeltasAfterPreemption.nonEmpty || jobToPreempt.inelasticDeltasAfterPreemption.nonEmpty)){
                        jobsThatCanBePreempted += jobToPreempt
                        inserted = true
                      }
                    }
                    if(!inserted){
                      jobsThatCanBePreempted += JobPreemption(job1, job1.claimInelasticDeltas, job1.claimElasticDeltas)
                    }
                  }
                }


                tmpJobsToLaunch.clear()
                jobsToLaunch.foreach { case (job1, inelastic, elastic) =>
                  var tmpClaimDelta_inelastic = new ListBuffer[ClaimDelta]()
                  var tmpClaimDelta_elastic = new ListBuffer[ClaimDelta]()
                  elastic.foreach(claimDelta => {
                    tmpClaimDelta_elastic += claimDelta
                  })
                  inelastic.foreach(claimDelta => {
                    tmpClaimDelta_inelastic += claimDelta
                  })
                  tmpJobsToLaunch += ((job1, tmpClaimDelta_inelastic, tmpClaimDelta_elastic))
                }

                jobsToLaunch.foreach { case (job1, inelastic, elastic) =>
                  elastic.foreach(claimDelta => {
                    claimDelta.unApply(privateCellState)
                  })
                  elastic.clear()
                }
                val isJobRunning = runningQueueAsList.contains(job)
                if(!isJobRunning) {
                  claimDelta_inelastic = scheduleJob(job, privateCellState)
                  if (!isAllocationSuccessfully(claimDelta_inelastic, job)) {
                    simulator.logger.info(schedulerPrefix + jobPrefix + " Failed to allocate all inelastic services (%d/%d[%d])".format(claimDelta_inelastic.size, job.moldableTasks, job.unscheduledTasks))
                    claimDelta_inelastic.foreach(claimDelta => {
                      claimDelta.unApply(privateCellState)
                    })
                    claimDelta_inelastic.clear()
                    if (!runningQueueAsList.contains(job) && jobsThatCanBePreempted.nonEmpty) {
                      simulator.logger.info(schedulerPrefix + jobPrefix + " Checking for preemptive resources for inelastic services.")
                      claimDelta_inelastic = preemptionForInelastic(job, jobsThatCanBePreempted, jobsToPreempt)
                    }
                  } else {
                    simulator.logger.info(schedulerPrefix + jobPrefix + " Successfully allocated all inelastic services (%d/%d[%d])".format(claimDelta_inelastic.size, job.moldableTasks, job.unscheduledTasks))
                  }
                }
                if (claimDelta_inelastic.nonEmpty || isJobRunning) {
                  jobsToLaunch += ((job, claimDelta_inelastic, claimDelta_elastic))
                }
                jobsToLaunch.foreach { case (job1, inelastic, elastic) =>
                  claimDelta_elastic = scheduleJob(job1, privateCellState, elastic = true)
                  if (job1.elasticTasksUnscheduled > 0
                    && (claimDelta_elastic.isEmpty || claimDelta_elastic.size < job1.elasticTasksUnscheduled)
                    && jobsThatCanBePreempted.nonEmpty
                    && !job1.workloadName.equals("Interactive")) {
                    simulator.logger.info(schedulerPrefix + jobPrefix +
                      "[%d (%s)] Checking for preemptive resources for elastic services. (%d/%d)".format(
                        job1.id, job1.workloadName, job1.elasticTasksUnscheduled, job1.elasticTasks))
                    claimDelta_elastic = preemptionForElastic(job1, jobsThatCanBePreempted, jobsToPreempt)
                  }
                  elastic.clear()
                  elastic ++= claimDelta_elastic
                }

                val currentFreeResource: Double = privateCellState.availableCpus * privateCellState.availableMem
                if(currentFreeResource >= clusterFreeResources && jobsToPreempt.isEmpty){
                  stop = true
                  jobsToLaunch.clear()
                  jobsToLaunch ++= tmpJobsToLaunch
                }
                clusterFreeResources = currentFreeResource
              }
            }else{
              claimDelta_inelastic = scheduleJob(job, privateCellState)
              if (!isAllocationSuccessfully(claimDelta_inelastic, job)) {
                claimDelta_inelastic.foreach(claimDelta => {
                  claimDelta.unApply(privateCellState)
                })
              }else {
                claimDelta_elastic = scheduleJob(job, privateCellState, elastic = true)
                if(claimDelta_elastic.size == job.elasticTasks){
                  jobsToLaunch += ((job, claimDelta_inelastic, claimDelta_elastic))
                }else{
                  simulator.logger.info(schedulerPrefix + jobPrefix + " Not all services scheduled (%d/%d). Rolling back. The cellstate had (%f cpus, %f mem) free."
                    .format(claimDelta_elastic.size, job.elasticTasks, privateCellState.availableCpus, privateCellState.availableMem))
                  simulator.logger.info(schedulerPrefix + jobPrefix + " Total resources requested: (%f cpus, %f mem)"
                    .format(job.cpusPerTask * job.elasticTasks, job.memPerTask * job.elasticTasks))
                  claimDelta_elastic.foreach(claimDelta => {
                    claimDelta.unApply(privateCellState)
                  })
                  simulator.logger.info(schedulerPrefix + jobPrefix + " The cellstate now have (%f cpu, %f mem) free."
                    .format(privateCellState.availableCpus, privateCellState.availableMem))

                  val taskCanFitPerCpus = Math.floor(privateCellState.cpusPerMachine / job.cpusPerTask) * privateCellState.numMachines
                  val taskCanFitPerMem = Math.floor(privateCellState.memPerMachine / job.memPerTask) * privateCellState.numMachines
                  if(taskCanFitPerCpus < job.numTasks || taskCanFitPerMem < job.numTasks) {
                    simulator.logger.warn((schedulerPrefix + jobPrefix + " The cell (%f cpus, %f mem) is not big enough " +
                      "to hold this job all at once which requires %d tasks for %f cpus " +
                      "and %f mem in total.").format(privateCellState.totalCpus,
                      privateCellState.totalMem,
                      job.numTasks,
                      job.cpusPerTask * job.numTasks,
                      job.memPerTask * job.numTasks))
                    jobsCannotFit += job
                  }
                }
              }
            }
          })
          jobsCannotFit.foreach(job => {
            jobsToAttemptScheduling -= job
          })

//          if(jobsToPreempt.nonEmpty && jobsToLaunch.nonEmpty){
//            var totalQueueGain: Double = 0
//            var totalExecutionLoss: Double = 0
//            jobsToPreempt.foreach(jobToPreempt => {
//              // The following control should be not necessary, but I had some jobToPreempt with no service to preempt
//              if(jobToPreempt.elasticDeltasToPreempt.nonEmpty){
//                val job: Job = jobToPreempt.job
//                val previousElasticTasksUnscheduled = job.elasticTasksUnscheduled
//
//                job.elasticTasksUnscheduled = job.elasticTasksUnscheduled + jobToPreempt.elasticDeltasToPreempt.size
//                val jobLeftDuration: Double = job.estimateJobDuration(currTime = simulator.currentTime, tasksRemoved = jobToPreempt.elasticDeltasToPreempt.size, mock = true)
//                job.elasticTasksUnscheduled = previousElasticTasksUnscheduled
//
//                val previousDuration: Double = job.jobFinishedWorking - simulator.currentTime
//                assert(jobLeftDuration > previousDuration, "A job cannot go faster after preemption!(%f | %f)".format(jobLeftDuration, previousDuration))
//
//                totalExecutionLoss += jobLeftDuration - previousDuration
//              }
//            })
//            totalExecutionLoss /= jobsToPreempt.size
//
//            val jobsAlreadyCounted: ListBuffer[Job] = new ListBuffer[Job]()
//            jobsToLaunch.foreach{ case (job, inelastic, elastic) =>
//              val memNeeded: Double = (inelastic.size + elastic.size) * job.memPerTask
//              val cpusNeeded: Double = (inelastic.size + elastic.size) * job.cpusPerTask
//
//              var memFree: Double = 0
//              var cpusFree: Double = 0
//
//              val runningJobSortedPerFinishTime = runningJobs.sortWith(compareJobFinishTime(_,_) < 0)
//              var finish: Boolean = false
//              for (elem <- runningJobSortedPerFinishTime if !finish) {
//                if(elem != null && !jobsAlreadyCounted.contains(elem) && elem.finalStatus != JobStates.Completed){
//                  val deltaSize = elem.allClaimDeltas.size
//                  memFree += deltaSize * job.memPerTask
//                  cpusFree += deltaSize * job.cpusPerTask
//                  if(memFree >= memNeeded && cpusFree >= cpusNeeded){
//                    totalQueueGain += (elem.jobFinishedWorking - simulator.currentTime)
//                    jobsAlreadyCounted += elem
//                    finish = true
//                  }
//                }
//              }
//            }
//            totalQueueGain /= jobsToLaunch.size
//
//            if(totalQueueGain <= totalExecutionLoss){
//              simulator.logger.warn(schedulerPrefix + allJobPrefix + "It is not worth applying preemption. totalQueueGain: %f | totalExecutionLoss: %f".format(totalQueueGain, totalExecutionLoss))
//              jobsToPreempt.clear()
//              jobsToLaunch.clear()
//            }
//            simulator.logger.info(schedulerPrefix + allJobPrefix + "It is worth applying preemption. totalQueueGain: %f | totalExecutionLoss: %f".format(totalQueueGain, totalExecutionLoss))
//          }

          simulator.logger.info(schedulerPrefix + allJobPrefix + "There are %d jobs that can be preempted.".format(jobsToPreempt.size))
          jobsToPreempt.foreach(jobToPreempt => {
            val job: Job = jobToPreempt.job
            val jobPrefix = "[Job %d (%s)] ".format(job.id, job.workloadName)

            val elasticClaimDeltasToPreempt: ListBuffer[ClaimDelta] = jobToPreempt.elasticDeltasToPreempt.sortWith(compareMachineSeqNum(_,_) < 0)
            if(jobToPreempt.elasticDeltasToPreempt.nonEmpty){
              elasticClaimDeltasToPreempt.foreach(claimDelta => {
                claimDelta.unApply(simulator.cellState)
              })
              job.claimElasticDeltas --= elasticClaimDeltasToPreempt
              // We have to reinsert the job in queue if this had all it's elastic services allocated
              if(job.elasticTasksUnscheduled == 0){
                addPendingJob(job, prepend = true)
              }
              job.elasticTasksUnscheduled = job.elasticTasksUnscheduled + elasticClaimDeltasToPreempt.size
              simulator.logger.info(schedulerPrefix + jobPrefix + "Preempted %d elastic services.".format(elasticClaimDeltasToPreempt.size))
            }

//            val inelasticClaimDeltasToPreempt: ListBuffer[ClaimDelta] = jobToPreempt.inelasticDeltasToPreempt.sortWith(compareMachineSeqNum(_,_) < 0)
//            if(jobToPreempt.inelasticDeltasToPreempt.nonEmpty){
//              inelasticClaimDeltasToPreempt.foreach(claimDelta => {
//                claimDelta.unApply(simulator.cellState)
//              })
//              job.reset()
//              // We have to reinsert the job in queue if this had all it's elastic services allocated
//              removeRunningJob(job)
//              if(!pendingQueueAsList.contains(job))
//                addPendingJob(job)
//              simulator.logger.info(schedulerPrefix + jobPrefix + "Preempted %d inelastic services.".format(inelasticClaimDeltasToPreempt.size))
//            }

//            if(jobToPreempt.inelasticDeltasToPreempt.nonEmpty) {
//              // We have to remove all the incoming simulation events that work on this job.
//              simulator.removeIf(x => x.itemId == job.id &&
//                (x.eventType == EventTypes.Remove || x.eventType == EventTypes.Trigger))
//            }else
            if(jobToPreempt.elasticDeltasToPreempt.nonEmpty){
              val jobLeftDuration: Double = job.estimateJobDuration(currTime = simulator.currentTime, tasksRemoved = elasticClaimDeltasToPreempt.size)

              job.jobFinishedWorking = simulator.currentTime + jobLeftDuration

              // We have to remove all the incoming simulation events that work on this job.
              simulator.removeIf(x => x.itemId == job.id &&
                (x.eventType == EventTypes.Remove || x.eventType == EventTypes.Trigger))

              simulator.afterDelay(jobLeftDuration, eventType = EventTypes.Remove, itemId = job.id) {
                simulator.logger.info(schedulerPrefix + jobPrefix + "Completed after %fs.".format(simulator.currentTime - job.jobStartedWorking))
                job.finalStatus = JobStates.Completed
                previousJob = null
                removePendingJob(job)
                removeRunningJob(job)
              }
              simulator.logger.info(schedulerPrefix + elasticPrefix + jobPrefix + "Adding finished event after %f seconds to wake up scheduler.".format(jobLeftDuration))
              simulator.cellState.scheduleEndEvents(job.allClaimDeltas, delay = jobLeftDuration, jobId = job.id)
            }
          })


          /*
           * After the simulation, we will deploy the allocations on the real cluster
           */
          simulator.logger.info(schedulerPrefix + allJobPrefix + "There are %d jobs that can be allocated.".format(jobsToLaunch.size))
          var serviceDeployed = 0
          jobsToLaunch.foreach { case (job, inelastic, elastic) =>
            val jobPrefix = "[Job %d (%s)] ".format(job.id, job.workloadName)
            var inelasticTasksUnscheduled: Int = job.unscheduledTasks
            var elasticTasksUnscheduled: Int = job.elasticTasksUnscheduled

            if (!runningQueueAsList.contains(job)) {
//              job.numTaskSchedulingAttempts += inelasticTasksUnscheduled
              if (inelastic.nonEmpty) {
                val commitResult = simulator.cellState.commit(inelastic)
                if (commitResult.committedDeltas.nonEmpty) {
//                  recordUsefulTimeScheduling(job, getThinkTime(job), job.numSchedulingAttempts == 1)
                  serviceDeployed += commitResult.committedDeltas.size

                  inelasticTasksUnscheduled -= commitResult.committedDeltas.size
                  job.claimInelasticDeltas ++= commitResult.committedDeltas
                  job.unscheduledTasks = inelasticTasksUnscheduled
//                  numSuccessfulTransactions += 1
                  job.finalStatus = JobStates.Partially_Scheduled

                  if (job.firstScheduled) {
                    job.timeInQueueTillFirstScheduled = simulator.currentTime - job.submitted
                    job.firstScheduled = false
                  }

                  simulator.logger.info(schedulerPrefix + jobPrefix + "Scheduled %d tasks, %d remaining."
                    .format(inelastic.size, job.unscheduledTasks))
                } else {
//                  numFailedTransactions += 1
                  simulator.logger.info(schedulerPrefix + jobPrefix + "There was a conflict when committing the task allocation to the real cell.")
//                  recordWastedTimeScheduling(job, getThinkTime(job), job.numSchedulingAttempts == 1)
                }
              } else {
//                recordWastedTimeScheduling(job, getThinkTime(job), job.numSchedulingAttempts == 1)
//                numNoResourcesFoundSchedulingAttempts += 1

                simulator.logger.info((schedulerPrefix + jobPrefix + "No tasks scheduled (%f cpu %f mem per task) " +
                  "during this scheduling attempt, recording wasted time.")
                  .format(job.cpusPerTask,
                    job.memPerTask))
              }
              if (inelasticTasksUnscheduled == 0) {
                val jobDuration: Double = job.estimateJobDuration()
                job.jobStartedWorking = simulator.currentTime
                job.jobFinishedWorking = simulator.currentTime + jobDuration

                simulator.afterDelay(jobDuration, eventType = EventTypes.Remove, itemId = job.id) {
                  simulator.logger.info(schedulerPrefix + jobPrefix + "Completed after %fs.".format(simulator.currentTime - job.jobStartedWorking))
                  job.finalStatus = JobStates.Completed
                  previousJob = null
                  removePendingJob(job)
                  removeRunningJob(job)
                }
                simulator.logger.info(schedulerPrefix + jobPrefix + "Adding finished event after %f seconds to wake up scheduler.".format(jobDuration))
                simulator.cellState.scheduleEndEvents(job.claimInelasticDeltas, delay = jobDuration, jobId = job.id)

                // All tasks in job scheduled so don't put it back in pendingQueueAsList.
                job.finalStatus = JobStates.Fully_Scheduled
                job.timeInQueueTillFullyScheduled = simulator.currentTime - job.submitted

                addRunningJob(job)

                simulator.logger.info((schedulerPrefix + jobPrefix + "Fully-Scheduled (%f cpu %f mem per task), " +
                  "after %d scheduling attempts.").format(job.cpusPerTask,
                  job.memPerTask,
                  job.numSchedulingAttempts))
              } else {
                simulator.logger.info((schedulerPrefix + jobPrefix + "Not fully scheduled, %d / %d tasks remain " +
                  "(shape: %f cpus, %f mem per task). Leaving it " +
                  "in the queue.").format(inelasticTasksUnscheduled,
                  job.moldableTasks,
                  job.cpusPerTask,
                  job.memPerTask))
              }
            }

            if (job.finalStatus == JobStates.Completed) {
              simulator.logger.info(schedulerPrefix + elasticPrefix + jobPrefix + "Finished during the thinking time. " +
                "Do not process it.")
            } else if (elasticTasksUnscheduled > 0){
              var elasticTasksLaunched = 0
              if (elastic.nonEmpty) {
                val commitResult = simulator.cellState.commit(elastic)
                if (commitResult.committedDeltas.nonEmpty) {
//                  recordUsefulTimeScheduling(job, getThinkTime(job), job.numSchedulingAttempts == 1)
                  serviceDeployed += commitResult.committedDeltas.size

                  elasticTasksLaunched = commitResult.committedDeltas.size
                  elasticTasksUnscheduled -= elasticTasksLaunched
                  job.claimElasticDeltas ++= commitResult.committedDeltas
                  job.elasticTasksUnscheduled = elasticTasksUnscheduled
//                  numSuccessfulTransactions += 1

                  simulator.logger.info(schedulerPrefix + elasticPrefix + jobPrefix + "Scheduled %d tasks, %d remaining."
                    .format(elasticTasksLaunched, job.elasticTasksUnscheduled))
                } else {
//                  numFailedTransactions += 1
                  simulator.logger.info(schedulerPrefix + elasticPrefix + jobPrefix + "There was a conflict when committing the task allocation to the real cell.")
//                  recordWastedTimeScheduling(job, getThinkTime(job), job.numSchedulingAttempts == 1)
                }

              } else if (job.elasticTasks > 0){
//                recordWastedTimeScheduling(job, getThinkTime(job), job.numSchedulingAttempts == 1)
//                numNoResourcesFoundSchedulingAttempts += 1

                simulator.logger.info((schedulerPrefix + elasticPrefix + jobPrefix + "No tasks scheduled (%f cpu %f mem per task) " +
                  "during this scheduling attempt, recording " +
                  "wasted time. %d unscheduled tasks remaining.")
                  .format(job.cpusPerTask,
                    job.memPerTask,
                    elasticTasksUnscheduled))
              }
              if (elasticTasksLaunched > 0) {
                val jobLeftDuration: Double = job.estimateJobDuration(currTime = simulator.currentTime, newTasksAllocated = elasticTasksLaunched)

                job.jobFinishedWorking = simulator.currentTime + jobLeftDuration

                // We have to remove all the incoming simulation events that work on this job.
                simulator.removeIf(x => x.itemId == job.id &&
                  (x.eventType == EventTypes.Remove || x.eventType == EventTypes.Trigger))

                simulator.afterDelay(jobLeftDuration, eventType = EventTypes.Remove, itemId = job.id) {
                  simulator.logger.info(schedulerPrefix + jobPrefix + "Completed after %fs.".format(simulator.currentTime - job.jobStartedWorking))
                  job.finalStatus = JobStates.Completed
                  previousJob = null
                  removePendingJob(job)
                  removeRunningJob(job)
                }
                simulator.logger.info(schedulerPrefix + elasticPrefix + jobPrefix + "Adding finished event after %f seconds to wake up scheduler.".format(jobLeftDuration))
                simulator.cellState.scheduleEndEvents(job.allClaimDeltas, delay = jobLeftDuration, jobId = job.id)
              }
            }

            if (job.finalStatus == JobStates.Completed || (inelasticTasksUnscheduled == 0 && elasticTasksUnscheduled == 0)) {
              jobsToAttemptScheduling -= job
            }
          }

          jobsToAttemptScheduling.foreach(job => {
            addPendingJob(job)
          })

          scheduling = false
          if(serviceDeployed == 0) {
            jobAttempt += 1
          }
          if(jobAttempt == 2) {
            jobAttempt = 0
            simulator.logger.info(schedulerPrefix + " Exiting because we could not schedule the job. This means that not enough resources were available.")
          }else
            scheduleNextJob()
        }
      }
    }
  }
}

case class JobPreemption(job: Job,
                         inelasticDeltas: ListBuffer[ClaimDelta],
                        elasticDeltas: ListBuffer[ClaimDelta]) {

  override def equals(that: Any): Boolean =
    that match {
      case that: JobPreemption => that.job.id == this.job.id
      case _ => false
    }

  val elasticDeltasAfterPreemption = new ListBuffer[ClaimDelta]()
  elasticDeltasAfterPreemption ++= elasticDeltas

  val elasticDeltasToPreempt = new ListBuffer[ClaimDelta]()

  val inelasticDeltasAfterPreemption = new ListBuffer[ClaimDelta]()
  inelasticDeltasAfterPreemption ++= inelasticDeltas

  val inelasticDeltasToPreempt = new ListBuffer[ClaimDelta]()

  def reset(): Unit = {
    elasticDeltasAfterPreemption.clear()
    elasticDeltasAfterPreemption ++= elasticDeltas

    elasticDeltasToPreempt.clear()

    inelasticDeltasAfterPreemption.clear()
    inelasticDeltasAfterPreemption ++= inelasticDeltas

    inelasticDeltasToPreempt.clear()
  }

  def copy(): JobPreemption = {
    val newJobPreemption = JobPreemption(job, inelasticDeltas, elasticDeltas)
    newJobPreemption.elasticDeltasAfterPreemption.clear()
    newJobPreemption.elasticDeltasAfterPreemption ++= elasticDeltasAfterPreemption

    newJobPreemption.elasticDeltasToPreempt.clear()
    newJobPreemption.elasticDeltasToPreempt ++= elasticDeltasToPreempt

    newJobPreemption.inelasticDeltasAfterPreemption.clear()
    newJobPreemption.inelasticDeltasAfterPreemption ++= inelasticDeltasAfterPreemption

    newJobPreemption.inelasticDeltasToPreempt.clear()
    newJobPreemption.inelasticDeltasToPreempt ++= inelasticDeltasToPreempt

    newJobPreemption
  }
}
