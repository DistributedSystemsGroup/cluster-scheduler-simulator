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

import org.apache.log4j.{Level, Logger}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random


object EventTypes extends Enumeration {
  val Remove, Trigger = Value
}

/**
 * A simple, generic, discrete event simulator. A modified version of the
 * basic discrete event simulator code from Programming In Scala, pg 398,
 * http://www.amazon.com/Programming-Scala-Comprehensive-Step-step/dp/0981531601
 */
abstract class Simulator(logging: Boolean = false){
  val logger = Logger.getLogger(this.getClass.getName)
  type Action = () => Unit

  if(!logging)
    logger.setLevel(Level.WARN)

  case class WorkItem(time: Double, action: Action, eventType: EventTypes.Value, itemId: Long)
  implicit object WorkItemOrdering extends Ordering[WorkItem] {
    /** Returns an integer whose sign communicates how x compares to y.
      *
      * The result sign has the following meaning:
      *
      *  - negative if x < y
      *  - positive if x > y
      *  - zero otherwise (if x == y)
      */
    def compare(a: WorkItem, b: WorkItem): Int = {
//      if (a.time < b.time) -1
//      else if (a.time > b.time) 1
//      else 0
      a.time.compare(b.time)
    }
  }

  protected var curtime: Double = 0.0 // simulation time, in seconds
  private[this] var _agenda = new collection.mutable.ListBuffer[WorkItem]()

  protected def agenda = _agenda

  def currentTime: Double = curtime

  def afterDelay(delay: Double, eventType: EventTypes.Value = null, itemId: Long = 0)(block: => Unit) {
    logger.trace("afterDelay() called. delay is %f.".format(delay))
    val item = WorkItem(currentTime + delay, () => block, eventType, itemId)
    _agenda += item
   _agenda = _agenda.sorted
    // log("inserted new WorkItem into agenda to run at time " + (currentTime + delay))
  }

  def removeIf(p: WorkItem => Boolean): Unit ={
    _agenda --= _agenda.filter(p)
  }

  def deqeue(): WorkItem = {
    val item = _agenda.head
    _agenda -= item
    item
  }

  var eventId:Long = 0
  private def next(): Double = {
    eventId += 1
    logger.debug("Processing event number %d".format(eventId))
    val item = deqeue()
    curtime = item.time
    if (curtime < 0)
      curtime = 0
    item.action()
    curtime
  }

  /**
   * Run the simulation for `@code runTime` virtual (i.e., simulated)
   * seconds or until `@code wallClockTimeout` seconds of execution
   * time elapses.
    *
    * @return Pair (boolean, double)
    *         1. true if simulation ran till runTime or completion, and false
    *         if simulation timed out.
    *         2. the simulation total time
   */
  def run(runTime:Option[Double] = None,
          wallClockTimeout:Option[Double] = None): (Boolean, Double) = {
    
    afterDelay(-1) {
      logger.info("*** Simulation started, time = "+ currentTime +". ***")
    }
    // Record wall clock time at beginning of simulation run.
    val startWallTime = System.currentTimeMillis()   
    def timedOut: Boolean = {
      val elapsedTime = (System.currentTimeMillis() - startWallTime) / 1000.0
      if (wallClockTimeout.exists(elapsedTime > _ )) {
        logger.info("Execution timed out after %f seconds, ending simulation now.".format(elapsedTime))
        true
      } else {
        false
      }
    }

    def run_(time: Double): Double = {
      if (_agenda.isEmpty || runTime.exists(_ <= time) || timedOut){
        logger.trace("run_() called. agenda.isEmpty = %s , runTime.exists(_ > time) = %s , timedOut = %s"
          .format(_agenda.isEmpty, runTime.exists(_ > time), timedOut))
        time
      } else
          run_(next())
    }

    val totalTime = run_(currentTime)
    logger.info("*** Simulation finished running, time = "+ totalTime +". ***")
    (!timedOut, totalTime)
  }
}

object AllocationModes extends Enumeration {
  val Incremental, All = Value
}

abstract class ClusterSimulatorDesc(val runTime: Double) {
  def newSimulator(constantThinkTime: Double,
                   perTaskThinkTime: Double,
                   blackListPercent: Double,
                   schedulerWorkloadsToSweepOver: Map[String, Seq[String]],
                   workloadToSchedulerMap: Map[String, Seq[String]],
                   cellStateDesc: CellStateDesc,
                   workloads: Seq[Workload],
                   prefillWorkloads: Seq[Workload],
                   logging: Boolean = false): ClusterSimulator
}

class PrefillScheduler(cellState: CellState)
  extends Scheduler(name = "prefillScheduler",
    constantThinkTimes = Map[String, Double](),
    perTaskThinkTimes = Map[String, Double](),
    numMachinesToBlackList = 0,
    allocationMode = AllocationModes.Incremental) {


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

/**
 * A simulator to compare different cluster scheduling architectures
 * (single agent, dynamically partitioned,and replicated state), based
 * on a set of input parameters that define the schedulers being used
 * and the workload being played.
  *
  * @param schedulers A Map from schedulerName to Scheduler, should
 *       exactly one entry for each scheduler that is registered with
 *       this simulator.
 * @param workloadToSchedulerMap A map from workloadName to Seq[SchedulerName].
 *       Used to determine which scheduler each job is assigned.
 */
class ClusterSimulator(val cellState: CellState,
                       val schedulers: Map[String, Scheduler],
                       val workloadToSchedulerMap: Map[String, Seq[String]],
                       val workloads: Seq[Workload],
                       prefillWorkloads: Seq[Workload],
                       logging: Boolean = false,
                       monitorUtilization: Boolean = true,
                       monitoringPeriod: Double = 1.0,
                       var prefillScheduler: PrefillScheduler = null)
                      extends Simulator(logging) {
  assert(schedulers.nonEmpty, "At least one scheduler must be provided to" +
                              "scheduler constructor.")
  assert(workloadToSchedulerMap.nonEmpty, "No workload->scheduler map setup.")

  workloadToSchedulerMap.values.flatten.foreach(schedulerName => assert(schedulers.contains(schedulerName)
                                                                   ,("Workload-Scheduler map points to a scheduler, " +
                                                                     "%s, that is not registered").format(schedulerName)))

  // Set up a pointer to this simulator in the cellstate.
  cellState.simulator = this
  // Set up a pointer to this simulator in each scheduler.
  schedulers.values.foreach(_.simulator = this)


  if(prefillScheduler == null)
    prefillScheduler =  new PrefillScheduler(cellState)
  prefillScheduler.simulator = this
  prefillScheduler.scheduleWorkloads(prefillWorkloads)

  // Set up workloads
  workloads.foreach(workload => {
    var jobsToRemove: ListBuffer[Job] = ListBuffer[Job]()
    var numSkipped, numLoaded = 0
    workload.getJobs.foreach(job => {
      val scheduler = getSchedulerForWorkloadName(job.workloadName)
      if (scheduler.isEmpty) {
        logger.warn(("Skipping a job fom a workload type (%s) that has " +
            "not been mapped to any registered schedulers. Please update " +
            "a mapping for this scheduler via the workloadSchedulerMap param.")
            .format(job.workloadName))
        jobsToRemove += job
        numSkipped += 1
      } else {
        // Schedule the task to get submitted to its scheduler at its
        // submission time.

        // assert(job.cpusPerTask * job.numTasks <= cellState.totalCpus + 0.000001 &&
        //        job.memPerTask * job.numTasks <= cellState.totalMem + 0.000001,
        //        ("The cell (%f cpus, %f mem) is not big enough to hold job %d " +
        //        "all at once which requires %f cpus and %f mem in total.")
        //        .format(cellState.totalCpus,
        //                cellState.totalMem,
        //                job.id,
        //                job.cpusPerTask * job.numTasks,
        //                job.memPerTask * job.numTasks))
//        if (job.cpusPerTask * job.numTasks > cellState.totalCpus ||
//            job.memPerTask * job.numTasks > cellState.totalMem){
        val taskCanFitPerCpus = Math.floor(cellState.cpusPerMachine / job.cpusPerTask) * cellState.numMachines
        val taskCanFitPerMem = Math.floor(cellState.memPerMachine / job.memPerTask) * cellState.numMachines
        if(taskCanFitPerCpus < job.numTasks || taskCanFitPerMem < job.numTasks) {
//        if(taskCanFitPerCpus < job.moldableTasks || taskCanFitPerMem < job.moldableTasks) {
          logger.warn(("The cell (%f cpus, %f mem) is not big enough " +
                  "to hold job id %d all at once which requires %d tasks for %f cpus " +
                  "and %f mem in total.").format(cellState.totalCpus,
                                                 cellState.totalMem,
                                                 job.id,
                                                 job.numTasks,
                                                 job.cpusPerTask * job.numTasks,
                                                 job.memPerTask * job.numTasks))
          numSkipped += 1
          jobsToRemove += job
        } else {
          afterDelay(job.submitted - currentTime) {
            scheduler.foreach(_.addJob(job))
          }
          numLoaded += 1
        }

      }
    })
    if(numSkipped > 0){
      logger.warn("Loaded %d jobs from workload %s, and skipped %d.".format(
        numLoaded, workload.name, numSkipped))
      workload.removeJobs(jobsToRemove)
    }

  })
  var roundRobinCounter = 0
  // If more than one scheduler is assigned a workload, round robin across them.
  def getSchedulerForWorkloadName(workloadName: String):Option[Scheduler] = {
    workloadToSchedulerMap.get(workloadName).map(schedulerNames => {
      // println("schedulerNames is %s".format(schedulerNames.mkString(" ")))
      roundRobinCounter += 1
      val name = schedulerNames(roundRobinCounter % schedulerNames.length)
      // println("Assigning job from workload %s to scheduler %s"
      //         .format(workloadName, name))
      schedulers(name)
    })
  }
  // Track utilization due to resources actually being accepted by a
  // framework/scheduler. This does not include the time resources spend
  // tied up while they are pessimistically locked (e.g. while they are
  // offered as part of a Mesos resource-offer). That type of utilization
  // is tracked separately below.
  def avgCpuUtilization: Double = sumCpuUtilization / numMonitoringMeasurements
  def avgMemUtilization: Double = sumMemUtilization / numMonitoringMeasurements
  // Track "utilization" of resources due to their being pessimistically locked
  // (i.e. while they are offered as part of a Mesos resource-offer).
  def avgCpuLocked: Double = sumCpuLocked / numMonitoringMeasurements
  def avgMemLocked: Double = sumMemLocked / numMonitoringMeasurements
  val cpuUtilization: ListBuffer[Double] = ListBuffer()
  val memUtilization: ListBuffer[Double] = ListBuffer()
  var sumCpuUtilization: Double = 0.0
  var sumMemUtilization: Double = 0.0
  var sumCpuLocked: Double = 0.0
  var sumMemLocked: Double = 0.0
  var numMonitoringMeasurements: Long = 0

  val pendingQueueStatus: scala.collection.mutable.Map[String, ListBuffer[Long]] = scala.collection.mutable.Map[String, ListBuffer[Long]]()
  val runningQueueStatus: scala.collection.mutable.Map[String, ListBuffer[Long]] = scala.collection.mutable.Map[String, ListBuffer[Long]]()

  schedulers.values.foreach(scheduler => {
    pendingQueueStatus.put(scheduler.name, ListBuffer[Long]())
    runningQueueStatus.put(scheduler.name, ListBuffer[Long]())
  })

  def measureUtilization(): Unit = {
    numMonitoringMeasurements += 1
    sumCpuUtilization += (cellState.totalOccupiedCpus / cellState.totalCpus)
    cpuUtilization += (cellState.totalOccupiedCpus / cellState.totalCpus)
    memUtilization += (cellState.totalOccupiedMem / cellState.totalMem)
    sumMemUtilization += (cellState.totalOccupiedMem / cellState.totalMem)
    sumCpuLocked += cellState.totalLockedCpus
    sumMemLocked += cellState.totalLockedMem
    logger.debug("Adding measurement %d. Avg cpu: %f. Avg mem: %f"
        .format(numMonitoringMeasurements,
          avgCpuUtilization, avgMemUtilization))

    schedulers.values.foreach(scheduler => {
      pendingQueueStatus(scheduler.name) += scheduler.jobQueueSize
      runningQueueStatus(scheduler.name) += scheduler.runningJobQueueSize
    })


    //Temporary: print utilization throughout the day.
    // if (numMonitoringMeasurements % 1000 == 0) {
    //   println("%f - Current cluster utilization: %f %f cpu , %f %f mem"
    //           .format(currentTime,
    //                   cellState.totalOccupiedCpus,
    //                   cellState.totalOccupiedCpus / cellState.totalCpus,
    //                   cellState.totalOccupiedMem,
    //                   cellState.totalOccupiedMem / cellState.totalMem))
    //   println(("%f - Current cluster utilization from locked resources: " +
    //            "%f cpu, %f mem")
    //            .format(currentTime,
    //                    cellState.totalLockedCpus,
    //                    cellState.totalLockedCpus/ cellState.totalCpus,
    //                    cellState.totalLockedMem,
    //                    cellState.totalLockedMem / cellState.totalMem))
    // }

    // Only schedule a monitoring event if the simulator has
    // more (non-monitoring) events to play. Else this will cause
    // the simulator to run forever just to keep monitoring.
    if (agenda.nonEmpty) {
      afterDelay(monitoringPeriod) {
        measureUtilization()
      }
    }
  }

  override
  def run(runTime:Option[Double] = None,
          wallClockTimeout:Option[Double] = None): (Boolean, Double) = {
    assert(currentTime == 0.0, "currentTime must be 0 at simulator run time.")
    schedulers.values.foreach(scheduler => {
      assert(scheduler.jobQueueSize == 0,
             "Schedulers are not allowed to have jobs in their " +
             "queues when we run the simulator.")
    })
    // Optionally, start the utilization monitoring loop.
    if (monitorUtilization) {
      afterDelay(-1) {
        measureUtilization()
      }
    }
    super.run(runTime, wallClockTimeout)
  }
}

class SchedulerDesc(val name: String,
                    val constantThinkTimes: Map[String, Double],
                    val perTaskThinkTimes: Map[String, Double])

/**
 * A Scheduler maintains `@code Job`s submitted to it in a queue, and
 * attempts to match those jobs with resources by making "job scheduling
 * decisions", which take a certain amount of "scheduling time".
 * A simulator is responsible for interacting with a Scheduler, e.g.,
 * by deciding which workload types should be assigned to which scheduler.
 * A Scheduler must accept Jobs associated with any workload type (workloads
 * are identified by name), though it can do whatever it wants with those
 * jobs, include, optionally, dropping them on the floor, or handling jobs
 * from different workloads differently, which concretely, means taking
 * a different amount of "scheduling time" to schedule jobs from different
 * workloads.
 *
 * @param name Unique name that this scheduler is known by for the purposes
 *        of jobs being assigned to.
 * @param constantThinkTimes Map from workloadNames to constant times,
 *        in seconds, this scheduler uses to schedule each job.
 * @param perTaskThinkTimes Map from workloadNames to times, in seconds,
 *        this scheduler uses to schedule each task that is assigned to
 *        a scheduler of that name.
 * @param numMachinesToBlackList a positive number representing how many
 *        machines (chosen randomly) this scheduler should ignore when
 *        making scheduling decisions.
 */
abstract class Scheduler(val name: String,
                         constantThinkTimes: Map[String, Double],
                         perTaskThinkTimes: Map[String, Double],
                         numMachinesToBlackList: Double,
                         allocationMode: AllocationModes.Value) {
  assert(constantThinkTimes.size == perTaskThinkTimes.size)
  assert(numMachinesToBlackList >= 0)

  assert(allocationMode == AllocationModes.Incremental ||
    allocationMode == AllocationModes.All,
    ("allocationMode must be one of: {%s, %s}, " +
      "but it was %s.").format(AllocationModes.Incremental, AllocationModes.All, allocationMode))

  protected val pendingQueue: collection.mutable.Queue[Job] = collection.mutable.Queue[Job]()
  protected val runningQueue: collection.mutable.Queue[Job] = collection.mutable.Queue[Job]()

  // This gets set when this scheduler is added to a Simulator.
  // TODO(andyk): eliminate this pointer and make the scheduler
  //              more functional.
  // TODO(andyk): Clean up these <subclass>Simulator classes
  //              by templatizing the Scheduler class and having only
  //              one simulator of the correct type, instead of one
  //              simulator for each of the parent and child classes.
  var simulator: ClusterSimulator = _
  var scheduling: Boolean = false

  // Job transaction stat counters.
  var numSuccessfulTransactions: Int = 0
  var numFailedTransactions: Int = 0
  var numRetriedTransactions: Int = 0
  var dailySuccessTransactions = mutable.HashMap[Int, Int]()
  var dailyFailedTransactions = mutable.HashMap[Int, Int]()
  var numJobsTimedOutScheduling: Int = 0
  // Task transaction stat counters.
  var numSuccessfulTaskTransactions: Int = 0
  var numFailedTaskTransactions: Int = 0
  var numNoResourcesFoundSchedulingAttempts: Int = 0
  // When trying to place a task, count the number of machines we look at 
  // that the task doesn't fit on. This is a sort of wasted work that
  // causes the simulation to go slow.
  var failedFindVictimAttempts: Int = 0
  // Keep a cache of candidate pools around, indexed by their length
  // to avoid the overhead of the Array.range call in our inner scheduling
  // loop.
  val candidatePoolCache = mutable.HashMap[Int, mutable.IndexedSeq[Int]]()
  var totalUsefulTimeScheduling = 0.0 // in seconds
  var totalWastedTimeScheduling = 0.0 // in seconds
  var firstAttemptUsefulTimeScheduling = 0.0 // in seconds
  var firstAttemptWastedTimeScheduling = 0.0 // in seconds
  var dailyUsefulTimeScheduling = mutable.HashMap[Int, Double]()
  var dailyWastedTimeScheduling = mutable.HashMap[Int, Double]()
  // Also track the time, in seconds, spent scheduling broken out by
  // workload type. Note that all Schedulers (even, e.g., SinglePath
  // schedulers) can handle jobs from multiple workload generators.
  var perWorkloadUsefulTimeScheduling = mutable.HashMap[String, Double]()
  var perWorkloadWastedTimeScheduling = mutable.HashMap[String, Double]()
  val randomNumberGenerator = new util.Random(Seed())

  // The following variables are used to understand the average queue size during the simulation
  var totalQueueSize: Long = 0
  var numSchedulingCalls: Long = 0
  def avgQueueSize: Double = totalQueueSize / numSchedulingCalls.toDouble

  override
  def toString = name

  def checkRegistered() = {
    assert(simulator != null, "You must assign a simulator to a " +
                              "Scheduler before you can use it.")
  }

  def wakeUp(): Unit = {
    //FIXME: Fix this hack thing to force the user to override this method
    throw new Exception("Please override this method.")
  }

  // Add a job to this scheduler's job queue.
  def addJob(job: Job): Unit = {
    checkRegistered()

    assert(job.unscheduledTasks > 0, "A job must have at least one unscheduled task.")
    // Make sure the perWorkloadTimeScheduling Map has a key for this job's
    // workload type, so that we still print something for statistics about
    // that workload type for this scheduler, even if this scheduler never
    // actually gets a chance to schedule a job of that type.
    perWorkloadUsefulTimeScheduling(job.workloadName) =
        perWorkloadUsefulTimeScheduling.getOrElse(job.workloadName,0.0)
    perWorkloadWastedTimeScheduling(job.workloadName) =
        perWorkloadWastedTimeScheduling.getOrElse(job.workloadName,0.0)
    job.lastEnqueued = simulator.currentTime
  }

  /**
   * Creates and applies ClaimDeltas for all available resources in the
   * provided `@code cellState`. This is intended to leave no resources
   * free in cellState, thus it doesn't use minCpu or minMem because that
   * could lead to leaving fragmentation. I haven't thought through 
   * very carefully if floating point math could cause a problem here.
   */
  def scheduleAllAvailable(cellState: CellState,
                           locked: Boolean): Seq[ClaimDelta]  = {
    val claimDeltas = collection.mutable.ListBuffer[ClaimDelta]()
    for(mID <- 0 until cellState.numMachines) {
      val cpusAvail = cellState.availableCpusPerMachine(mID)
      val memAvail = cellState.availableMemPerMachine(mID)
      if (cpusAvail > 0.0 || memAvail > 0.0) {
        // Create and apply a claim delta.
        assert(mID >= 0 && mID < cellState.machineSeqNums.length)
        //TODO(andyk): Clean up semantics around taskDuration in ClaimDelta
        //             since we want to represent offered resources, not
        //             tasks with these deltas.
        val claimDelta = new ClaimDelta(this,
                                        mID,
                                        cellState.machineSeqNums(mID),
                                        -1.0,
                                        cpusAvail,
                                        memAvail)
        claimDelta.apply(cellState, locked)
        claimDeltas += claimDelta
      }
    }
    claimDeltas
  }

  /**
   * Given a job and a cellstate, find machines that the tasks of the
   * job will fit into, and allocate the resources on that machine to
   * those tasks, accounting those resources to this scheduler, modifying
   * the provided cellstate (by calling apply() on the created deltas).
   *
   * Implements the following randomized first fit scheduling algorithm:
   * while(more machines in candidate pool and more tasks to schedule):
   *   candidateMachine = random machine in pool
   *   if(candidate machine can hold at least one of this jobs tasks):
   *     create a delta assigning the task to that machine
   *   else:
   *     remove from candidate pool
   *
    * @return List of deltas, one per task, so that the transactions can
   *         be played on some other cellstate if desired.
   */
  def scheduleJob(job: Job,
                  cellState: CellState,
                  elastic:Boolean = false): ListBuffer[ClaimDelta] = {
    assert(simulator != null)
    assert(cellState != null)
    assert(job.cpusPerTask <= cellState.cpusPerMachine,
      "Looking for machine with %f cpus, but machines only have %f cpus."
        .format(job.cpusPerTask, cellState.cpusPerMachine))
    assert(job.memPerTask <= cellState.memPerMachine,
      "Looking for machine with %f mem, but machines only have %f mem."
        .format(job.memPerTask, cellState.memPerMachine))
    val claimDeltas = collection.mutable.ListBuffer[ClaimDelta]()

    // Cache candidate pools in this scheduler for performance improvements.
    val candidatePool =
      candidatePoolCache.getOrElseUpdate(cellState.numMachines,
        Array.range(0, cellState.numMachines))

    var numRemainingTasks = if(!elastic) job.unscheduledTasks else job.elasticTasksUnscheduled
    var remainingCandidates =
      math.max(0, cellState.numMachines - numMachinesToBlackList).toInt
    while(numRemainingTasks > 0 && remainingCandidates > 0) {
      // Pick a random machine out of the remaining pool, i.e., out of the set
      // of machineIDs in the first remainingCandidate slots of the candidate
      // pool.
      val candidateIndex = randomNumberGenerator.nextInt(remainingCandidates)
      val currMachID = candidatePool(candidateIndex)

      // If one of this job's tasks will fit on this machine, then assign
      // to it by creating a claimDelta and leave it in the candidate pool.
      if (cellState.availableCpusPerMachine(currMachID) >= job.cpusPerTask &&
        cellState.availableMemPerMachine(currMachID) >= job.memPerTask) {
        assert(currMachID >= 0 && currMachID < cellState.machineSeqNums.length)
        val claimDelta = new ClaimDelta(this,
          currMachID,
          cellState.machineSeqNums(currMachID),
          job.taskDuration,
          job.cpusPerTask,
          job.memPerTask)
        claimDelta.apply(cellState = cellState, locked = false)
        claimDeltas += claimDelta
        numRemainingTasks -= 1
      } else {
        failedFindVictimAttempts += 1
        // Move the chosen candidate to the end of the range of
        // remainingCandidates so that we won't choose it again after we
        // decrement remainingCandidates. Do this by swapping it with the
        // machineID currently at position (remainingCandidates - 1)
        candidatePool(candidateIndex) = candidatePool(remainingCandidates - 1)
        candidatePool(remainingCandidates - 1) = currMachID
        remainingCandidates -= 1
        simulator.logger.debug(
          ("%s in scheduling algorithm, tried machine %d, but " +
            "%f cpus and %f mem are required, and it only " +
            "has %f cpus and %f mem available.")
            .format(name,
              currMachID,
              job.cpusPerTask,
              job.memPerTask,
              cellState.availableCpusPerMachine(currMachID),
              cellState.availableMemPerMachine(currMachID)))
      }
    }
    claimDeltas
  }

  // Give up on a job if (a) it hasn't scheduled a single task in
  // 100 tries or (b) it hasn't finished scheduling after 1000 tries.
  def giveUpSchedulingJob(job: Job): Boolean = {
    allocationMode match {
      case AllocationModes.Incremental =>
        (job.numSchedulingAttempts > 100 && job.unscheduledTasks == job.moldableTasks) || job.numSchedulingAttempts > 1000
      case AllocationModes.All => job.numSchedulingAttempts > 1000
      case _ => false
    }
  }

  def jobQueueSize: Long = pendingQueue.size
  def runningJobQueueSize: Long = runningQueue.size

  def isMultiPath: Boolean =
    constantThinkTimes.values.toSet.size > 1 ||
    perTaskThinkTimes.values.toSet.size > 1

  def addDailyTimeScheduling(counter: mutable.HashMap[Int, Double],
                             timeScheduling: Double) = {
    val index: Int = math.floor(simulator.currentTime / 86400).toInt
    val currAmt: Double = counter.getOrElse(index, 0.0)
    counter(index) = currAmt + timeScheduling
  }

  def recordUsefulTimeScheduling(job: Job,
                                 timeScheduling: Double,
                                 isFirstSchedAttempt: Boolean): Unit = {
    assert(simulator != null, "This scheduler has not been added to a " +
                              "simulator yet.")
    // Scheduler level stats.
    totalUsefulTimeScheduling += timeScheduling
    addDailyTimeScheduling(dailyUsefulTimeScheduling, timeScheduling)
    if (isFirstSchedAttempt) {
      firstAttemptUsefulTimeScheduling += timeScheduling
    }
    simulator.logger.debug("Recorded %f seconds of %s useful think time, total now: %f."
                  .format(timeScheduling, name, totalUsefulTimeScheduling))

    // Job/workload level stats.
    job.usefulTimeScheduling += timeScheduling
    simulator.logger.debug("Recorded %f seconds of job %s useful think time, total now: %f."
                  .format(timeScheduling, job.id, simulator.workloads.filter(_.name == job.workloadName).head.totalJobUsefulThinkTimes))

    // Also track per-path (i.e., per workload) scheduling times
    perWorkloadUsefulTimeScheduling(job.workloadName) =
        perWorkloadUsefulTimeScheduling.getOrElse(job.workloadName,0.0) +
        timeScheduling
  }

  def recordWastedTimeScheduling(job: Job,
                                 timeScheduling: Double,
                                 isFirstSchedAttempt: Boolean): Unit = {
    assert(simulator != null, "This scheduler has not been added to a " +
                              "simulator yet.")
    // Scheduler level stats.
    totalWastedTimeScheduling += timeScheduling
    addDailyTimeScheduling(dailyWastedTimeScheduling, timeScheduling)
    if (isFirstSchedAttempt) {
      firstAttemptWastedTimeScheduling += timeScheduling
    }
    simulator.logger.debug("Recorded %f seconds of %s wasted think time, total now: %f."
      .format(timeScheduling, name, totalWastedTimeScheduling))

    // Job/workload level stats.
    job.wastedTimeScheduling += timeScheduling
    simulator.logger.debug("Recorded %f seconds of job %s wasted think time, total now: %f."
      .format(timeScheduling, job.id, simulator.workloads.filter(_.name == job.workloadName).head.totalJobWastedThinkTimes))

    // Also track per-path (i.e., per workload) scheduling times
    perWorkloadWastedTimeScheduling(job.workloadName) =
        perWorkloadWastedTimeScheduling.getOrElse(job.workloadName,0.0) +
        timeScheduling
  }

  /**
   * Computes the time, in seconds, this scheduler requires to make
   * a scheduling decision for `@code job`.
   *
   * @param job the job to determine this schedulers think time for
   */
  def getThinkTime(job: Job): Double = {
    assert(constantThinkTimes.contains(job.workloadName))
    assert(perTaskThinkTimes.contains(job.workloadName))
    constantThinkTimes(job.workloadName) +
        perTaskThinkTimes(job.workloadName) * job.unscheduledTasks.toFloat
  }

  /**
    * Computes the time, in seconds, this scheduler requires to make
    * a scheduling decision for `@code job`.
    *
    * @param job the job to determine this schedulers think time for
    * @param unscheduledTask  the number of tasks left to be scheduled
    */
  def getThinkTime(job: Job, unscheduledTask: Int): Double = {
    assert(constantThinkTimes.contains(job.workloadName))
    assert(perTaskThinkTimes.contains(job.workloadName))
    constantThinkTimes(job.workloadName) +
      perTaskThinkTimes(job.workloadName) * unscheduledTask.toFloat
  }
}

class ClaimDelta(val scheduler: Scheduler,
                 val machineID: Int,
                 val machineSeqNum: Long,
                 val duration: Double,
                 val cpus: Double,
                 val mem: Double,
                 val onlyLocked: Boolean = false) {
  /**
   * Claim `@code cpus` and `@code mem` from `@code cellState`.
   * Increments the sequenceNum of the machine with ID referenced
   * by machineID.
   */
  def apply(cellState: CellState, locked: Boolean): Unit = {
    cellState.assignResources(scheduler, machineID, cpus, mem, locked)
    // Mark that the machine has changed, used for testing for conflicts
    // when using optimistic concurrency.
    cellState.machineSeqNums(machineID) += 1
  }

  def unApply(cellState: CellState, locked: Boolean = false): Unit = {
    cellState.freeResources(scheduler, machineID, cpus, mem, onlyLocked || locked)
  }
}

class CellStateDesc(val numMachines: Int,
                    val cpusPerMachine: Double,
                    val memPerMachine: Double)

class CellState(val numMachines: Int,
                val cpusPerMachine: Double,
                val memPerMachine: Double,
                val conflictMode: String,
                val transactionMode: String) {
  assert(conflictMode.equals("resource-fit") ||
         conflictMode.equals("sequence-numbers"),
         "conflictMode must be one of: {'resource-fit', 'sequence-numbers'}, " +
         "but it was %s.".format(conflictMode))
  assert(transactionMode.equals("all-or-nothing") ||
         transactionMode.equals("incremental"),
         "transactionMode must be one of: {'all-or-nothing', 'incremental'}, " +
         "but it was %s.".format(transactionMode))
  var simulator: ClusterSimulator = _
  // An array where value at position k is the total cpus that have been
  // allocated for machine k.
  val allocatedCpusPerMachine = new Array[Double](numMachines)
  val allocatedMemPerMachine = new Array[Double](numMachines)
  val machineSeqNums = new Array[Long](numMachines)

  // Map from scheduler name to number of cpus assigned to that scheduler.
  val occupiedCpus = mutable.HashMap[String, Double]()
  val occupiedMem = mutable.HashMap[String, Double]()
  // Map from scheduler name to number of cpus locked while that scheduler
  // makes scheduling decisions about them, i.e., while resource offers made
  // to that scheduler containing those amount of resources is pending.
  val lockedCpus = mutable.HashMap[String, Double]()
  val lockedMem = mutable.HashMap[String, Double]()

  // These used to be functions that sum the values of occupiedCpus,
  // occupiedMem, lockedCpus, and lockedMem, but its much faster to use
  // scalars that we update in assignResources and freeResources.
  var totalOccupiedCpus = 0.0
  var totalOccupiedMem = 0.0
  var totalLockedCpus = 0.0
  var totalLockedMem = 0.0

  def totalCpus = numMachines * cpusPerMachine
  def totalMem = numMachines * memPerMachine
  def availableCpus = totalCpus - (totalOccupiedCpus + totalLockedCpus)
  def availableMem = totalMem - (totalOccupiedMem + totalLockedMem)
  def isFull = availableCpus.floor <= 0 || availableMem.floor <= 0

  // Convenience methods to see how many cpus/mem are available on a machine.
  def availableCpusPerMachine(machineID: Int) = {
    assert(machineID <= allocatedCpusPerMachine.length - 1,
           "There is no machine with ID %d.".format(machineID))
    cpusPerMachine - allocatedCpusPerMachine(machineID)
  }
  def availableMemPerMachine(machineID: Int) = {
    assert(machineID <= allocatedMemPerMachine.length - 1,
           "There is no machine with ID %d.".format(machineID))
    memPerMachine - allocatedMemPerMachine(machineID)
  }

  /**
   * Allocate resources on a machine to a scheduler.
   *
   * @param locked  Mark these resources as being pessimistically locked
   *                (i.e. while they are offered as part of a Mesos
   *                resource-offer).
   */
  def assignResources(scheduler: Scheduler,
                      machineID: Int,
                      cpus: Double,
                      mem: Double,
                      locked: Boolean) = {
    // Track the resources used by this scheduler.
     assert(cpus <= availableCpus + 0.000001,
            ("Attempting to assign more CPUs (%f) than " +
             "are currently available in CellState (%f).")
            .format(cpus, availableCpus))
     assert(mem <= availableMem + 0.000001,
            ("Attempting to assign more mem (%f) than " +
             "is currently available in CellState (%f).")
            .format(mem, availableMem))
    if (locked) {
      lockedCpus(scheduler.name) = lockedCpus.getOrElse(scheduler.name, 0.0) + cpus
      lockedMem(scheduler.name) = lockedMem.getOrElse(scheduler.name, 0.0) + mem
      assert(lockedCpus(scheduler.name) <= totalCpus + 0.000001)
      assert(lockedMem(scheduler.name) <= totalMem + 0.000001)
      totalLockedCpus += cpus
      totalLockedMem += mem
    } else {
      occupiedCpus(scheduler.name) = occupiedCpus.getOrElse(scheduler.name, 0.0) + cpus
      occupiedMem(scheduler.name) = occupiedMem.getOrElse(scheduler.name, 0.0) + mem
      assert(occupiedCpus(scheduler.name) <= totalCpus + 0.000001)
      assert(occupiedMem(scheduler.name) <= totalMem + 0.000001)
      totalOccupiedCpus += cpus
      totalOccupiedMem += mem
    }

    // Also track the per machine resources available.
    assert(availableCpusPerMachine(machineID) >= cpus,
           ("Scheduler %s (%d) tried to claim %f cpus on machine %d in cell " +
            "state %d, but it only has %f unallocated cpus right now.")
           .format(scheduler.name,
                   scheduler.hashCode,
                   cpus,
                   machineID,
                   hashCode(),
                   availableCpusPerMachine(machineID)))
    assert(availableMemPerMachine(machineID) >= mem, 
           ("Scheduler %s (%d) tried to claim %f mem on machine %d in cell " +
            "state %d, but it only has %f mem unallocated right now.")
           .format(scheduler.name,
                   scheduler.hashCode,
                   mem,
                   machineID,
                   hashCode(),
                   availableMemPerMachine(machineID)))
    allocatedCpusPerMachine(machineID) += cpus
    allocatedMemPerMachine(machineID) += mem
  }

  // Release the specified number of resources used by this scheduler.
  def freeResources(scheduler: Scheduler,
                    machineID: Int,
                    cpus: Double,
                    mem: Double,
                    locked: Boolean) = {
    if (locked) {
      assert(lockedCpus.contains(scheduler.name))
      val safeToFreeCpus: Boolean = lockedCpus(scheduler.name) >= (cpus - 0.001)
      assert(safeToFreeCpus,
             "%s tried to free %f cpus, but was only occupying %f."
             .format(scheduler.name, cpus, lockedCpus(scheduler.name)))
      assert(lockedMem.contains(scheduler.name))
      val safeToFreeMem: Boolean = lockedMem(scheduler.name) >= (mem - 0.001)
      assert(safeToFreeMem,
             "%s tried to free %f mem, but was only occupying %f."
             .format(scheduler.name, mem, lockedMem(scheduler.name)))
      lockedCpus(scheduler.name) = lockedCpus(scheduler.name) - cpus
      lockedMem(scheduler.name) = lockedMem(scheduler.name) - mem
      totalLockedCpus -= cpus
      totalLockedMem -= mem
    } else {
      assert(occupiedCpus.contains(scheduler.name))
      val safeToFreeCpus: Boolean = occupiedCpus(scheduler.name) >= (cpus - 0.001)
      assert(safeToFreeCpus,
             "%s tried to free %f cpus, but was only occupying %f."
             .format(scheduler.name, cpus, occupiedCpus(scheduler.name)))
      assert(occupiedMem.contains(scheduler.name))
      val safeToFreeMem: Boolean = occupiedMem(scheduler.name) >= (mem - 0.001)
      assert(safeToFreeMem,
             "%s tried to free %f mem, but was only occupying %f."
             .format(scheduler.name, mem, occupiedMem(scheduler.name)))
      occupiedCpus(scheduler.name) = occupiedCpus(scheduler.name) - cpus
      occupiedMem(scheduler.name) = occupiedMem(scheduler.name) - mem
      totalOccupiedCpus -= cpus
      totalOccupiedMem -= mem
    }

    // Also track the per machine resources available.
    assert(availableCpusPerMachine(machineID) + cpus <=
           cpusPerMachine + 0.000001, "Cpus are %f".format(availableCpusPerMachine(machineID) + cpus))
    assert(availableMemPerMachine(machineID) + mem <=
           memPerMachine + 0.000001)
    allocatedCpusPerMachine(machineID) -= cpus
    allocatedMemPerMachine(machineID) -= mem
  }

  /**
   * Return a copy of this cell state in its current state.
   */
  def copy: CellState = {
    val newCellState = new CellState(numMachines,
                                     cpusPerMachine,
                                     memPerMachine,
                                     conflictMode,
                                     transactionMode)
    Array.copy(src = allocatedCpusPerMachine,
               srcPos = 0,
               dest = newCellState.allocatedCpusPerMachine,
               destPos = 0,
               length = numMachines)
    Array.copy(src = allocatedMemPerMachine,
               srcPos = 0,
               dest = newCellState.allocatedMemPerMachine,
               destPos = 0,
               length = numMachines)
    Array.copy(src = machineSeqNums, 
               srcPos = 0,
               dest = newCellState.machineSeqNums,
               destPos = 0,
               length = numMachines)
    newCellState.occupiedCpus ++= occupiedCpus
    newCellState.occupiedMem ++= occupiedMem
    newCellState.lockedCpus ++= lockedCpus
    newCellState.lockedMem ++= lockedMem
    newCellState.totalOccupiedCpus = totalOccupiedCpus
    newCellState.totalOccupiedMem = totalOccupiedMem
    newCellState.totalLockedCpus = totalLockedCpus
    newCellState.totalLockedMem = totalLockedMem
    newCellState
  }

  case class CommitResult(committedDeltas: Seq[ClaimDelta],
                          conflictedDeltas: Seq[ClaimDelta])

  /**
   *  Attempt to play the list of deltas, return any that conflicted.
   */
  def commit(deltas: Seq[ClaimDelta],
             scheduleEndEvent: Boolean = false): CommitResult = {
    var rollback = false // Track if we need to rollback changes.
    var appliedDeltas = collection.mutable.ListBuffer[ClaimDelta]()
    var conflictDeltas = collection.mutable.ListBuffer[ClaimDelta]()
    var conflictMachines = collection.mutable.ListBuffer[Int]()

    def commitNonConflictingDeltas(): Unit = {
      deltas.foreach(d => {
        // We check two things here
        // 1) if the deltas can cause conflict
        // 2) if the machine that is contained in the delta had previous conflict
        // This is to improve performances and to avoid a bug when using "incremental" and "sequence-number" that
        // affect the sequence number of a machine.
        // The bug appears when job A assign 2 tasks on the same machine and thus will have a difference in the
        // seq-number of 1. But at the same time job B may have already incremented the seq-number to the same value by
        // placing one task with a lot of resources requirements. Therefore when the seq-number of job A task 2 and job
        // B task 1 have the same values, but the resources left on the machine are different from what they think
        if (causesConflict(d) || conflictMachines.contains(d.machineID)) {
          simulator.logger.info("delta (%s mach-%d seqNum-%d) caused a conflict."
                        .format(d.scheduler, d.machineID, d.machineSeqNum))
          conflictDeltas += d
          conflictMachines += d.machineID
          if (transactionMode == "all-or-nothing") {
            rollback = true
            return
          } else if (transactionMode == "incremental") {
            
          } else {
            simulator.logger.error("Invalid transactionMode.")
          }
        } else {
          d.apply(cellState = this, locked = false)
          appliedDeltas += d
        }
      })
    }
    commitNonConflictingDeltas()
    // Rollback if necessary.
    if (rollback) {
      simulator.logger.info("Rolling back %d deltas.".format(appliedDeltas.length))
      appliedDeltas.foreach(d => {
        d.unApply(cellState = this, locked = false)
        conflictDeltas += d
        appliedDeltas -= d
      })
    }
    
    if (scheduleEndEvent) { 
      scheduleEndEvents(appliedDeltas)
    }
    CommitResult(appliedDeltas, conflictDeltas)
  }

  /**
    * Create an end event for each delta provided. The end event will
    * free the resources used by the task represented by the ClaimDelta.
    * We also calculate the highest delay after which the resources are
    * free and we return that value
    *
    * @param claimDeltas The resources that were busy
    * @param delay Set the delay at which the end events should occur.
    *              if value is -1 use the duration of the task
    * @param jobId The id of the job which this deltas belong to
    * @return The higher delay after which the resources are free
    */
  def scheduleEndEvents(claimDeltas: Seq[ClaimDelta], delay: Double = -1, jobId: Long = 0): Unit = {
    assert(simulator != null, "Simulator must be non-null in CellState.")
    var maxDelay = 0.0
    claimDeltas.foreach(appliedDelta => {
      val realDelay = if(delay == -1) appliedDelta.duration else delay
      if(realDelay > maxDelay) maxDelay = realDelay
      simulator.afterDelay(realDelay, eventType = EventTypes.Remove , itemId = jobId) {
        appliedDelta.unApply(simulator.cellState)
        simulator.logger.info(("A task started by scheduler %s finished. " +
                       "Freeing %f cpus, %f mem. Available: %f cpus, %f mem.")
                     .format(appliedDelta.scheduler.name,
                             appliedDelta.cpus,
                             appliedDelta.mem,
                             availableCpus,
                             availableMem))
      }
    })

    simulator.schedulers.foreach{case(key, scheduler) =>
        simulator.afterDelay(maxDelay, eventType = EventTypes.Trigger, itemId = jobId){
          scheduler.wakeUp()
        }
      }
  }

  /**
   * Tests if this delta causes a transaction conflict.
   * Different test scheme is used depending on conflictMode.
   */
  def causesConflict(delta: ClaimDelta): Boolean = {
    conflictMode match {
      case "sequence-numbers" =>
        simulator.logger.debug("Checking if seqNum for machine %d are different. (%d - %d)"
          .format(delta.machineID, delta.machineSeqNum, machineSeqNums(delta.machineID)))
        // Use machine sequence numbers to test for conflicts.
        if (delta.machineSeqNum != machineSeqNums(delta.machineID)) {
          simulator.logger.info("Sequence-number conflict occurred " +
            "(sched-%s, mach-%d, seq-num-%d, seq-num-%d)."
              .format(delta.scheduler,
                delta.machineID,
                delta.machineSeqNum,
                machineSeqNums(delta.machineID)))
          true
        } else {
          false
        }
      case "resource-fit" =>
        // Check if the machine is currently short of resources,
        // regardless of whether sequence nums have changed.
        if (availableCpusPerMachine(delta.machineID) < delta.cpus ||
            availableMemPerMachine(delta.machineID) <  delta.mem) {
          simulator.logger.info("Resource-aware conflict occurred " +
                        "(sched-%s, mach-%d, cpus-%f, mem-%f)."
                        .format(delta.scheduler,
                                delta.machineID,
                                delta.cpus,
                                delta.mem))
          true
        } else {
          false
        }
      case _ =>
        simulator.logger.error("Unrecognized conflictMode %s.".format(conflictMode))
        true // Should never be reached.
    }
  }
}

object JobStates extends Enumeration {
  val TimedOut, Not_Scheduled, Partially_Scheduled, Fully_Scheduled, Completed = Value
}

/**
  *
  * @param id           Job ID
  * @param submitted    The time the job was submitted in seconds
  * @param _numTasks     Number of tasks that compose this job
  * @param taskDuration Durations of each task in seconds
  * @param workloadName Type of job
  * @param cpusPerTask  Number of cpus required by this job
  * @param memPerTask   Amount of ram, in GB, required by this job
  * @param isRigid      Boolean value to check if the job is picky or not
  */
case class Job(id: Long,
               submitted: Double,
               _numTasks: Int,
               var taskDuration: Double,
               workloadName: String,
               cpusPerTask: Double,
               memPerTask: Double,
               isRigid: Boolean = false,
               numMoldableTasks: Option[Integer] = None,
               priority: Int = 0,
               error: Double = 1) {
  assert(_numTasks != 0, "numTasks for job %d is %d".format(id, _numTasks))
  assert(!cpusPerTask.isNaN, "cpusPerTask for job %d is %f".format(id, cpusPerTask))
  assert(!memPerTask.isNaN, "memPerTask for job %d is %f".format(id, memPerTask))

  val sizeAdjustment: Double = {
    if(workloadName.equals("Interactive"))
      0.001
    else
      1
  }

  override def equals(that: Any): Boolean =
    that match {
      case that: Job => that.id == this.id && that.workloadName.equals(this.workloadName)
      case _ => false
    }

  val logger = Logger.getLogger(this.getClass.getName)

  var requestedCores: Double = Int.MaxValue

  private[this] var _unscheduledTasks: Int = _numTasks
  var finalStatus = JobStates.Not_Scheduled

  // Time, in seconds, this job spent waiting in its scheduler's queue
  private[this] var _firstScheduled: Boolean = true
  private[this] var _timeInQueueTillFirstScheduled: Double = 0.0
  private[this] var _timeInQueueTillFullyScheduled: Double = 0.0

  // Timestamp last inserted into scheduler's queue, used to compute timeInQueue.
  var lastEnqueued: Double = 0.0
  var lastSchedulingStartTime: Double = 0.0
  // Number of scheduling attempts for this job (regardless of how many
  // tasks were successfully scheduled in each attempt).
  var numSchedulingAttempts: Long = 0
  // Number of "task scheduling attempts". I.e. each time a scheduling
  // attempt is made for this job, we increase this counter by the number
  // of tasks that still need to be be scheduled (at the beginning of the
  // scheduling attempt). This is useful for analyzing the impact of no-fit
  // events on the scheduler busytime metric.
  var numTaskSchedulingAttempts: Long = 0
  var usefulTimeScheduling: Double = 0.0
  var wastedTimeScheduling: Double = 0.0
  // Store information to calculate the execution time of the job
  private[this] var _jobStartedWorking: Double = 0.0
  var jobFinishedWorking: Double = 0.0

  // Zoe Applications variables
  private[this] var _isZoeApp: Boolean = false
  private[this] var _moldableTasks: Int = 0
  private[this] var _elasticTasks: Int = 0
  private[this] var _moldableTasksUnscheduled: Int = 0
  private[this] var _elasticTasksUnscheduled: Int = 0

  private[this] var _jobDuration: Double = taskDuration
  private[this] var _elasticTaskSpeedUpContribution: Double = 0
  private[this] var _size: Double = 0
  private[this] var _progress: Double = 0
  // This variable is used to calculate the relative (non absolute) progression
  private[this] var _lastProgressTimeCalculation: Double = 0

  var claimInelasticDeltas: collection.mutable.ListBuffer[ClaimDelta] = collection.mutable.ListBuffer[ClaimDelta]()
  var claimElasticDeltas: collection.mutable.ListBuffer[ClaimDelta] = collection.mutable.ListBuffer[ClaimDelta]()
  def allClaimDeltas: collection.mutable.ListBuffer[ClaimDelta] = claimInelasticDeltas ++ claimElasticDeltas

  /*
    * We set the number of moldable tasks that compose the Zoe Applications
    * remember that at least one moldable service must exist
    * a Zoe Application cannot be composed by just elastic services
    */
  numMoldableTasks match {
    case Some(param) =>
      _isZoeApp = true

      _moldableTasks = param
      if(_moldableTasks == 0)
        _moldableTasks = 1
      _elasticTasks = _numTasks
      unscheduledTasks = _moldableTasks
      elasticTasksUnscheduled = _elasticTasks

//      _jobDuration = _elasticTasks * taskDuration
      _jobDuration = taskDuration
      _elasticTaskSpeedUpContribution = taskDuration
    case None => None
  }

  def isZoeApp: Boolean = _isZoeApp

  // Some getter and Setter
  def numTasks: Int = if(_isZoeApp) _moldableTasks + _elasticTasks else _numTasks

  def timeInQueueTillFirstScheduled: Double = _timeInQueueTillFirstScheduled
  def timeInQueueTillFirstScheduled_=(value: Double): Unit = {
    _timeInQueueTillFirstScheduled = value
  }

  def timeInQueueTillFullyScheduled: Double = _timeInQueueTillFullyScheduled
  def timeInQueueTillFullyScheduled_=(value: Double): Unit = {
    _timeInQueueTillFullyScheduled = value
  }

  def jobStartedWorking: Double = _jobStartedWorking
  def jobStartedWorking_=(value: Double): Unit = {
    _jobStartedWorking = value
    _lastProgressTimeCalculation = _jobStartedWorking
  }

  def firstScheduled: Boolean = _firstScheduled
  def firstScheduled_=(value: Boolean): Unit = {
    _firstScheduled = value
  }

  def unscheduledTasks: Int = {
    if(_isZoeApp) _moldableTasksUnscheduled else _unscheduledTasks
  }
  def unscheduledTasks_=(value:Int): Unit = {
    if(_isZoeApp) _moldableTasksUnscheduled = value else _unscheduledTasks = value
  }

  def elasticTasksUnscheduled: Int = _elasticTasksUnscheduled
  def elasticTasksUnscheduled_=(value: Int): Unit = {
    _elasticTasksUnscheduled = value
    assert(_elasticTasksUnscheduled <= elasticTasks, "Elastic Services to schedule are more than requested (%d/%d)".format(_elasticTasksUnscheduled, elasticTasks))
  }
  def elasticTasks: Int = _elasticTasks

  def jobDuration: Double = _jobDuration

  def elasticTaskSpeedUpContribution: Double = _elasticTaskSpeedUpContribution

  def remainingTime: Double = (1 - _progress) * jobDuration

  def estimateJobDuration(currTime: Double = 0.0, newTasksAllocated: Int = 0, tasksRemoved: Int = 0, mock: Boolean = false): Double = {
    val tasksAllocated = scheduledTasks + scheduledElasticTasks
    var relativeProgress: Double = 0.0
    val tmp_lastProgressTimeCalculation = _lastProgressTimeCalculation
    val tmp_progress = _progress

    if(_jobStartedWorking != currTime){
      relativeProgress = (currTime - _lastProgressTimeCalculation) /
        ((numTasks / (tasksAllocated - newTasksAllocated + tasksRemoved).toDouble) * jobDuration)
    }

    _lastProgressTimeCalculation = currTime
    _progress += relativeProgress

    assert(_progress < 1.0, "Progress cannot be higher than 1! (%f)".format(_progress))

    val timeLeft: Double = ( 1 - _progress) * ((numTasks / tasksAllocated.toDouble) * jobDuration)

    // This was the only solution that I found. It is ugly but it is working, dunno why moving down here the piece of code
    // that increments the _progress and updates the _lastProgressTimeCalculation was causing the _progress to go over 1.0
    if(mock){
      _lastProgressTimeCalculation = tmp_lastProgressTimeCalculation
      _progress = tmp_progress
    }

    timeLeft
  }

  def reset(): Unit = {
    _progress = 0
    unscheduledTasks = moldableTasks
    elasticTasksUnscheduled = elasticTasks
    claimInelasticDeltas = collection.mutable.ListBuffer[ClaimDelta]()
    claimElasticDeltas = collection.mutable.ListBuffer[ClaimDelta]()
    finalStatus =  JobStates.Not_Scheduled
    _firstScheduled = true
    _timeInQueueTillFirstScheduled = 0.0
    _timeInQueueTillFullyScheduled = 0.0
    _jobStartedWorking = 0.0
    jobFinishedWorking = 0.0
  }

  /**
    * It will return the moldable component of this job.
    * The moldable component is the tasks set that are required to be deployed before the job can start to
    * produce some work.
    * Currently only a Zoe Application support this knowledge.
    * For this reason this function abstract the type of Job (normal job or Zoe Application) to the scheduler simulators.
    *
    * @return The number of tasks
    */
  def moldableTasks: Int = {
    if(_isZoeApp) _moldableTasks else numTasks
  }

  def isScheduled: Boolean = finalStatus == JobStates.Partially_Scheduled || finalStatus == JobStates.Fully_Scheduled ||
    (finalStatus == JobStates.TimedOut && unscheduledTasks != moldableTasks) || finalStatus == JobStates.Completed
  def isFullyScheduled: Boolean = finalStatus == JobStates.Fully_Scheduled || finalStatus == JobStates.Completed
  def isCompleted: Boolean = finalStatus == JobStates.Completed
  def isPartiallyScheduled: Boolean = finalStatus == JobStates.Partially_Scheduled
  def isTimedOut: Boolean = finalStatus == JobStates.TimedOut
  def isNotScheduled: Boolean = finalStatus == JobStates.Not_Scheduled

  def scheduledTasks = moldableTasks - unscheduledTasks
  def scheduledElasticTasks = elasticTasks - elasticTasksUnscheduled

  def responseRatio(currentTime: Double): Double = 1 + (currentTime - submitted) / jobDuration

  // For Spark
  def coresGranted = cpusPerTask * scheduledTasks
  def coresLeft = math.max(0, requestedCores - coresGranted)

  def cpusStillNeeded: Double = cpusPerTask * unscheduledTasks
  def memStillNeeded: Double = memPerTask * unscheduledTasks

  // Calculate the maximum number of this jobs tasks that can fit into
  // the specified resources
  def numTasksToSchedule(cpusAvail: Double, memAvail: Double): Int = {
    if (cpusAvail == 0.0 || memAvail == 0.0) {
      0
    } else {
      val cpusChoppedToTaskSize = cpusAvail - (cpusAvail % cpusPerTask)
      val memChoppedToTaskSize = memAvail - (memAvail % memPerTask)
      val maxTasksThatWillFitByCpu = math.round(cpusChoppedToTaskSize / cpusPerTask)
      val maxTasksThatWillFitByMem = math.round(memChoppedToTaskSize / memPerTask)
      val maxTasksThatWillFit = math.min(maxTasksThatWillFitByCpu,
                                         maxTasksThatWillFitByMem)
      math.min(unscheduledTasks, maxTasksThatWillFit.toInt)
    }
  }

  // We cannot use this function for the Zoe Scheduler due to the fact that
  // jobs are not removed from the queue and then re-queued, thus the value
  // in timeInQueueTillFullyScheduled and timeInQueueTillFirstScheduled will
  // be uncorrect.
  // Notherless, using numSchedulingAttempts to check if the first task has been
  // scheduled is wrong, since a schedulingAttempts might not schedule any tasks
  // at all
  def updateTimeInQueueStats(currentTime: Double) = {
    // Every time part of this job is partially scheduled, add to
    // the counter tracking how long it spends in the queue till
    // its final task is scheduled.
    timeInQueueTillFullyScheduled += currentTime - lastEnqueued
    // If this is the first scheduling done for this job, then make a note
    // about how long the job waited in the queue for this first scheduling.
    if (numSchedulingAttempts == 0) {
      timeInQueueTillFirstScheduled += currentTime - lastEnqueued
    }
  }

  logger.trace("New Job. numTasks: %d, cpusPerTask: %f, memPerTask: %f".format(numTasks, cpusPerTask, memPerTask))
}

/**
 * A class that holds a list of jobs, each of which is used to record
 * statistics during a run of the simulator.
 *
 * Keep track of avgJobInterarrivalTime for easy reference later when
 * ExperimentRun wants to record it in experiment result protos.
 */
class Workload(val name: String,
               private var jobs: ListBuffer[Job] = ListBuffer()) {
  val logger = Logger.getLogger(this.getClass.getName)
  def getJobs: Seq[Job] = jobs
  def addJob(job: Job) = {
    assert (job.workloadName == name)
    jobs.append(job)
  }
  def removeJob(job: Job) = {
    assert (job.workloadName == name)
    jobs -= job
  }
  def addJobs(jobs: Seq[Job]) = jobs.foreach(job => {addJob(job)})
  def removeJobs(jobs: Seq[Job]) = jobs.foreach(job => {removeJob(job)})
  def sortJobs(): Unit = {
    jobs = jobs.sortBy(_.submitted)
  }
  def numJobs: Int = jobs.length

//  def cpus: Double = jobs.map(j => {j.numTasks * j.cpusPerTask}).sum
//  def mem: Double = jobs.map(j => {j.numTasks * j.memPerTask}).sum
  // Generate a new workload that has a copy of the jobs that
  // this workload has.
  def copy: Workload = {
    val newWorkload = new Workload(name)
    jobs.foreach(job => {
      newWorkload.addJob(job.copy())
    })
    newWorkload
  }

  def totalJobUsefulThinkTimes: Double = jobs.map(_.usefulTimeScheduling).sum
  def totalJobWastedThinkTimes: Double = jobs.map(_.wastedTimeScheduling).sum

  def avgJobInterarrivalTime: Double = {
    val submittedTimesArray = new Array[Double](jobs.length)
    jobs.map(_.submitted).copyToArray(submittedTimesArray)
    util.Sorting.quickSort(submittedTimesArray)
    // pass along (running avg, count)
    var sumInterarrivalTime = 0.0
    for(i <- 1 until submittedTimesArray.length) {
      sumInterarrivalTime += submittedTimesArray(i) - submittedTimesArray(i-1)
    }
    sumInterarrivalTime / submittedTimesArray.length
  }

  def jobUsefulThinkTimesPercentile(percentile: Double): Double = {
    assert(percentile <= 1.0 && percentile >= 0)
    val scheduledJobs = jobs.filter(_.isScheduled).toList
    // println("Setting up thinkTimesArray of length " +
    //         scheduledJobs.length)
    if (scheduledJobs.nonEmpty) {
      val thinkTimesArray = new Array[Double](scheduledJobs.length)
      scheduledJobs.map(job => {
        job.usefulTimeScheduling
      }).copyToArray(thinkTimesArray)
      util.Sorting.quickSort(thinkTimesArray)
      //println(thinkTimesArray.deep.toSeq.mkString("-*-"))
      // println("Looking up think time percentile value at position " +
      //         ((thinkTimesArray.length-1) * percentile).toInt)
      thinkTimesArray(((thinkTimesArray.length-1) * percentile).toInt)
    } else {
      -1.0
    }
  }

  def avgJobQueueTimeTillFirstScheduled: Double = {
    // println("Computing avgJobQueueTimeTillFirstScheduled.")
    val scheduledJobs = jobs.filter(_.isScheduled)
    if (scheduledJobs.nonEmpty) {
      val queueTimes = scheduledJobs.map(_.timeInQueueTillFirstScheduled).sum
      queueTimes / scheduledJobs.length
    } else {
      -1.0
    }
  }

  def avgJobQueueTimeTillFullyScheduled: Double = {
    // println("Computing avgJobQueueTimeTillFullyScheduled.")
    val scheduledJobs = jobs.filter(_.isFullyScheduled)
    if (scheduledJobs.nonEmpty) {
      val queueTimes = scheduledJobs.map(_.timeInQueueTillFullyScheduled).sum
      queueTimes / scheduledJobs.length
    } else {
      -1.0
    }
  }

  def avgJobRampUpTime: Double = {
    // println("Computing avgJobRampUpTime.")
    val scheduledJobs = jobs.filter(_.isFullyScheduled)
    if (scheduledJobs.nonEmpty) {
      val queueTimes = scheduledJobs.map(job => job.timeInQueueTillFullyScheduled - job.timeInQueueTillFirstScheduled).sum
      queueTimes / scheduledJobs.length
    } else {
      -1.0
    }
  }

  def jobQueueTimeTillFirstScheduledPercentile(percentile: Double): Double = {
    assert(percentile <= 1.0 && percentile >= 0)
    val scheduled = jobs.filter(_.isScheduled)
    if (scheduled.nonEmpty) {
      val queueTimesArray = new Array[Double](scheduled.length)
      scheduled.map(_.timeInQueueTillFirstScheduled)
               .copyToArray(queueTimesArray)
      util.Sorting.quickSort(queueTimesArray)
      val result =
          queueTimesArray(((queueTimesArray.length-1) * percentile).toInt)
      logger.debug(("Looking up job queue time till first scheduled " +
               "percentile value at position %d of %d: %f.")
              .format((queueTimesArray.length * percentile).toInt,
                      queueTimesArray.length,
                      result))
      result
    } else {
      -1.0
    }
  }

  def jobQueueTimeTillFullyScheduledPercentile(percentile: Double): Double = {
    assert(percentile <= 1.0 && percentile >= 0)
    val scheduled = jobs.filter(_.isFullyScheduled)
    if (scheduled.nonEmpty) {
      val queueTimesArray = new Array[Double](scheduled.length)
      scheduled.map(_.timeInQueueTillFullyScheduled)
               .copyToArray(queueTimesArray)
      util.Sorting.quickSort(queueTimesArray)
      val result = queueTimesArray(((queueTimesArray.length-1) * 0.9).toInt)
      logger.debug(("Looking up job queue time till fully scheduled " +
               "percentile value at position %d of %d: %f.")
              .format((queueTimesArray.length * percentile).toInt,
                      queueTimesArray.length,
                      result))
      result
    } else {
      -1.0
    }
  }

  def numSchedulingAttemptsPercentile(percentile: Double): Double = {
    assert(percentile <= 1.0 && percentile >= 0)
    logger.debug("largest 200 job scheduling attempt counts: " +
            jobs.map(_.numSchedulingAttempts)
                .sorted
                .takeRight(200)
                .mkString(","))
    val scheduled = jobs.filter(_.numSchedulingAttempts > 0)
    if (scheduled.nonEmpty) {
      val schedulingAttemptsArray = new Array[Long](scheduled.length)
      scheduled.map(_.numSchedulingAttempts).copyToArray(schedulingAttemptsArray)
      util.Sorting.quickSort(schedulingAttemptsArray)
      val result = schedulingAttemptsArray(((schedulingAttemptsArray.length-1) * 0.9).toInt)
      logger.debug(("Looking up num job scheduling attempts " +
               "percentile value at position %d of %d: %d.")
              .format((schedulingAttemptsArray.length * percentile).toInt,
                      schedulingAttemptsArray.length,
                      result))
      result
    } else {
      -1.0
    }
  }

  def numTaskSchedulingAttemptsPercentile(percentile: Double): Double = {
    assert(percentile <= 1.0 && percentile >= 0)
    logger.debug("largest 200 task scheduling attempt counts: " +
            jobs.map(_.numTaskSchedulingAttempts)
                .sorted
                .takeRight(200)
                .mkString(","))
    val scheduled = jobs.filter(_.numTaskSchedulingAttempts > 0)
    if (scheduled.nonEmpty) {
      val taskSchedulingAttemptsArray = new Array[Long](scheduled.length)
      scheduled.map(_.numTaskSchedulingAttempts).copyToArray(taskSchedulingAttemptsArray)
      util.Sorting.quickSort(taskSchedulingAttemptsArray)
      val result = taskSchedulingAttemptsArray(((taskSchedulingAttemptsArray.length-1) * 0.9).toInt)
      logger.debug(("Looking up num task scheduling attempts " +
               "percentile value at position %d of %d: %d.")
              .format((taskSchedulingAttemptsArray.length * percentile).toInt,
                      taskSchedulingAttemptsArray.length,
                      result))
      result
    } else {
      -1.0
    }
  }
}

case class WorkloadDesc(
                         cell: String,
                        assignmentPolicy: String,
                        // getJob(0) is called 
                        workloadGenerators: List[WorkloadGenerator],
                        cellStateDesc: CellStateDesc,
                        prefillWorkloadGenerators: List[WorkloadGenerator] =
                            List[WorkloadGenerator]()) {
  assert(!cell.contains(" "), "Cell names cannot have spaces in them.")
  assert(!assignmentPolicy.contains(" "),
         "Assignment policies cannot have spaces in them.")
  assert(prefillWorkloadGenerators.length ==
         prefillWorkloadGenerators.map(_.workloadName).toSet.size)

  val workloads:ListBuffer[Workload] = ListBuffer[Workload]()

  def generateWorkloads(timeWindow: Double): Unit ={
    workloadGenerators.foreach(workloadGenerator => {
      workloads += workloadGenerator.newWorkload(timeWindow)
    })
  }

  def cloneWorkloads(): ListBuffer[Workload] ={
    val newWorkloads:ListBuffer[Workload] = ListBuffer[Workload]()
    workloads.foreach(workload => {
      newWorkloads += workload.copy
    })
    newWorkloads
  }
}

object UniqueIDGenerator {
  var counter = 0
  def getUniqueID: Int = {
    counter += 1
    counter
  }
}

/**
 * A threadsafe Workload factory.
 */
trait WorkloadGenerator {
  val workloadName: String
  /**
   * Generate a workload using this workloadGenerator's parameters
   * The timestamps of the jobs in this workload should fall in the range
   * [0, timeWindow].
   *
   * @param maxCpus The maximum number of cpus that the workload returned
   *                can contain.
   * @param maxMem The maximum amount of mem that the workload returned
   *                can contain.
   * @param updatedAvgJobInterarrivalTime if non-None, then the interarrival
   *                                      times of jobs in the workload returned
   *                                      will be approximately this value.
   */
  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Double] = None,
                  maxMem: Option[Double] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None)
                 : Workload
}

/**
 * An object for building and caching empirical distributions
 * from traces. Caches them because they are expensive to build
 * (require reading from disk) and are re-used between different
 * experiments.
 */
object DistCache {
  val logger = Logger.getLogger(this.getClass.getName)
  val distributions =
      collection.mutable.Map[(String, String), Array[Double]]()

  def getDistribution(workloadName: String,
                      traceFileName: String): Array[Double] = {
    distributions.getOrElseUpdate((workloadName, traceFileName),
                                  buildDist(workloadName, traceFileName))
  }

  def buildDist(workloadName: String, traceFileName: String): Array[Double] = {
    assert(workloadName.equals("Batch") || workloadName.equals("Service") || workloadName.equals("Interactive") || workloadName.equals("Batch-MPI"))
    val dataPoints = new collection.mutable.ListBuffer[Double]()
    val refDistribution = new Array[Double](1001)

    var realDataSum: Double = 0.0
    if(traceFileName.equals("interactive-runtime-dist")){
      for(i <- 0 until 50){
        val dataPoint: Double = Random.nextInt(30)
        dataPoints.append(dataPoint)
        realDataSum += dataPoint
      }
      for(i <- 0 until 900){
        val dataPoint: Double = Random.nextInt(50400) + 30
        dataPoints.append(dataPoint)
        realDataSum += dataPoint
      }
      for(i <- 0 until 50){
        val dataPoint: Double = Random.nextInt(30) + 50430
        dataPoints.append(dataPoint)
        realDataSum += dataPoint
      }
    }else{
      logger.info(("Reading tracefile %s and building distribution of " +
        "job data points based on it.").format(traceFileName))
      val traceSrc = io.Source.fromFile(traceFileName)
      val lines = traceSrc.getLines()

      lines.foreach(line => {
        val parsedLine = line.split(" ")
        // The following parsing code is based on the space-delimited schema
        // used in textfile. See the README for a description.
        // val cell: Double = parsedLine(1).toDouble
        // val allocationPolicy: String = parsedLine(2)
        val isServiceJob: Boolean = parsedLine(2).equals("1")
        val dataPoint: Double = parsedLine(3).toDouble
        // Add the job to this workload if its job type is the same as
        // this generator's, which we determine based on workloadName.
        if ((isServiceJob && workloadName.equals("Service")) ||
          (!isServiceJob && workloadName.equals("Batch")) ||
          (!isServiceJob && workloadName.equals("Batch-MPI")) ||
          (isServiceJob && workloadName.equals("Interactive"))) {
          dataPoints.append(dataPoint)
          realDataSum += dataPoint
        }
      })
    }

    assert(dataPoints.nonEmpty,
           "Trace file must contain at least one data point.")
    logger.info(("Done reading tracefile of %d jobs, average of real data " +
             "points was %f. Now constructing distribution.")
            .format(dataPoints.length,
                    realDataSum/dataPoints.length))
    val dataPointsArray = dataPoints.toArray
    util.Sorting.quickSort(dataPointsArray)
    for (i <- 0 to 1000) {
      // Store summary quantiles.
      // 99.9 %tile = length * .999
      val index = ((dataPointsArray.length - 1) * i/1000.0).toInt
      val currPercentile =
        dataPointsArray(index)
      refDistribution(i) = currPercentile
      // println("refDistribution(%d) = dataPointsArray(%d) = %f"
      //         .format(i, index, currPercentile))
    }
    refDistribution
  }
}

object PrefillJobListsCache {
  val logger = Logger.getLogger(this.getClass.getName)
  // Map from (workloadname, traceFileName) -> list of jobs.
  val jobLists =
      collection.mutable.Map[(String, String), Iterable[Job]]()
  val cpusPerTaskDistributions =
      collection.mutable.Map[String, Array[Double]]()
  val memPerTaskDistributions =
      collection.mutable.Map[String, Array[Double]]()

  def getOrLoadJobs(workloadName: String,
                    traceFileName: String): Iterable[Job] = {
    val cachedJobs = jobLists.getOrElseUpdate((workloadName, traceFileName), {
      val newJobs = collection.mutable.Map[String, Job]()
      val traceSrc = io.Source.fromFile(traceFileName)
      val lines: Iterator[String] = traceSrc.getLines()
      lines.foreach(line => {
        val parsedLine = line.split(" ")
        // The following parsing code is based on the space-delimited schema
        // used in textfile. See the README Andy made for a description of it.

        // Parse the fields that are common between both (6 & 8 column)
        // row formats.
        val timestamp: Double = parsedLine(1).toDouble
        val jobID: String = parsedLine(2)
        val isHighPriority: Boolean = if(parsedLine(3).equals("1")) {true}
                                      else {false}
        val schedulingClass: Int = parsedLine(4).toInt
        // Label the job according to PBB workload split. SchedulingClass 0 & 1
        // are batch, 2 & 3 are service.
        // (isServiceJob == false) => this is a batch job.
        val isServiceJob = isHighPriority && schedulingClass != 0 && schedulingClass != 1
        // Add the job to this workload if its job type is the same as
        // this generator's, which we determine based on workloadName
        // and if we haven't reached the resource size limits of the
        // requested workload.
        if ((isServiceJob && workloadName.equals("PrefillService")) ||
            (!isServiceJob && workloadName.equals("PrefillBatch")) ||
            workloadName.equals("PrefillBatchService")) {
          if (parsedLine(0).equals("11")) {
            assert(parsedLine.length == 8, "Found %d fields, expecting %d"
                                           .format(parsedLine.length ,8))

            val numTasks: Int =parsedLine(5).toInt
            assert(numTasks != 0, "Number of tasks for job %s is %d".format(jobID, numTasks))

            val cpusPerJob: Double = parsedLine(6).toDouble
            // The tracefile has memory in bytes, our simulator is in GB.
            val memPerJob: Double = parsedLine(7).toDouble / 1024 / 1024 / 1024
            val newJob = Job(UniqueIDGenerator.getUniqueID,
                             0.0, // Submitted
                             numTasks,
                             -1, // to be replaced w/ simulator.runTime
                             workloadName,
                             cpusPerJob / numTasks,
                             memPerJob / numTasks)
            newJobs(jobID) = newJob
          // Update the job/task duration for jobs that we have end times for.
          } else if (parsedLine(0).equals("12")) {
            assert(parsedLine.length == 6, "Found %d fields, expecting %d"
                                           .format(parsedLine.length ,6))
            assert(newJobs.contains(jobID), "Expect to find job %s in newJobs."
                                            .format(jobID))
            newJobs(jobID).taskDuration = timestamp
          } else {
            logger.error("Invalid trace event type code %s in tracefile %s"
                      .format(parsedLine(0), traceFileName))
          }
        }
      })
      traceSrc.close()
      // Add the newly parsed list of jobs to the cache.
      logger.debug("loaded %d newJobs.".format(newJobs.size))
      newJobs.values
    })
    // Return a copy of the cached jobs since the job durations will
    // be updated according to the experiment time window.
    logger.debug("returning %d jobs from cache".format(cachedJobs.size))
    cachedJobs.map(_.copy())
  }

  /**
   * When we load jobs from a trace file, we fill in the duration for all jobs
   * that don't have an end event as -1, then we fill in the duration for all
   * such jobs.
   */
  def getJobs(workloadName: String,
              traceFileName: String,
              timeWindow: Double): Iterable[Job] = {
    val jobs = getOrLoadJobs(workloadName, traceFileName)
    // Update duration of jobs with duration set to -1.
    jobs.foreach(job => {
      if (job.taskDuration == -1)
        job.taskDuration = timeWindow
    })
    jobs
  }

  def buildDist(dataPoints: Array[Double]): Array[Double] = {
    val refDistribution = new Array[Double](1001)
    assert(dataPoints.length > 0,
           "dataPoints must contain at least one data point.")
    util.Sorting.quickSort(dataPoints)
    for (i <- 0 to 1000) {
      // Store summary quantiles. 99.9 %tile = length * .999
      val index = ((dataPoints.length - 1) * i/1000.0).toInt
      val currPercentile =
        dataPoints(index)
      refDistribution(i) = currPercentile
    }
    refDistribution
  }

  def getCpusPerTaskDistribution(workloadName: String,
                                 traceFileName: String): Array[Double] = {
    //TODO(andyk): Fix this ugly hack of prepending "Prefill".
    val jobs = getOrLoadJobs("Prefill" + workloadName, traceFileName)
    logger.trace("getCpusPerTaskDistribution called.")
    cpusPerTaskDistributions.getOrElseUpdate(
        traceFileName, buildDist(jobs.map(_.cpusPerTask).toArray))
  }

  def getMemPerTaskDistribution(workloadName: String,
                                traceFileName: String): Array[Double] = {
    //TODO(andyk): Fix this ugly hack of prepending "Prefill".
    val jobs = getOrLoadJobs("Prefill" + workloadName, traceFileName)
    logger.trace("getMemPerTaskDistribution called.")
    memPerTaskDistributions.getOrElseUpdate(
        traceFileName, buildDist(jobs.map(_.memPerTask).toArray))
  }
}

