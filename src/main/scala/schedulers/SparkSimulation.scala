/**
 * Copyright (c) 2013, Appier Inc.
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

package ClusterSchedulingSimulation.schedulers

import ClusterSchedulingSimulation.{CellState, Scheduler, _}

import scala.collection.mutable.HashMap

/* This class and its subclasses are used by factory method
 * ClusterSimulator.newScheduler() to determine which type of Simulator
 * to create and also to carry any extra fields that the factory needs to
 * construct the simulator.
 */
class SparkSimulatorDesc(schedulerDescs: Seq[SchedulerDesc],
                              runTime: Double)
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
    var schedulers = HashMap[String, Scheduler]()
    // Create schedulers according to experiment parameters.
    schedulerDescs.foreach(schedDesc => {
      // If any of the scheduler-workload pairs we're sweeping over
      // are for this scheduler, then apply them before
      // registering it.
      var constantThinkTimes = HashMap[String, Double](
          schedDesc.constantThinkTimes.toSeq: _*)
      var perTaskThinkTimes = HashMap[String, Double](
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
          new SparkScheduler(schedDesc.name,
                                  constantThinkTimes.toMap,
                                  perTaskThinkTimes.toMap,
                                  math.floor(newBlackListPercent *
                                    cellStateDesc.numMachines.toDouble).toInt)
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
                         logging)
  }
}

class SparkScheduler(name: String,
                          constantThinkTimes: Map[String, Double],
                          perTaskThinkTimes: Map[String, Double],
                          numMachinesToBlackList: Double = 0)
                         extends Scheduler(name,
                                           constantThinkTimes,
                                           perTaskThinkTimes,
                                           numMachinesToBlackList) {

  println("scheduler-id-info: %d, %s, %d, %s, %s"
          .format(Thread.currentThread().getId(),
                  name,
                  hashCode(),
                  constantThinkTimes.mkString(";"),
                  perTaskThinkTimes.mkString(";")))

  override
  def addJob(job: Job) = {
    assert(simulator != null, "This scheduler has not been added to a " +
                              "simulator yet.")
    super.addJob(job)
    job.lastEnqueued = simulator.currentTime
    pendingQueue.enqueue(job)
    simulator.logger.info("enqueued job " + job.id)
    if (!scheduling)
      scheduleNextJobAction()
  }

  /**
   * Checks to see if there is currently a job in this scheduler's job queue.
   * If there is, and this scheduler is not currently scheduling a job, then
   * pop that job off of the queue and "begin scheduling it". Scheduling a
   * job consists of setting this scheduler's state to scheduling = true, and
   * adding a finishSchedulingJobAction to the simulators event queue by
   * calling afterDelay().
   */
  def scheduleNextJobAction(): Unit = {
    assert(simulator != null, "This scheduler has not been added to a " +
                              "simulator yet.")
    if (!scheduling && !pendingQueue.isEmpty) {
      scheduling = true
      val job = pendingQueue.dequeue
      job.updateTimeInQueueStats(simulator.currentTime)
      job.lastSchedulingStartTime = simulator.currentTime
      val thinkTime = getThinkTime(job)
      simulator.logger.info("getThinkTime returned " + thinkTime)
      simulator.afterDelay(thinkTime) {
        simulator.logger.info(("Scheduler %s finished scheduling job %d. " +
                       "Attempting to schedule next job in scheduler's " +
                       "pendingQueue.").format(name, job.id))
        job.numSchedulingAttempts += 1
        job.numTaskSchedulingAttempts += job.unscheduledTasks
        val claimDeltas = scheduleJob(job, simulator.cellState)
        if(claimDeltas.length > 0) {
          simulator.cellState.scheduleEndEvents(claimDeltas)
          job.unscheduledTasks -= claimDeltas.length
          simulator.logger.info("scheduled %d tasks of job %d's, %d remaining."
                        .format(claimDeltas.length, job.id, job.unscheduledTasks))
          numSuccessfulTransactions += 1
          recordUsefulTimeScheduling(job,
                                     thinkTime,
                                     job.numSchedulingAttempts == 1)
        } else {
          simulator.logger.info(("No tasks scheduled for job %d (%f cpu %f mem) " +
                         "during this scheduling attempt, not recording " +
                         "any busy time. %d unscheduled tasks remaining.")
                        .format(job.id,
                                job.cpusPerTask,
                                job.memPerTask,
                                job.unscheduledTasks))
        }
        var jobEventType = "" // Set this conditionally below; used in logging.
        // If the job isn't yet fully scheduled, put it back in the queue.
        if (job.unscheduledTasks > 0) {
          simulator.logger.info(("Job %s didn't fully schedule, %d / %d tasks remain " +
                         "(shape: %f cpus, %f mem). Putting it " +
                         "back in the queue").format(job.id,
                                                     job.unscheduledTasks,
                                                     job.numTasks,
                                                     job.cpusPerTask,
                                                     job.memPerTask))
          // Give up on a job if (a) it hasn't scheduled a single task in
          // 100 tries or (b) it hasn't finished scheduling after 1000 tries.
          /*
          if ((job.numSchedulingAttempts > 100 &&
               job.unscheduledTasks == job.numTasks) ||
              job.numSchedulingAttempts > 1000) {
            println(("Abandoning job %d (%f cpu %f mem) with %d/%d " +
                   "remaining tasks, after %d scheduling " +
                   "attempts.").format(job.id,
                                       job.cpusPerTask,
                                       job.memPerTask,
                                       job.unscheduledTasks,
                                       job.numTasks,
                                       job.numSchedulingAttempts))
            numJobsTimedOutScheduling += 1
            jobEventType = "abandoned"
          } else {
            simulator.afterDelay(1) {
              addJob(job)
            }
          }
          */
          simulator.afterDelay(1) {
            addJob(job)
          }
        } else {
          // All tasks in job scheduled so don't put it back in pendingQueue.
          jobEventType = "fully-scheduled"
        }
        if (!jobEventType.equals("")) {
          // println("%s %s %d %s %d %d %f"
          //         .format(Thread.currentThread().getId(),
          //                 name,
          //                 hashCode(),
          //                 jobEventType,
          //                 job.id,
          //                 job.numSchedulingAttempts,
          //                 simulator.currentTime - job.submitted))
        }

        scheduling = false
        scheduleNextJobAction()
      }
      simulator.logger.info("Scheduler named '%s' started scheduling job %d "
                    .format(name,job.id))
    }
  }

  /**
   * Given a job and a cellstate, find machines that the tasks of the
   * job will fit into, and allocate the resources on that machine to
   * those tasks, accounting those resoures to this scheduler, modifying
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
   * @param  machineBlackList an array of of machineIDs that should be
   *                          ignored when scheduling (i.e. tasks will
   *                          not be scheduled on any of them).
   *
   * @return List of deltas, one per task, so that the transactions can
   *         be played on some other cellstate if desired.
   */
  override
  def scheduleJob(job: Job,
                  cellState: CellState): Seq[ClaimDelta] = {
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

    var remainingCandidates =
        math.max(0, cellState.numMachines - numMachinesToBlackList).toInt
    while(job.coresLeft > 0 && cellState.availableCpus > 0 && remainingCandidates > 0) {
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
        val locked = if (job.unscheduledTasks > 0) false else true

        val claimDelta = new ClaimDelta(this,
                                        currMachID,
                                        cellState.machineSeqNums(currMachID),
                                        job.taskDuration,
                                        job.cpusPerTask,
                                        job.memPerTask,
                                        locked)
        claimDelta.apply(cellState = cellState, locked = locked)
        if (!locked) {
          job.unscheduledTasks -= 1
        }
        claimDeltas += claimDelta
      } else {
        failedFindVictimAttempts += 1
        // Move the chosen candidate to the end of the range of
        // remainingCandidates so that we won't choose it again after we
        // decrement remainingCandidates. Do this by swapping it with the
        // machineID currently at position (remainingCandidates - 1)
        candidatePool(candidateIndex) = candidatePool(remainingCandidates - 1)
        candidatePool(remainingCandidates - 1) = currMachID
        remainingCandidates -= 1
        // simulator.logger.info(
        //     ("%s in scheduling algorithm, tried machine %d, but " +
        //      "%f cpus and %f mem are required, and it only " +
        //      "has %f cpus and %f mem available.")
        //      .format(name,
        //              currMachID,
        //              job.cpusPerTask,
        //              job.memPerTask,
        //              cellState.availableCpusPerMachine(currMachID),
        //              cellState.availableMemPerMachine(currMachID)))
      }
    }
    return claimDeltas
  }
}
