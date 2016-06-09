package ClusterSchedulingSimulation


import org.apache.commons.math.distribution.ExponentialDistributionImpl
import org.apache.log4j.Logger

/**
  * Generates a pre-fill workload based on an input trace from a cluster.
  * Given a workloadName, it has hard coded rules to split up jobs
  * in the input file according to the PBB style split based on
  * the jobs' priority level and schedulingClass.
  */
class PrefillPbbTraceWorkloadGenerator(val workloadName: String,
                                       traceFileName: String)
  extends WorkloadGenerator {
  val logger = Logger.getLogger(this.getClass.getName)
  assert(workloadName.equals("PrefillBatch") ||
    workloadName.equals("PrefillService") ||
    workloadName.equals("PrefillBatchService"))

  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Double] = None,
                  maxMem: Option[Double] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None)
  : Workload = this.synchronized {
    assert(updatedAvgJobInterarrivalTime.isEmpty)
    assert(timeWindow >= 0)

    val joblist = PrefillJobListsCache.getJobs(workloadName,
      traceFileName,
      timeWindow)
    val workload = new Workload(workloadName)
    //TODO(andyk): Make this more functional.
    def reachedMaxCpu(currCpus: Double) = maxCpus.exists(currCpus >= _)
    def reachedMaxMem(currMem: Double) = maxMem.exists(currMem >= _)
    var iter = joblist.toIterator
    var numCpus = 0.0
    var numMem = 0.0
    var counter = 0
    while (iter.hasNext) {
      counter += 1
      val nextJob = iter.next
      numCpus += nextJob.numTasks * nextJob.cpusPerTask
      numMem += nextJob.numTasks * nextJob.memPerTask
      if (reachedMaxCpu(numCpus) || reachedMaxMem(numMem)) {
        logger.debug("reachedMaxCpu = %s, reachedMaxMem = %s"
          .format(reachedMaxCpu(numCpus), reachedMaxMem(numMem)))
        iter = Iterator()
      } else {
        // Use copy to be sure we don't share state between runs of the
        // simulator.
        workload.addJob(nextJob.copy())
        iter = iter.drop(0)
      }
    }
    logger.debug("Returning workload with %f cpus and %f mem in total."
      .format(workload.getJobs.map(j => {
        j.numTasks * j.cpusPerTask
      }).sum,
        workload.getJobs.map(j => {
          j.numTasks * j.memPerTask
        }).sum))
    workload
  }

  def updateJobsArrivalTime(workload: Workload,
                            timeWindow: Double,
                            updatedAvgJobInterarrivalTime: Option[Double] = None
                           ): Unit = this.synchronized {
    //TODO(pace): implement the method
  }
}

/**
  * A thread-safe Workload factory. Generates workloads with jobs that have
  * interarrival rates, numTasks, and lengths sampled from exponential
  * distributions. Assumes that all tasks in a job are identical
  * (and so no per-task data is required).
  *
  * @param workloadName the name that will be assigned to Workloads produced by
  *                     this factory, and to the tasks they contain.
  * @param initAvgJobInterarrivalTime initial average inter-arrival time in
  *                                   seconds. This can be overridden by passing
  *                                   a non None value for
  *                                   updatedAvgJobInterarrivalTime to
  *                                   newWorkload().
  */
class ExpExpExpWorkloadGenerator(val workloadName: String,
                                 initAvgJobInterarrivalTime: Double,
                                 avgTasksPerJob: Double,
                                 avgJobDuration: Double,
                                 avgCpusPerTask: Double,
                                 avgMemPerTask: Double)
  extends WorkloadGenerator{
  val numTasksGenerator =
    new ExponentialDistributionImpl(avgTasksPerJob.toFloat)
  val durationGenerator = new ExponentialDistributionImpl(avgJobDuration)

  def newJob(submissionTime: Double): Job = {
    // Don't allow jobs with zero tasks.
    var dur = durationGenerator.sample()
    while (dur <= 0.0)
      dur = durationGenerator.sample()
    Job(UniqueIDGenerator.getUniqueID,
      submissionTime,
      // Use ceil to avoid jobs with 0 tasks.
      math.ceil(numTasksGenerator.sample().toFloat).toInt,
      dur,
      workloadName,
      avgCpusPerTask,
      avgMemPerTask)
  }

  /**
    * Synchronized so that Experiments, which can share this WorkloadGenerator,
    * can safely call newWorkload concurrently.
    */
  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Double] = None,
                  maxMem: Option[Double] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None)
  : Workload = this.synchronized {
    assert(maxCpus.isEmpty)
    assert(maxMem.isEmpty)
    assert(timeWindow >= 0)
    // Create the job-interarrival-time number generator using the
    // parameter passed in, if any, else use the default parameter.
    val avgJobInterarrivalTime =
      updatedAvgJobInterarrivalTime.getOrElse(initAvgJobInterarrivalTime)
    val interarrivalTimeGenerator =
      new ExponentialDistributionImpl(avgJobInterarrivalTime)
    val workload = new Workload(workloadName)
    // create a new list of jobs for the experiment runTime window
    // using the current WorkloadGenerator.
    var nextJobSubmissionTime = interarrivalTimeGenerator.sample()
    while (nextJobSubmissionTime < timeWindow) {
      val job = newJob(nextJobSubmissionTime)
      assert(job.workloadName == workload.name)
      workload.addJob(job)
      nextJobSubmissionTime += interarrivalTimeGenerator.sample()
    }
    workload
  }

  def updateJobsArrivalTime(workload: Workload,
                            timeWindow: Double,
                            updatedAvgJobInterarrivalTime: Option[Double] = None
                           ): Unit = this.synchronized {
    //TODO(pace): implement the method
  }
}

/**
  * A thread-safe Workload factory. Generates workloads with jobs that have
  * sizes and lengths sampled from exponential distributions. Assumes that
  * all tasks in a job are identical, so no per-task data is required.
  * Generates interarrival times by sampling from an empirical distribution
  * built from a tracefile containing the interarrival times of jobs
  * in a real cluster.
  */
class InterarrivalTimeTraceExpExpWLGenerator(val workloadName: String,
                                             traceFileName: String,
                                             avgTasksPerJob: Double,
                                             avgJobDuration: Double,
                                             avgCpusPerTask: Double,
                                             avgMemPerTask: Double,
                                             maxCpusPerTask: Double,
                                             maxMemPerTask: Double)
  extends WorkloadGenerator {
  assert(workloadName.equals("Batch") || workloadName.equals("Service"))
  // Build the distribution from the input trace textfile that we'll
  // use to generate random job interarrival times.
  val interarrivalTimes = new collection.mutable.ListBuffer[Double]()
  var refDistribution: Array[Double] =
    DistCache.getDistribution(workloadName, traceFileName)

  val numTasksGenerator =
    new ExponentialDistributionImpl(avgTasksPerJob.toFloat)
  val durationGenerator = new ExponentialDistributionImpl(avgJobDuration)
  val cpusPerTaskGenerator =
    new ExponentialDistributionImpl(avgCpusPerTask)
  val memPerTaskGenerator = new ExponentialDistributionImpl(avgMemPerTask)
  val randomNumberGenerator = new util.Random(Seed())

  /**
    * @param value quantile [0, 1] representing distribution quantile to return.
    */
  def getInterarrivalTime(value: Double): Double = {
    // Look up the two closest quantiles and interpolate.
    assert(value >= 0 || value <=1, "value must be >= 0 and <= 1.")
    val rawIndex = value * (refDistribution.length - 1)
    val interpAmount = rawIndex % 1
    if (interpAmount == 0) {
      refDistribution(rawIndex.toInt)
    } else {
      val below = refDistribution(math.floor(rawIndex).toInt)
      val above = refDistribution(math.ceil(rawIndex).toInt)
      below + interpAmount * (below + above)
    }
  }

  def newJob(submissionTime: Double): Job = {
    // Don't allow jobs with zero tasks.
    var dur = durationGenerator.sample()
    while (dur <= 0.0)
      dur = durationGenerator.sample()
    // Sample until we get task cpu and mem sizes that are small enough.
    var cpusPerTask = cpusPerTaskGenerator.sample()
    while (cpusPerTask >= maxCpusPerTask) {
      cpusPerTask = cpusPerTaskGenerator.sample()
    }
    var memPerTask = memPerTaskGenerator.sample()
    while (memPerTask >= maxMemPerTask) {
      memPerTask = memPerTaskGenerator.sample()
    }
    Job(UniqueIDGenerator.getUniqueID,
      submissionTime,
      // Use ceil to avoid jobs with 0 tasks.
      math.ceil(numTasksGenerator.sample().toFloat).toInt,
      dur,
      workloadName,
      cpusPerTask,
      memPerTask)
  }

  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Double] = None,
                  maxMem: Option[Double] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None)
  : Workload = this.synchronized {
    assert(updatedAvgJobInterarrivalTime.isEmpty)
    assert(timeWindow >= 0)
    assert(maxCpus.isEmpty)
    assert(maxMem.isEmpty)
    val workload = new Workload(workloadName)
    // create a new list of jobs for the experiment runTime window
    // using the current WorkloadGenerator.
    var nextJobSubmissionTime = getInterarrivalTime(randomNumberGenerator.nextFloat)
    var numJobs = 0
    while (nextJobSubmissionTime < timeWindow) {
      val job = newJob(nextJobSubmissionTime)
      assert(job.workloadName == workload.name)
      workload.addJob(job)
      nextJobSubmissionTime += getInterarrivalTime(randomNumberGenerator.nextFloat)
      numJobs += 1
    }
    assert(numJobs == workload.numJobs)
    workload
  }

  def updateJobsArrivalTime(workload: Workload,
                            timeWindow: Double,
                            updatedAvgJobInterarrivalTime: Option[Double] = None
                           ): Unit = this.synchronized {
    //TODO(pace): implement the method
  }
}

/**
  * A thread-safe Workload factory. Generates workloads with jobs that have
  * numTasks, duration, and interarrival_time set by sampling from an empirical
  * distribution built from a tracefile containing the interarrival times of
  * jobs in a real cluster. Task shapes are drawn from exponential distributions.
  * All tasks in a job are identical, so no per-task data is required.
  */
class TraceWLGenerator(val workloadName: String,
                       interarrivalTraceFileName: String,
                       tasksPerJobTraceFileName: String,
                       jobDurationTraceFileName: String,
                       avgCpusPerTask: Double,
                       avgMemPerTask: Double,
                       maxCpusPerTask: Double,
                       maxMemPerTask: Double)
  extends WorkloadGenerator {
  assert(workloadName.equals("Batch") || workloadName.equals("Service"))
  // Build the distributions from the input trace textfile that we'll
  // use to generate random job interarrival times.
  var interarrivalDist: Array[Double] =
    DistCache.getDistribution(workloadName, interarrivalTraceFileName)
  var tasksPerJobDist: Array[Double] =
    DistCache.getDistribution(workloadName, tasksPerJobTraceFileName)
  var jobDurationDist: Array[Double] =
    DistCache.getDistribution(workloadName, jobDurationTraceFileName)
  val cpusPerTaskGenerator =
    new ExponentialDistributionImpl(avgCpusPerTask)
  val memPerTaskGenerator = new ExponentialDistributionImpl(avgMemPerTask)
  val randomNumberGenerator = new util.Random(Seed())

  /**
    * @param value [0, 1] representing distribution quantile to return.
    */
  def getQuantile(empDistribution: Array[Double],
                  value: Double): Double = {
    // Look up the two closest quantiles and interpolate.
    assert(value >= 0 || value <=1, "value must be >= 0 and <= 1.")
    val rawIndex = value * (empDistribution.length - 1)
    val interpAmount = rawIndex % 1
    if (interpAmount == 0) {
      empDistribution(rawIndex.toInt)
    } else {
      val below = empDistribution(math.floor(rawIndex).toInt)
      val above = empDistribution(math.ceil(rawIndex).toInt)
      below + interpAmount * (below + above)
    }
  }

  def newJob(submissionTime: Double): Job = {
    // Don't allow jobs with zero tasks.
    var dur = 0.0
    while (dur <= 0.0)
      dur = getQuantile(jobDurationDist, randomNumberGenerator.nextFloat)
    // Use ceil to avoid jobs with 0 tasks.
    val numTasks =
      math.ceil(getQuantile(tasksPerJobDist, randomNumberGenerator.nextFloat).toFloat).toInt
    assert(numTasks != 0, "Jobs must have at least one task.")
    // Sample until we get task cpu and mem sizes that are small enough.
    var cpusPerTask = cpusPerTaskGenerator.sample()
    while (cpusPerTask >= maxCpusPerTask) {
      cpusPerTask = cpusPerTaskGenerator.sample()
    }
    var memPerTask = memPerTaskGenerator.sample()
    while (memPerTask >= maxMemPerTask) {
      memPerTask = memPerTaskGenerator.sample()
    }
    Job(UniqueIDGenerator.getUniqueID,
      submissionTime,
      numTasks,
      dur,
      workloadName,
      cpusPerTask,
      memPerTask)
  }

  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Double] = None,
                  maxMem: Option[Double] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None)
  : Workload = this.synchronized {
    assert(updatedAvgJobInterarrivalTime.isEmpty)
    assert(timeWindow >= 0)
    assert(maxCpus.isEmpty)
    assert(maxMem.isEmpty)
    // Reset the randomNumberGenerator using the global seed so that
    // the same workload will be generated each time newWorkload is
    // called with the same parameters. This will ensure that
    // Experiments run in different threads will get the same
    // workloads and be a bit more fair to compare to each other.
    randomNumberGenerator.setSeed(Seed())
    val workload = new Workload(workloadName)
    // create a new list of jobs for the experiment runTime window
    // using the current WorkloadGenerator.
    var nextJobSubmissionTime = getQuantile(interarrivalDist, randomNumberGenerator.nextFloat)
    var numJobs = 0
    while (nextJobSubmissionTime < timeWindow) {
      val job = newJob(nextJobSubmissionTime)
      assert(job.workloadName == workload.name)
      workload.addJob(job)
      nextJobSubmissionTime += getQuantile(interarrivalDist, randomNumberGenerator.nextFloat)
      numJobs += 1
    }
    assert(numJobs == workload.numJobs)
    workload
  }

  def updateJobsArrivalTime(workload: Workload,
                            timeWindow: Double,
                            updatedAvgJobInterarrivalTime: Option[Double] = None
                           ): Unit = this.synchronized {
    //TODO(pace): implement the method
  }
}

/**
  * A thread-safe Workload factory. Generates workloads with jobs that have
  * numTasks, duration, and interarrival_time set by sampling from an empirical
  * distribution built from a tracefile containing the interarrival times of
  * jobs in a real cluster. Task shapes are drawn from empirical distributions
  * built from a prefill trace file. All tasks in a job are identical, so no
  * per-task data is required.
  */
class TraceAllWLGenerator(val workloadName: String,
                          interarrivalTraceFileName: String,
                          tasksPerJobTraceFileName: String,
                          jobDurationTraceFileName: String,
                          prefillTraceFileName: String,
                          maxCpusPerTask: Double,
                          maxMemPerTask: Double,
                          maxJobsPerWorkload: Int = 10000)
  extends WorkloadGenerator {
  val logger = Logger.getLogger(this.getClass.getName)
  logger.info("Generating %s Workload...".format(workloadName))
  assert(workloadName.equals("Batch") || workloadName.equals("Service"))
  // Build the distributions from the input trace textfile that we'll
  // use to generate random job interarrival times.
  var interarrivalDist: Array[Double] =
    DistCache.getDistribution(workloadName, interarrivalTraceFileName)

  var tasksPerJobDist: Array[Double] =
    DistCache.getDistribution(workloadName, tasksPerJobTraceFileName)

  //  var tasksPerJobDist: Array[Double] =
  //      (1 until Workloads.maxTasksPerJob).toArray.map(_.toDouble)


  var jobDurationDist: Array[Double] =
    DistCache.getDistribution(workloadName, jobDurationTraceFileName)
  val cpusPerTaskDist: Array[Double] =
    PrefillJobListsCache.getCpusPerTaskDistribution(workloadName,
      prefillTraceFileName)
  val memPerTaskDist: Array[Double] =
    PrefillJobListsCache.getMemPerTaskDistribution(workloadName,
      prefillTraceFileName)
  val randomNumberGenerator = new util.Random(Seed())

  /**
    * @param value [0, 1] representing distribution quantile to return.
    */
  def getQuantile(empDistribution: Array[Double],
                  value: Double): Double = {
    // Look up the two closest quantiles and interpolate.
    assert(value >= 0 || value <=1, "value must be >= 0 and <= 1.")
    val rawIndex = value * (empDistribution.length - 1)
    val interpAmount = rawIndex % 1
    if (interpAmount == 0) {
      empDistribution(rawIndex.toInt)
    } else {
      val below = empDistribution(math.floor(rawIndex).toInt)
      val above = empDistribution(math.ceil(rawIndex).toInt)
      below + interpAmount * (below + above)
    }
  }

  def newJob(submissionTime: Double): Job = {
    // Don't allow jobs with duration 0.
    var dur = 0.0
    while (dur <= 0.0)
      dur = getQuantile(jobDurationDist, randomNumberGenerator.nextFloat)
    // Use ceil to avoid jobs with 0 tasks.
    val numTasks = math.ceil(getQuantile(tasksPerJobDist,
      randomNumberGenerator.nextFloat).toFloat).toInt
    assert(numTasks != 0, "Jobs must have at least one task.")
    // Sample from the empirical distribution until we get task
    // cpu and mem sizes that are small enough.
    var cpusPerTask = 0.7 * getQuantile(cpusPerTaskDist,
      randomNumberGenerator.nextFloat).toFloat
    while (cpusPerTask.isNaN || cpusPerTask >= maxCpusPerTask) {
      cpusPerTask = 0.7 * getQuantile(cpusPerTaskDist,
        randomNumberGenerator.nextFloat).toFloat
    }
    var memPerTask = 0.7 * getQuantile(memPerTaskDist,
      randomNumberGenerator.nextFloat).toFloat
    while (memPerTask.isNaN || memPerTask >= maxMemPerTask) {
      memPerTask = 0.7 * getQuantile(memPerTaskDist,
        randomNumberGenerator.nextFloat).toFloat
    }
    logger.debug("New Job with %f cpusPerTask and %f memPerTask".format(cpusPerTask, memPerTask))
    Job(UniqueIDGenerator.getUniqueID,
      submissionTime,
      numTasks,
      dur,
      workloadName,
      cpusPerTask,
      memPerTask)
  }

  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Double] = None,
                  maxMem: Option[Double] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None)
  : Workload = this.synchronized {
    assert(timeWindow >= 0)
    assert(maxCpus.isEmpty)
    assert(maxMem.isEmpty)
    // Reset the randomNumberGenerator using the global seed so that
    // the same workload will be generated each time newWorkload is
    // called with the same parameters. This will ensure that
    // Experiments run in different threads will get the same
    // workloads and be a bit more fair to compare to each other.
    randomNumberGenerator.setSeed(Seed())
    val workload = new Workload(workloadName)
    // create a new list of jobs for the experiment runTime window
    // using the current WorkloadGenerator.
    var nextJobSubmissionTime = getQuantile(interarrivalDist,
      randomNumberGenerator.nextFloat)
    var numJobs = 0
    while (numJobs < maxJobsPerWorkload) {
      if (nextJobSubmissionTime < timeWindow) {
        val job = newJob(nextJobSubmissionTime)
        assert(job.workloadName == workload.name)
        workload.addJob(job)
        numJobs += 1
      }
      // For this type of WorkloadGenerator in which interarrival rate is
      // sampled from an empirical distribution, the
      // updatedAvgJobInterarrivalTime parameter represents a scaling factor
      // for the value sampled from the distribution.
      val newinterArrivalTime = updatedAvgJobInterarrivalTime.getOrElse(1.0) *
        getQuantile(interarrivalDist,
          randomNumberGenerator.nextFloat)
      if (newinterArrivalTime + nextJobSubmissionTime < timeWindow)
        nextJobSubmissionTime += newinterArrivalTime
    }
    assert(numJobs == workload.numJobs)
    workload
  }

  def updateJobsArrivalTime(workload: Workload,
                            timeWindow: Double,
                            updatedAvgJobInterarrivalTime: Option[Double] = None
                           ): Unit = this.synchronized {
    // Reset the randomNumberGenerator using the global seed so that
    // the same workload will be generated each time newWorkload is
    // called with the same parameters. This will ensure that
    // Experiments run in different threads will get the same
    // workloads and be a bit more fair to compare to each other.
    randomNumberGenerator.setSeed(Seed())
    // update the list of jobs for the experiment runTime window
    // using the current WorkloadGenerator.
    var nextJobSubmissionTime = getQuantile(interarrivalDist,
      randomNumberGenerator.nextFloat)
    for(job <- workload.getJobs) {
      if (nextJobSubmissionTime < timeWindow) {
        job.submitted = nextJobSubmissionTime
      }
      // For this type of WorkloadGenerator in which interarrival rate is
      // sampled from an empirical distribution, the
      // updatedAvgJobInterarrivalTime parameter represents a scaling factor
      // for the value sampled from the distribution.
      val newinterArrivalTime = updatedAvgJobInterarrivalTime.getOrElse(1.0) *
        getQuantile(interarrivalDist,
          randomNumberGenerator.nextFloat)
      if (newinterArrivalTime + nextJobSubmissionTime < timeWindow)
        nextJobSubmissionTime += newinterArrivalTime
    }
  }

  logger.info("Done generating %s Workload.\n".format(workloadName))
}

/**
* Generates jobs at a uniform rate, of a uniform size.
*/
class UniformWorkloadGenerator(val workloadName: String,
                               initJobInterarrivalTime: Double,
                               tasksPerJob: Int,
                               jobDuration: Double,
                               cpusPerTask: Double,
                               memPerTask: Double,
                               isRigid: Boolean = false)
  extends WorkloadGenerator {
  def newJob(submissionTime: Double): Job = {
    Job(UniqueIDGenerator.getUniqueID,
      submissionTime,
      tasksPerJob,
      jobDuration,
      workloadName,
      cpusPerTask,
      memPerTask,
      isRigid)
  }
  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Double] = None,
                  maxMem: Option[Double] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None)
  : Workload = this.synchronized {
    assert(timeWindow >= 0)
    val jobInterarrivalTime = updatedAvgJobInterarrivalTime
      .getOrElse(initJobInterarrivalTime)
    val workload = new Workload(workloadName)
    var nextJobSubmissionTime = 0.0
    while (nextJobSubmissionTime < timeWindow) {
      val job = newJob(nextJobSubmissionTime)
      assert(job.workloadName == workload.name)
      workload.addJob(job)
      nextJobSubmissionTime += jobInterarrivalTime
    }
    workload
  }

  def updateJobsArrivalTime(workload: Workload,
                            timeWindow: Double,
                            updatedAvgJobInterarrivalTime: Option[Double] = None
                           ): Unit = this.synchronized {
    //TODO(pace): implement the method
  }
}

/**
  * A thread-safe Workload factory. Generates workloads with jobs that have
  * numTasks, duration, and interarrival_time set by sampling from an empirical
  * distribution built from a tracefile containing the interarrival times of
  * jobs in a real cluster. Task shapes are drawn from empirical distributions
  * built from a prefill trace file. All tasks in a job are identical, so no
  * per-task data is required.
  *
  * In particular in this generator it is implemented the applications that are
  * used in Zoe. Thus a job is actually a Zoe Applications and the task are Zoe
  * Services.
  */
class TraceAllZoeWLGenerator(val workloadName: String,
                             interarrivalTraceFileName: String,
                             tasksPerJobTraceFileName: String,
                             jobDurationTraceFileName: String,
                             prefillTraceFileName: String,
                             maxCpusPerTask: Double,
                             maxMemPerTask: Double,
                             maxJobsPerWorkload: Int = 10000)
  extends WorkloadGenerator {
  val logger = Logger.getLogger(this.getClass.getName)
  logger.info("Generating %s Workload...".format(workloadName))
  assert(workloadName.equals("Batch") || workloadName.equals("Service"))
  // Build the distributions from the input trace textfile that we'll
  // use to generate random job interarrival times.
  var interarrivalDist: Array[Double] =
    DistCache.getDistribution(workloadName, interarrivalTraceFileName)

  var tasksPerJobDist: Array[Double] =
    DistCache.getDistribution(workloadName, tasksPerJobTraceFileName)

  //  var tasksPerJobDist: Array[Double] =
  //      (1 until Workloads.maxTasksPerJob).toArray.map(_.toDouble)


  var jobDurationDist: Array[Double] =
    DistCache.getDistribution(workloadName, jobDurationTraceFileName)
  val cpusPerTaskDist: Array[Double] =
    PrefillJobListsCache.getCpusPerTaskDistribution(workloadName,
      prefillTraceFileName)
  val memPerTaskDist: Array[Double] =
    PrefillJobListsCache.getMemPerTaskDistribution(workloadName,
      prefillTraceFileName)
  val randomNumberGenerator = new util.Random(Seed())

  /**
    * @param value [0, 1] representing distribution quantile to return.
    */
  def getQuantile(empDistribution: Array[Double],
                  value: Double): Double = {
    // Look up the two closest quantiles and interpolate.
    assert(value >= 0 || value <=1, "value must be >= 0 and <= 1.")
    val rawIndex = value * (empDistribution.length - 1)
    val interpAmount = rawIndex % 1
    if (interpAmount == 0) {
      empDistribution(rawIndex.toInt)
    } else {
      val below = empDistribution(math.floor(rawIndex).toInt)
      val above = empDistribution(math.ceil(rawIndex).toInt)
      below + interpAmount * (below + above)
    }
  }

  def newJob(submissionTime: Double): Job = {
    // Don't allow jobs with duration 0.
    var dur = 0.0
    while (dur <= 0.0)
      dur = getQuantile(jobDurationDist, randomNumberGenerator.nextFloat)
    // Use ceil to avoid jobs with 0 tasks.
    val numTasks = math.ceil(getQuantile(tasksPerJobDist,
      randomNumberGenerator.nextFloat).toFloat).toInt
    assert(numTasks != 0, "Jobs must have at least one task.")
    // Sample from the empirical distribution until we get task
    // cpu and mem sizes that are small enough.
    var cpusPerTask = 0.7 * getQuantile(cpusPerTaskDist,
      randomNumberGenerator.nextFloat).toFloat
    while (cpusPerTask.isNaN || cpusPerTask >= maxCpusPerTask) {
      cpusPerTask = 0.7 * getQuantile(cpusPerTaskDist,
        randomNumberGenerator.nextFloat).toFloat
    }
    var memPerTask = 0.7 * getQuantile(memPerTaskDist,
      randomNumberGenerator.nextFloat).toFloat
    while (memPerTask.isNaN || memPerTask >= maxMemPerTask) {
      memPerTask = 0.7 * getQuantile(memPerTaskDist,
        randomNumberGenerator.nextFloat).toFloat
    }
    logger.debug("New Job with %f cpusPerTask and %f memPerTask".format(cpusPerTask, memPerTask))
    val job = Job(UniqueIDGenerator.getUniqueID,
      submissionTime,
      numTasks,
      dur,
      workloadName,
      cpusPerTask,
      memPerTask)

    // We set the number of moldable tasks that compose the Zoe Applications
    job.setMoldableTasksPercentage(randomNumberGenerator.nextInt(100))

    job
  }

  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Double] = None,
                  maxMem: Option[Double] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None)
  : Workload = this.synchronized {
    assert(timeWindow >= 0)
    assert(maxCpus.isEmpty)
    assert(maxMem.isEmpty)
    // Reset the randomNumberGenerator using the global seed so that
    // the same workload will be generated each time newWorkload is
    // called with the same parameters. This will ensure that
    // Experiments run in different threads will get the same
    // workloads and be a bit more fair to compare to each other.
    randomNumberGenerator.setSeed(Seed())
    val workload = new Workload(workloadName)
    // create a new list of jobs for the experiment runTime window
    // using the current WorkloadGenerator.
    var nextJobSubmissionTime = getQuantile(interarrivalDist,
      randomNumberGenerator.nextFloat)
    var numJobs = 0
    while (numJobs < maxJobsPerWorkload) {
      if (nextJobSubmissionTime < timeWindow) {
        val job = newJob(nextJobSubmissionTime)
        assert(job.workloadName == workload.name)
        workload.addJob(job)
        numJobs += 1
      }
      // For this type of WorkloadGenerator in which interarrival rate is
      // sampled from an empirical distribution, the
      // updatedAvgJobInterarrivalTime parameter represents a scaling factor
      // for the value sampled from the distribution.
      val newinterArrivalTime = updatedAvgJobInterarrivalTime.getOrElse(1.0) *
        getQuantile(interarrivalDist,
          randomNumberGenerator.nextFloat)
      if (newinterArrivalTime + nextJobSubmissionTime < timeWindow)
        nextJobSubmissionTime += newinterArrivalTime
    }
    assert(numJobs == workload.numJobs)
    workload
  }

  def updateJobsArrivalTime(workload: Workload,
                            timeWindow: Double,
                            updatedAvgJobInterarrivalTime: Option[Double] = None
                           ): Unit = this.synchronized {
    // Reset the randomNumberGenerator using the global seed so that
    // the same workload will be generated each time newWorkload is
    // called with the same parameters. This will ensure that
    // Experiments run in different threads will get the same
    // workloads and be a bit more fair to compare to each other.
    randomNumberGenerator.setSeed(Seed())
    // update the list of jobs for the experiment runTime window
    // using the current WorkloadGenerator.
    var nextJobSubmissionTime = getQuantile(interarrivalDist,
      randomNumberGenerator.nextFloat)
    for(job <- workload.getJobs) {
      if (nextJobSubmissionTime < timeWindow) {
        job.submitted = nextJobSubmissionTime
      }
      // For this type of WorkloadGenerator in which interarrival rate is
      // sampled from an empirical distribution, the
      // updatedAvgJobInterarrivalTime parameter represents a scaling factor
      // for the value sampled from the distribution.
      val newinterArrivalTime = updatedAvgJobInterarrivalTime.getOrElse(1.0) *
        getQuantile(interarrivalDist,
          randomNumberGenerator.nextFloat)
      if (newinterArrivalTime + nextJobSubmissionTime < timeWindow)
        nextJobSubmissionTime += newinterArrivalTime
    }
  }

  logger.info("Done generating %s Workload.\n".format(workloadName))
}
