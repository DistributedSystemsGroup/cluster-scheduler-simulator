package ClusterSchedulingSimulation


import org.apache.commons.math.distribution.ExponentialDistributionImpl
import org.apache.commons.math3.distribution.LogNormalDistribution
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
  assert(workloadName.equals("PrefillBatch") || workloadName.equals("PrefillService") ||
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
                             jobsPerWorkload: Int = 10000,
                             scaleFactor: Int = 1,
                             allMoldable: Boolean = false,
                             introduceError:Boolean = false)
  extends WorkloadGenerator {
  val logger = Logger.getLogger(this.getClass.getName)
  logger.info("Generating %s Workload...".format(workloadName))
  assert(workloadName.equals("Batch") || workloadName.equals("Service") || workloadName.equals("Interactive") || workloadName.equals("Batch-MPI"))
  // Build the distributions from the input trace textfile that we'll
  // use to generate random job interarrival times.
  var interarrivalDist: Array[Double] =
  DistCache.getDistribution(workloadName, interarrivalTraceFileName)
  var tasksPerJobDist: Array[Double] =
    DistCache.getDistribution(workloadName, tasksPerJobTraceFileName)


  var jobDurationDist: Array[Double] = if(!workloadName.equals("Interactive")){
    DistCache.getDistribution(workloadName, jobDurationTraceFileName)
  }else{
    DistCache.getDistribution(workloadName, "interactive-runtime-dist")
  }


  val cpusPerTaskDist: Array[Double] =
    PrefillJobListsCache.getCpusPerTaskDistribution(workloadName,
      prefillTraceFileName)
  val memPerTaskDist: Array[Double] =
    PrefillJobListsCache.getMemPerTaskDistribution(workloadName,
      prefillTraceFileName)
  val randomNumberGenerator = new util.Random(Seed())

  def generateError(mu:Double = 0, sigma:Double = 0.5, factor:Double = 1): Double = {
    var error:Double = 1
    if(introduceError){
      val logNormal:LogNormalDistribution  = new LogNormalDistribution(mu, sigma)
      error = factor * logNormal.sample()
    }
    error

//    val randomNumberGenerator = util.Random
//    var error: Double = 1 - (randomNumberGenerator.nextDouble() * (randomNumberGenerator.nextInt(2) - 1))
//    while(error <= 0 && error >= 2)
//      error = 1 - (randomNumberGenerator.nextDouble() * (randomNumberGenerator.nextInt(2) - 1))
//    error
  }

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

  def newJob(submissionTime: Double, numMoldableTasks: Integer, timeWindow: Double): Job = {
    // Don't allow jobs with duration 0.
    var dur = 0.0
    while (dur <= 0.0 || dur >= timeWindow * 0.1) {
      dur = getQuantile(jobDurationDist, randomNumberGenerator.nextFloat)
      if (!workloadName.equals("Interactive"))
        dur *= 30
    }

    // Use ceil to avoid jobs with 0 tasks.
    var numTasks = math.ceil(getQuantile(tasksPerJobDist, randomNumberGenerator.nextFloat).toFloat).toInt
    assert(numTasks != 0, "Jobs must have at least one task.")
    if(workloadName.equals("Batch"))
      numTasks *= 10
    if(workloadName.equals("Interactive"))
      numTasks -= 1

    // Sample from the empirical distribution until we get task
    // cpu and mem sizes that are small enough.
    var cpusPerTask = 0.7 * getQuantile(cpusPerTaskDist, randomNumberGenerator.nextFloat).toFloat
    while (cpusPerTask.isNaN || cpusPerTask >= maxCpusPerTask || cpusPerTask == 0) {
      cpusPerTask = 0.7 * getQuantile(cpusPerTaskDist, randomNumberGenerator.nextFloat).toFloat
    }
    var memPerTask = 0.7 * getQuantile(memPerTaskDist, randomNumberGenerator.nextFloat).toFloat
    while (memPerTask.isNaN || memPerTask >= maxMemPerTask || memPerTask == 0) {
      memPerTask = 0.7 * getQuantile(memPerTaskDist, randomNumberGenerator.nextFloat).toFloat
    }
    logger.debug("New Job with %f cpusPerTask and %f memPerTask".format(cpusPerTask, memPerTask))
    Job(UniqueIDGenerator.getUniqueID,
      submissionTime,
      numTasks,
      dur,
      workloadName,
      cpusPerTask,
      memPerTask,
      numMoldableTasks = Option(numMoldableTasks),
      priority = randomNumberGenerator.nextInt(1000),
      error = generateError()
    )
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
    var nextJobSubmissionTime = getQuantile(interarrivalDist, randomNumberGenerator.nextFloat)
    var numJobs = 0
    // We will fill the workload with the number of job asked
    while (numJobs < jobsPerWorkload) {
      if (nextJobSubmissionTime < timeWindow) {
        var moldableTasks: Integer =  randomNumberGenerator.nextInt(5)
//        if(workloadName.equals("Interactive"))
//          moldableTasks = randomNumberGenerator.nextInt(3)
        if(allMoldable)
          moldableTasks = null
        for (i <- 1 to scaleFactor){
          val job = newJob(nextJobSubmissionTime, moldableTasks, timeWindow)
          assert(job.workloadName == workload.name)
          workload.addJob(job)
        }
        numJobs += 1
      }
      // For this type of WorkloadGenerator in which interarrival rate is
      // sampled from an empirical distribution, the
      // updatedAvgJobInterarrivalTime parameter represents a scaling factor
      // for the value sampled from the distribution.
      val newinterArrivalTime = updatedAvgJobInterarrivalTime.getOrElse(1.0) *
        getQuantile(interarrivalDist, randomNumberGenerator.nextFloat) * 100
      if (newinterArrivalTime + nextJobSubmissionTime < timeWindow * 0.5)
        nextJobSubmissionTime += newinterArrivalTime
      else{
        logger.warn("[%s] job submission going outside the time window. Resetting it.".format(workloadName))
        nextJobSubmissionTime = getQuantile(interarrivalDist, randomNumberGenerator.nextFloat)
      }

    }
//    assert(numJobs == workload.numJobs, "Num Jobs generated differs from what has been asked")
    workload.sortJobs()
    val lastJob = workload.getJobs.last
    logger.info("[%s] The last job arrives at %f and will finish at %f".format(workloadName, lastJob.submitted, lastJob.submitted + lastJob.jobDuration))

    workload
  }

  logger.info("Done generating %s Workload.\n".format(workloadName))
}

/**
  * Generates jobs at a uniform rate, of a uniform size.
  */
class UniformZoeWorkloadGenerator(
                                   val workloadName: String,
                                   initJobInterarrivalTime: Double,
                                   tasksPerJob: Int,
                                   jobDuration: Double,
                                   cpusPerTask: Double,
                                   memPerTask: Double,
                                   numMoldableTasks: Integer = null,
                                   jobsPerWorkload: Int = 10000,
                                   isRigid: Boolean = false)
  extends WorkloadGenerator {

  val logger = Logger.getLogger(this.getClass.getName)
  logger.info("Generating %s Workload...".format(workloadName))

  def newJob(submissionTime: Double, priority: Int): Job = {
    Job(UniqueIDGenerator.getUniqueID,
      submissionTime,
      tasksPerJob,
      jobDuration,
      workloadName,
      cpusPerTask,
      memPerTask,
      isRigid = isRigid,
      numMoldableTasks = Option(numMoldableTasks),
      priority = priority
    )
  }

  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Double] = None,
                  maxMem: Option[Double] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None): Workload = this.synchronized {
    assert(timeWindow >= 0)
    val jobInterArrivalTime = updatedAvgJobInterarrivalTime.getOrElse(1.0) * initJobInterarrivalTime
    val workload = new Workload(workloadName)
    var nextJobSubmissionTime = 0.0
    var numJobs = 0
    while (numJobs < jobsPerWorkload && nextJobSubmissionTime < timeWindow) {
      val job = newJob(nextJobSubmissionTime, numJobs)
      assert(job.workloadName == workload.name)
      workload.addJob(job)
      numJobs += 1
      nextJobSubmissionTime += jobInterArrivalTime
    }

    if(numJobs < jobsPerWorkload)
      logger.warn(("The number of job generated for workload %s is lower (%d) than asked (%d)." +
        "The time windows for the simulation is not large enough. Job inter-arrival time was set to %f.")
        .format(workload.name, numJobs, jobsPerWorkload, jobInterArrivalTime))
    workload
  }

  logger.info("Done generating %s Workload.\n".format(workloadName))
}

/**
  * Generates jobs at a uniform rate, of a uniform size.
  */
class FakeZoeWorkloadGenerator(
                                 val workloadName: String
                               )
  extends WorkloadGenerator {

  val logger = Logger.getLogger(this.getClass.getName)
  logger.info("Generating %s Workload...".format(workloadName))

  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Double] = None,
                  maxMem: Option[Double] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None): Workload = this.synchronized {
    assert(timeWindow >= 0)
    val workload = new Workload(workloadName)

    val jobA = Job(1,
      0,
      4,
      10,
      workloadName,
      0.1,
      12.8,
      isRigid = false,
      numMoldableTasks = Option(3)
    )
    workload.addJob(jobA)
    val jobB = Job(2,
      0,
      3,
      10,
      workloadName,
      0.1,
      12.8,
      isRigid = false,
      numMoldableTasks = Option(3)
    )
    workload.addJob(jobB)
    val jobC = Job(3,
      0,
      5,
      10,
      workloadName,
      0.1,
      12.8,
      isRigid = false,
      numMoldableTasks = Option(3)
    )
    workload.addJob(jobC)
    val jobD = Job(4,
      0,
      2,
      10,
      workloadName,
      0.1,
      12.8,
      isRigid = false,
      numMoldableTasks = Option(3)
    )
    workload.addJob(jobD)

    workload
  }

  logger.info("Done generating %s Workload.\n".format(workloadName))
}

class FakePreemptiveZoeWorkloadGenerator(
                                val workloadName: String
                              )
  extends WorkloadGenerator {

  val logger = Logger.getLogger(this.getClass.getName)
  logger.info("Generating %s Workload...".format(workloadName))

  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Double] = None,
                  maxMem: Option[Double] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None): Workload = this.synchronized {
    assert(timeWindow >= 0)
    val workload = new Workload(workloadName)

    val jobA = Job(1,
      0,
      4,
      20,
      workloadName,
      0.1,
      6.4,
      isRigid = false,
      numMoldableTasks = Option(6)
    )
    workload.addJob(jobA)
    val jobB = Job(2,
      0,
      2,
      25,
      workloadName,
      0.1,
      6.4,
      isRigid = false,
      numMoldableTasks = Option(6)
    )
    workload.addJob(jobB)
    val jobC = Job(3,
      0,
      8,
      12.5,
      workloadName,
      0.1,
      6.4,
      isRigid = false,
      numMoldableTasks = Option(8)
    )
    workload.addJob(jobC)
    val jobD = Job(4,
      5,
      1,
      7.5,
      workloadName,
      0.1,
      6.4,
      isRigid = false,
      numMoldableTasks = Option(19)
    )
    workload.addJob(jobD)
//    val jobE = Job(5,
//      5,
//      4,
//      5,
//      workloadName,
//      0.1,
//      6.4,
//      isRigid = false,
//      numMoldableTasks = Option(2)
//    )
//    workload.addJob(jobE)

    workload
  }

  logger.info("Done generating %s Workload.\n".format(workloadName))
}
