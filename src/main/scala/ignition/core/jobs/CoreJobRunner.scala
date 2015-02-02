package ignition.core.jobs

import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTimeZone, DateTime}

import scala.util.Try

object CoreJobRunner {

  case class RunnerContext(sparkContext: SparkContext,
                           config: RunnerConfig)


  case class RunnerConfig(setupName: String = "nosetup",
                          date: DateTime = DateTime.now.withZone(DateTimeZone.UTC),
                          tag: String = "notag",
                          user: String = "nouser",
                          master: String = "local[*]",
                          executorMemory: String = "2G",
                          additionalArgs: Map[String, String] = Map.empty)

  def runJobSetup(args: Array[String], jobsSetups: Map[String, (RunnerContext) => Unit]) {
    val parser = new scopt.OptionParser[RunnerConfig]("Runner") {
      help("help") text("prints this usage text")
      arg[String]("<setup-name>") required() action { (x, c) =>
        c.copy(setupName = x)
      } text(s"one of ${jobsSetups.keySet}")
      // Note: we use runner-option name because when passing args to spark-submit we need to avoid name conflicts
      opt[String]('d', "runner-date") action { (x, c) =>
        c.copy(date = new DateTime(x))
      }
      opt[String]('t', "runner-tag") action { (x, c) =>
        c.copy(tag = x)
      }
      opt[String]('u', "runner-user") action { (x, c) =>
        c.copy(user = x)
      }
      opt[String]('m', "runner-master") action { (x, c) =>
        c.copy(master = x)
      }
      opt[String]('e', "runner-executor-memory") action { (x, c) =>
        c.copy(executorMemory = x)
      }

      opt[(String, String)]('w', "runner-with-arg") unbounded() action { (x, c) =>
        c.copy(additionalArgs = c.additionalArgs ++ Map(x))
      }
    }

    parser.parse(args, RunnerConfig()) map { config =>
      val jobSetup = jobsSetups.get(config.setupName)

      require(jobSetup.isDefined,
        s"Invalid job setup ${config.setupName}, available jobs setups: ${jobsSetups.keySet}")

      val appName = s"${config.setupName}.${config.tag}"


      val sparkConf = new SparkConf
//      val sparkConf = new SparkConf
      sparkConf.setMaster(config.master)
      sparkConf.setAppName(appName)
      //sparkConf.setJars(SparkContext.jarOfClass(this.getClass).toSeq)
      sparkConf.set("spark.executor.memory", config.executorMemory)
      sparkConf.set("spark.logConf", "true")
      sparkConf.set("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=/mnt -verbose:gc -XX:-PrintGCDetails -XX:+PrintGCTimeStamps -XX:-UseGCOverheadLimit")
      //sparkConf.set("spark.speculation", "true")
      sparkConf.set("spark.akka.frameSize", "500")
      sparkConf.set("spark.files.userClassPathFirst", "true")
//
      sparkConf.set("spark.default.parallelism", "1600")
      sparkConf.set("spark.shuffle.memoryFraction", "0.2")
      sparkConf.set("spark.storage.memoryFraction", "0.3")
      //sparkConf.set("spark.reducer.maxMbInFlight", "15")
      sparkConf.set("spark.hadoop.validateOutputSpecs", "true")
      //sparkConf.set("spark.storage.blockManagerSlaveTimeoutMs", "120000")
//      sparkConf.set("spark.eventLog.enabled", "true") // not supported on 1.0.2
      //sparkConf.set("spark.core.connection.ack.wait.timeout", "600")
      //sparkConf.set("spark.shuffle.spill.compress", "false")

      val sc = new SparkContext(sparkConf)


      val context = RunnerContext(sc, config)

      try {
        jobSetup.get.apply(context)
      } catch {
        case t: Throwable =>
          t.printStackTrace()
          System.exit(1) // force exit of all threads
      }
      Try { sc.stop() }
      System.exit(0) // force exit of all threads
    }
  }
}
