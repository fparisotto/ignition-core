package ignition.core.jobs.utils

import ignition.core.utils.ByteUtils
import ignition.core.utils.CollectionUtils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.hadoop.fs.{FileStatus, Path, FileSystem}
import org.apache.spark.rdd.{UnionRDD, RDD}
import org.joda.time.DateTime
import ignition.core.utils.DateUtils._

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.reflect.ClassTag
import scala.util.Try

object SparkContextUtils {

  case class Bucket(var size: Long, paths: ArrayBuffer[String])
  case class S3File(path: String, isDir: Boolean, size: Long)

  implicit class SparkContextImprovements(sc: SparkContext) {

    private def getFileSystem(path: Path): FileSystem = {
      path.getFileSystem(sc.hadoopConfiguration)
    }

    private def getStatus(commaSeparatedPaths: String, removeEmpty: Boolean): Seq[FileStatus] = {
      val paths = ignition.core.utils.HadoopUtils.getPathStrings(commaSeparatedPaths).map(new Path(_)).toSeq
      val fs = getFileSystem(paths.head)
      for {
        path <- paths
        status <- Option(fs.globStatus(path)).getOrElse(Array.empty).toSeq
        if status.isDirectory || !removeEmpty || status.getLen > 0 // remove empty files if necessary
      } yield status
    }

    private def delete(path: Path): Unit = {
      val fs = getFileSystem(path)
      fs.delete(path, true)
    }

    // This call is equivalent to a ls -d in shell, but won't fail if part of a path matches nothing,
    // For instance, given path = s3n://bucket/{a,b}, it will work fine if a exists but b is missing
    def sortedGlobPath(_paths: Seq[String], removeEmpty: Boolean = true): Seq[String] = {
      val paths = _paths.flatMap(path => ignition.core.utils.HadoopUtils.getPathStrings(path))
      paths.flatMap(p => getStatus(p, removeEmpty)).map(_.getPath.toString).distinct.sorted
    }

    // This function will expand the paths then group they and give to RDDs
    // We group to avoid too many RDDs on union (each RDD take some memory on driver)
    // We avoid passing a path too big to one RDD to avoid a Hadoop bug where just part of the path is processed when the path is big
    private def processPaths[T:ClassTag](f: (String) => RDD[T], paths: Seq[String], minimumPaths: Int): RDD[T] = {
      val splittedPaths = paths.flatMap(ignition.core.utils.HadoopUtils.getPathStrings)
      if (splittedPaths.size < minimumPaths)
        throw new Exception(s"Not enough paths found for $paths")

      val rdds = splittedPaths.grouped(5000).map(pathGroup => f(pathGroup.mkString(",")))

      new UnionRDD(sc, rdds.toList)
    }

    private def processTextFiles(paths: Seq[String], minimumPaths: Int): RDD[String] = {
      processPaths((p) => sc.textFile(p), paths, minimumPaths)
    }

    private def filterPaths(paths: Seq[String],
                            requireSuccess: Boolean,
                            inclusiveStartDate: Boolean,
                            startDate: Option[DateTime],
                            inclusiveEndDate: Boolean,
                            endDate: Option[DateTime],
                            lastN: Option[Int],
                            ignoreMalformedDates: Boolean)(implicit dateExtractor: PathDateExtractor): Seq[String] = {
      val sortedPaths = sortedGlobPath(paths)
      val filteredByDate = if (startDate.isEmpty && endDate.isEmpty)
        sortedPaths
      else
        sortedPaths.filter { p =>
          val tryDate = Try { dateExtractor.extractFromPath(p) }
          if (tryDate.isFailure && ignoreMalformedDates)
            false
          else {
            val date = tryDate.get
            val goodStartDate = startDate.isEmpty || (inclusiveStartDate && date.saneEqual(startDate.get) || date.isAfter(startDate.get))
            val goodEndDate = endDate.isEmpty || (inclusiveEndDate && date.saneEqual(endDate.get) || date.isBefore(endDate.get))
            goodStartDate && goodEndDate
          }
        }

      // Use a stream here to avoid checking the success if we are going to just take a few files
      val filteredBySuccessAndReversed = filteredByDate.reverse.toStream.dropWhile(p => requireSuccess && sortedGlobPath(Seq(s"$p/{_SUCCESS,_FINISHED}"), removeEmpty = false).isEmpty)

      if (lastN.isDefined)
        filteredBySuccessAndReversed.take(lastN.get).reverse.toList
      else
        filteredBySuccessAndReversed.reverse.toList
    }



    def getFilteredPaths(paths: Seq[String],
                         requireSuccess: Boolean,
                         inclusiveStartDate: Boolean,
                         startDate: Option[DateTime],
                         inclusiveEndDate: Boolean,
                         endDate: Option[DateTime],
                         lastN: Option[Int],
                         ignoreMalformedDates: Boolean)(implicit dateExtractor: PathDateExtractor): Seq[String] = {
      require(lastN.isEmpty || endDate.isDefined, "If you are going to get the last files, better specify the end date to avoid getting files in the future")
      filterPaths(paths, requireSuccess, inclusiveStartDate, startDate, inclusiveEndDate, endDate, lastN, ignoreMalformedDates)
    }


    lazy val hdfsPathPrefix = sc.master.replaceFirst("spark://(.*):7077", "hdfs://$1:9000/")

    def synchToHdfs(paths: Seq[String], pathsToRdd: (Seq[String], Int) => RDD[String], forceSynch: Boolean): Seq[String] = {
      val filesToOutput = 1500
      def mapPaths(actionWhenNeedsSynching: (String, String) => Unit): Seq[String] = {
        paths.map(p => {
          val hdfsPath = p.replace("s3n://", hdfsPathPrefix)
          if (forceSynch || getStatus(hdfsPath, false).isEmpty || getStatus(s"$hdfsPath/*", true).filterNot(_.isDirectory).size != filesToOutput) {
            val _hdfsPath = new Path(hdfsPath)
            actionWhenNeedsSynching(p, hdfsPath)
          }
          hdfsPath
        })
      }
      // We delete first because we may have two paths in the same parent
      mapPaths((p, hdfsPath) => delete(new Path(hdfsPath).getParent))// delete parent to avoid old files being accumulated
      // FIXME: We should be using a variable from the SparkContext, not a hard coded value (1500).
      mapPaths((p, hdfsPath) => pathsToRdd(Seq(p), 0).coalesce(filesToOutput, true).saveAsTextFile(hdfsPath))
    }


    def getTextFiles(paths: Seq[String], synchLocally: Boolean = false, forceSynch: Boolean = false, minimumPaths: Int = 1): RDD[String] = {
      if (synchLocally)
        processTextFiles(synchToHdfs(paths, processTextFiles, forceSynch), minimumPaths)
      else
        processTextFiles(paths, minimumPaths)
    }

    def filterAndGetTextFiles(path: String,
                              requireSuccess: Boolean = false,
                              inclusiveStartDate: Boolean = true,
                              startDate: Option[DateTime] = None,
                              inclusiveEndDate: Boolean = true,
                              endDate: Option[DateTime] = None,
                              lastN: Option[Int] = None,
                              synchLocally: Boolean = false,
                              forceSynch: Boolean = false,
                              ignoreMalformedDates: Boolean = false,
                              minimumPaths: Int = 1)(implicit dateExtractor: PathDateExtractor): RDD[String] = {
      val paths = getFilteredPaths(Seq(path), requireSuccess, inclusiveStartDate, startDate, inclusiveEndDate, endDate, lastN, ignoreMalformedDates)
      if (paths.size < minimumPaths)
        throw new Exception(s"Tried with start/end time equals to $startDate/$endDate for path $path but but the resulting number of paths $paths is less than the required")
      getTextFiles(paths, synchLocally, forceSynch, minimumPaths)
    }

    private def stringHadoopFile(paths: Seq[String], minimumPaths: Int): RDD[Try[String]] = {
      processPaths((p) => sc.sequenceFile(p, classOf[LongWritable], classOf[org.apache.hadoop.io.BytesWritable])
                .map({ case (k, v) => Try { ByteUtils.toString(v.getBytes, 0, v.getLength, "UTF-8") } }), paths, minimumPaths)
    }

    def filterAndGetStringHadoopFiles(path: String,
                                      requireSuccess: Boolean = false,
                                      inclusiveStartDate: Boolean = true,
                                      startDate: Option[DateTime] = None,
                                      inclusiveEndDate: Boolean = true,
                                      endDate: Option[DateTime] = None,
                                      lastN: Option[Int] = None,
                                      ignoreMalformedDates: Boolean = false,
                                      minimumPaths: Int = 1)(implicit dateExtractor: PathDateExtractor): RDD[Try[String]] = {
      val paths = getFilteredPaths(Seq(path), requireSuccess, inclusiveStartDate, startDate, inclusiveEndDate, endDate, lastN, ignoreMalformedDates)
      if (paths.size < minimumPaths)
        throw new Exception(s"Tried with start/end time equals to $startDate/$endDate for path $path but but the resulting number of paths $paths is less than the required")
      else
        stringHadoopFile(paths, minimumPaths)
    }

    private def objectHadoopFile[T:ClassTag](paths: Seq[String], minimumPaths: Int): RDD[T] = {
      processPaths(sc.objectFile[T](_), paths, minimumPaths)
    }

    def filterAndGetObjectHadoopFiles[T:ClassTag](path: String,
                                                  requireSuccess: Boolean = false,
                                                  inclusiveStartDate: Boolean = true,
                                                  startDate: Option[DateTime] = None,
                                                  inclusiveEndDate: Boolean = true,
                                                  endDate: Option[DateTime] = None,
                                                  lastN: Option[Int] = None,
                                                  ignoreMalformedDates: Boolean = false,
                                                  minimumPaths: Int = 1)(implicit dateExtractor: PathDateExtractor): RDD[T] = {
      val paths = getFilteredPaths(Seq(path), requireSuccess, inclusiveStartDate, startDate, inclusiveEndDate, endDate, lastN, ignoreMalformedDates)
      if (paths.size < minimumPaths)
        throw new Exception(s"Tried with start/end time equals to $startDate/$endDate for path $path but but the resulting number of paths $paths is less than the required")
      else
        objectHadoopFile(paths, minimumPaths)
    }

    def parallelTextFiles(paths: Seq[String], maxBytesPerPartition: Long): RDD[String] = {
      val s3Paths = parallelListFiles(paths)
      val buckets = buildBuckets(s3Paths, maxBytesPerPartition)
      val files = buckets.flatMap(_.paths)

      val s3Key = sc.hadoopConfiguration.get("fs.s3n.awsAccessKeyId")
      val s3Secret = sc.hadoopConfiguration.get("fs.s3n.awsSecretAccessKey")

      val partitionedFiles = sc.parallelize(files).map(file => file -> ()).partitionBy(createPartitioner(buckets))

      partitionedFiles.mapPartitions { files =>
        val conf = new Configuration()
        conf.set("fs.s3n.awsAccessKeyId", s3Key)
        conf.set("fs.s3n.awsSecretAccessKey", s3Secret)
        val codecFactory = new CompressionCodecFactory(conf)
        files.map { case (path, _) => path } flatMap { s3Path =>
          val fileSystem = FileSystem.get(new java.net.URI(s3Path), conf)
          val path = new Path(s3Path)
          val inputStream = Option(codecFactory.getCodec(path)) match {
            case Some(compression) => compression.createInputStream(fileSystem.open(path))
            case None => fileSystem.open(path)
          }
          Source.fromInputStream(inputStream).getLines()
        }
      }
    }

    private def createPartitioner(buckets: Seq[Bucket]): Partitioner = {
      val size = buckets.size
      val partitions: Map[Any, Int] = buckets.zipWithIndex.flatMap { case (bucket, index) => bucket.paths.map(path => path -> index) }.toMap
      new Partitioner {
        override def numPartitions: Int = size
        override def getPartition(key: Any): Int = partitions(key)
      }
    }

    private def buildBuckets(files: Seq[S3File], maxBytesPerPartition: Long): Seq[Bucket] = {
      val buckets = ArrayBuffer.empty[Bucket]
      files.distinctBy(_.path).foreach { file =>
        val size = file.size
        val bucket = buckets.find(bucket => bucket.size + size < maxBytesPerPartition) match {
          case Some(bucketFound) => bucketFound
          case None =>
            val newBucket = Bucket(0, ArrayBuffer.empty)
            buckets += newBucket
            newBucket
        }
        bucket.size += size
        bucket.paths += file.path
      }
      buckets
    }

    def parallelListFiles(paths: Seq[String]): Seq[S3File] = {
      val s3Key = sc.hadoopConfiguration.get("fs.s3n.awsAccessKeyId")
      val s3Secret = sc.hadoopConfiguration.get("fs.s3n.awsSecretAccessKey")

      val remainingDirectories = new scala.collection.mutable.ArrayBuffer[S3File]
      remainingDirectories ++= paths.map(S3File(_, isDir = true, 0))
      val allFiles = new scala.collection.mutable.ArrayBuffer[S3File]

      while (remainingDirectories.nonEmpty) {
        val newDirs = sc.parallelize(remainingDirectories.map(_.path))
        val currentBatch = newDirs.flatMap { path =>
          val conf = new Configuration()
          conf.set("fs.s3n.awsAccessKeyId", s3Key)
          conf.set("fs.s3n.awsSecretAccessKey", s3Secret)
          val fileSystem = FileSystem.get(new java.net.URI(path), conf)
          try {
            val hadoopPath = new Path(path)
            if (fileSystem.isDirectory(hadoopPath)) {
              val sanitize = Option(fileSystem.listStatus(hadoopPath)).getOrElse(Array.empty)
              sanitize.map(status => S3File(status.getPath.toString, status.isDirectory, status.getLen))
            } else if (fileSystem.isFile(hadoopPath)) {
              val status = fileSystem.getFileStatus(hadoopPath)
              Seq(S3File(status.getPath.toString, status.isDirectory, status.getLen))
            } else { // Maybe is glob or not found
              val sanitize = Option(fileSystem.globStatus(hadoopPath)).getOrElse(Array.empty)
              sanitize.map(status => S3File(status.getPath.toString, status.isDirectory, status.getLen))
            }
          } catch {
            case e: java.io.FileNotFoundException =>
              println(s"File $path not found.")
              e.printStackTrace()
              Nil
          }
        }.collect()
        val (dirs, files) = currentBatch.partition(_.isDir)
        remainingDirectories.clear()
        remainingDirectories ++= dirs
        allFiles ++= files
      }

      allFiles
    }

  }
}
