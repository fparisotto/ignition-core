package ignition.core.utils

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{S3ObjectSummary, ObjectListing}
import ignition.core.jobs.utils.PathDateExtractor
import ignition.core.utils.DateUtils._
import org.joda.time.DateTime

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Try

object S3Utils {

  implicit lazy val amazonS3ClientFromEnvironmentVariables = new AmazonS3Client(new EnvironmentVariableCredentialsProvider())

  def s3List(bucket: String, prefix: String, predicate: S3ObjectSummary => Boolean = _ => true)
            (implicit s3: AmazonS3Client): List[S3ObjectSummary] = {
    def inner(acc: mutable.ArrayBuffer[S3ObjectSummary], listing: ObjectListing): List[S3ObjectSummary] = {
      acc ++= listing.getObjectSummaries.toList.filter(predicate)
      if (listing.isTruncated)
        inner(acc, s3.listNextBatchOfObjects(listing))
      else
        acc.toList
    }

    inner(new mutable.ArrayBuffer[S3ObjectSummary], s3.listObjects(bucket, prefix))
  }

  def s3ListAndFilterFiles(bucket: String,
                           prefix: String,
                           start: Option[DateTime] = None,
                           end: Option[DateTime] = None,
                           endsWith: Option[String] = None,
                           exclusionPattern: Option[String] = Option("_$folder$"),
                           predicate: S3ObjectSummary => Boolean = _ => true)
                          (implicit s3: AmazonS3Client, pathDateExtractor: PathDateExtractor): List[S3ObjectSummary] = {

    def excludePatternValidation(s3Object: S3ObjectSummary, exclusionPatternOption: Option[String]): Option[S3ObjectSummary] =
      exclusionPatternOption match {
        case Some(pattern) if s3Object.getKey.contains(pattern) => None
        case Some(_) | None => Option(s3Object)
      }

    def endsWithValidation(s3Object: S3ObjectSummary, endsWithOption: Option[String]): Option[S3ObjectSummary] =
      endsWithOption match {
        case Some(pattern) if s3Object.getKey.endsWith(pattern) => Option(s3Object)
        case Some(_) => None
        case None => Option(s3Object)
      }

    def extractDateFromKey(s3Object: S3ObjectSummary): Option[DateTime] =
      Try(pathDateExtractor.extractFromPath(s"s3://$bucket/${s3Object.getKey}")).toOption

    def startValidation(s3Object: S3ObjectSummary, extractedDate: DateTime, startOption: Option[DateTime]): Option[S3ObjectSummary] =
      startOption match {
        case Some(startDate) if startDate.isEqualOrBefore(extractedDate) => Option(s3Object)
        case Some(_) => None
        case None => Option(s3Object)
      }

    def endValidation(s3Object: S3ObjectSummary, extractedDate: DateTime, endOption: Option[DateTime]): Option[S3ObjectSummary] =
      endOption match {
        case Some(endDate) if endDate.isEqualOrAfter(extractedDate) => Option(s3Object)
        case Some(_) => None
        case None => Option(s3Object)
      }

    def applyPredicate(s3Object: S3ObjectSummary): Option[S3ObjectSummary] =
      if (predicate(s3Object))
        Option(s3Object)
      else
        None

    val allValidations: S3ObjectSummary => Boolean = s3Object => {
      val validatedS3Object = for {
        withValidPattern <- excludePatternValidation(s3Object, exclusionPattern)
        withValidEndsWith <- endsWithValidation(withValidPattern, endsWith)
        extractedDate <- extractDateFromKey(withValidEndsWith)
        withValidStart <- startValidation(withValidEndsWith, extractedDate, start)
        withValidEnd <- endValidation(withValidStart, extractedDate, end)
        valid <- applyPredicate(withValidEnd)
      } yield valid
      validatedS3Object.isDefined
    }

    s3List(bucket, prefix, allValidations)(s3)
  }

}
