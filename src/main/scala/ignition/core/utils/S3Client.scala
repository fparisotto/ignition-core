package ignition.core.utils

import java.util.Properties

import org.jets3t.service.impl.rest.httpclient.RestS3Service
import org.jets3t.service.model.{S3Object, StorageObject}
import org.jets3t.service.security.AWSCredentials
import org.jets3t.service.{Constants, Jets3tProperties}


class S3Client {

  val jets3tProperties = {
    val jets3tProperties = Jets3tProperties.getInstance(Constants.JETS3T_PROPERTIES_FILENAME)
    val properties = new Properties()
//    properties.put("httpclient.max-connections", "2") // The maximum number of simultaneous connections to allow globally
//    properties.put("httpclient.retry-max", "10") // How many times to retry connections when they fail with IO errors
//    properties.put("httpclient.socket-timeout-ms", "30000") // How many milliseconds to wait before a connection times out. 0 means infinity.

    jets3tProperties.loadAndReplaceProperties(properties, "ignition'")
    jets3tProperties
  }

  val service = new RestS3Service(
    new AWSCredentials(System.getenv("AWS_ACCESS_KEY_ID"), System.getenv("AWS_SECRET_ACCESS_KEY")),
    null, null, jets3tProperties
  )

  def writeContent(bucket: String, key: String, content: String, contentType: String = "text/plain"): S3Object = {
    val obj = new S3Object(key, content)
    obj.setContentType(contentType)
    service.putObject(bucket, obj)
  }

  def readContent(bucket: String, key: String): S3Object = {
    service.getObject(bucket, key, null, null, null, null, null, null)
  }

  def list(bucket: String, key: String): Array[StorageObject] = {
    service.listObjectsChunked(bucket, key, null, 99999L, null, true).getObjects
  }

  def copyFile(sourceBucket: String, sourceKey: String,
               destBucket: String, destKey: String,
               destContentType: Option[String] = None,
               destContentEncoding: Option[String] = None): Unit = {
    val destFile = new S3Object(destKey)
    val replaceMetaData = destContentType.isDefined || destContentEncoding.isDefined
    destContentEncoding.foreach(encoding => destFile.setContentEncoding(encoding))
    destContentType.foreach(contentType => destFile.setContentType(contentType))
    service.copyObject(sourceBucket, sourceKey, destBucket, destFile, replaceMetaData)
  }

  def fileExists(bucket: String, key: String): Boolean = {
    try {
      service.getObjectDetails(bucket, key, null, null, null, null)
      true
    } catch {
      case e: org.jets3t.service.S3ServiceException if e.getResponseCode == 404 => false
    }
  }
}
