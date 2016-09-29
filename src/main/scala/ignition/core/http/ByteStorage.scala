package ignition.core.http

import java.io._
import java.nio.file.{Files, Paths}
import java.util.UUID

import org.slf4j.LoggerFactory

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class ByteStorage(memoryThreshold: Int = 1024 * 1024 * 5) extends AutoCloseable {

  lazy val log = LoggerFactory.getLogger(getClass)

  lazy val tempDirPath = Files.createDirectories(Paths.get(System.getProperty("java.io.tmpdir"), "ByteStorage"))

  lazy val buffer = new ByteArrayOutputStream

  var fileStorage: Option[(File, FileOutputStream)] = None

  def write(bytes: Array[Byte]): Unit = try {
    if (fileStorage.isDefined) {
      writeOnFile(bytes)
    } else if (buffer.size() + bytes.length > memoryThreshold) {
      log.debug("Memory threshold {} reach, going to file storage", memoryThreshold)
      setupFileStorage()
      writeOnFile(buffer.toByteArray)
      writeOnFile(bytes)
      // on ByteArrayOutputStream close() takes not effect,
      // but if we change the buffer impl this is the a good moment to free resources
      buffer.close()
    } else {
      buffer.write(bytes)
    }
  } catch {
    case NonFatal(ex) =>
      close()
      throw ex
  }

  override def close(): Unit = fileStorage match {
    case Some((file, outputStream)) => try {
        log.debug("Cleaning up temp file {}", file.getAbsolutePath)
        outputStream.close()
        file.delete()
      } catch {
        case NonFatal(ex) => log.warn(s"Fail to cleanup temp file ${file.getAbsolutePath}", ex)
      }
    case None =>
      log.debug("Cleaning up memory buffer")
      buffer.close()
  }

  private def setupFileStorage(): Unit = if (fileStorage.isEmpty) {
    tryCreateTempFile match {
      case Success(storage) => fileStorage = Option(storage)
      case Failure(ex) => throw ex
    }
  } else {
    throw new IllegalStateException("File storage already setup")
  }

  private def tryCreateTempFile: Try[(File, FileOutputStream)] = Try {
    val tempFile = File.createTempFile(s"temp_byte_storage_${UUID.randomUUID().toString}", ".temp", tempDirPath.toFile)
    tempFile.deleteOnExit()
    log.debug("Creating temp file {}", tempFile.getAbsolutePath)
    (tempFile, new FileOutputStream(tempFile))
  }

  private def writeOnFile(bytes: Array[Byte]): Unit = fileStorage match {
    case Some((_, outputStream)) => outputStream.write(bytes)
    case None => throw new IllegalStateException("File storage not initialized")
  }

  def getInputStream(): InputStream = fileStorage match {
    case Some((file, outputStream)) => try {
      outputStream.close()
      new DeleteOnCloseFileInputStream(file)
    } catch {
      case NonFatal(ex) =>
        log.error("Fail to create InputStream", ex)
        close()
        throw ex
    }
    case None => new ByteArrayInputStream(buffer.toByteArray)
  }

  override def finalize() = try {
    fileStorage match {
      case Some((file, outputStream)) =>
        log.debug("Cleaning up temp file {}", file.getAbsolutePath)
        outputStream.close()
        file.delete()
      case None =>
    }
  } finally {
    super.finalize()
  }

}

private class DeleteOnCloseFileInputStream(file: File) extends FileInputStream(file) {
  lazy val log = LoggerFactory.getLogger(getClass)
  override def close() = try {
    log.debug("Cleaning up file {}", file.getAbsolutePath)
    file.delete()
  } catch {
    case NonFatal(ex) =>
      log.warn(s"Failed to clean up file ${file.getAbsolutePath}", ex)
  } finally {
    super.close()
  }
}