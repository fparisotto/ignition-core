package ignition.core.utils

object ExceptionUtils {

  implicit class ExceptionImprovements(e: Throwable) {
    def getFullStackTraceString(): String = org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e)
  }

}
