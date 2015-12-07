package ignition.core.utils

object ExceptionUtils {

  implicit class ExceptionImprovements(e: Throwable) {
    def getFullStacktraceString(): String = org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e)
  }

}
