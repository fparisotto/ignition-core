package ignition.core.http

import java.io.InputStream
import java.util.concurrent.TimeUnit

import akka.util.Timeout
import spray.http.{HttpEntity, HttpMethod, HttpMethods}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps


object AsyncHttpClientStreamApi {
  
  case class Credentials(user: String, password: String) {
    def isEmpty = user.isEmpty && password.isEmpty

    def toOption = Some(this).filter(!_.isEmpty)
  }

  object Credentials {
    val empty = Credentials("", "")
  }

  // TODO: return a stream is dangerous because implies into a lock
  case class StreamResponse(status: Int, content: InputStream)

  case class RequestConfiguration(maxRedirects: Int = 15,
                                  maxConnectionsPerHost: Int = 500,
                                  pipelining: Boolean = false,
                                  idleTimeout: Duration = Duration(30, TimeUnit.SECONDS),
                                  requestTimeout: Duration = Duration(20, TimeUnit.SECONDS),
                                  connectingTimeout: Duration = Duration(10, TimeUnit.SECONDS))

  case class Request(url: String,
                     params: Map[String, String] = Map.empty,
                     credentials: Option[Credentials] = None,
                     method: HttpMethod = HttpMethods.GET,
                     body: HttpEntity = HttpEntity.Empty,
                     requestConfiguration: Option[RequestConfiguration] = Option(RequestConfiguration()))

  case class RequestException(message: String, response: StreamResponse) extends RuntimeException(message)

  object NoOpReporter extends ReporterCallback {
    def onRequest(request: Request): Unit = {}
    def onResponse(request: Request, status: Int): Unit = {}
    def onFailure(request: Request, status: Int): Unit = {}
    def onRetry(request: Request): Unit = {}
    def onGiveUp(request: Request): Unit = {}
    def onError(request: Request, error: Any): Unit = {}
  }

  abstract class ReporterCallback {
    def onRequest(request: Request): Unit
    def onResponse(request: Request, status: Int): Unit
    def onFailure(request: Request, status: Int): Unit
    def onRetry(request: Request): Unit
    def onGiveUp(request: Request): Unit
    def onError(request: Request, error: Any): Unit
  }
}

trait AsyncHttpClientStreamApi {

  def makeRequest(request: AsyncHttpClientStreamApi.Request, initialBackoff: FiniteDuration = 100 milliseconds, retryOnHttpStatus: Seq[Int] = List.empty)
                             (implicit timeout: Timeout, reporter: AsyncHttpClientStreamApi.ReporterCallback = AsyncHttpClientStreamApi.NoOpReporter): Future[AsyncHttpClientStreamApi.StreamResponse]

}