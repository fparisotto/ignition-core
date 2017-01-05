package ignition.core.http

import java.io.InputStream
import java.net.{URL, URLDecoder, URLEncoder}
import java.util.concurrent.TimeUnit

import akka.util.Timeout
import spray.http.Uri.Query
import spray.http._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

object AsyncHttpClientStreamApi {

  // Due to ancient standards, Java will encode space as + instead of using percent.
  //
  // See:
  // http://stackoverflow.com/questions/1634271/url-encoding-the-space-character-or-20
  // https://docs.oracle.com/javase/7/docs/api/java/net/URLEncoder.html#encode(java.lang.String,%20java.lang.String)
  private def sanitizePathSegment(segment: String) =
    URLEncoder.encode(URLDecoder.decode(segment, "UTF-8"), "UTF-8").replace("+", "%20")

  def sanitizeUrl(strUrl: String) = {
    val url = new URL(strUrl)
    val sanePath = url.getPath.split("/").map(sanitizePathSegment).mkString("/")

    Uri.from(
      scheme = url.getProtocol,
      userinfo = Option(url.getUserInfo).getOrElse(""),
      host = url.getHost,
      port = Seq(url.getPort, 0).max,
      path = sanePath,
      query = Query(Option(url.getQuery)),
      fragment = Option(url.getRef))
  }
  
  case class Credentials(user: String, password: String) {
    def isEmpty = user.isEmpty && password.isEmpty

    def toOption = Some(this).filter(!_.isEmpty)
  }

  object Credentials {
    val empty = Credentials("", "")
  }

  // TODO: return a stream is dangerous because implies into a lock
  case class StreamResponse(status: Int, content: InputStream)

  // If any value is None, it will fallback to the implementation's default
  object RequestConfiguration {
    val defaultMaxRedirects: Int = 15
    val defaultMaxConnectionsPerHost: Int = 500
    val defaultPipelining: Boolean = false
    val defaultIdleTimeout: FiniteDuration = Duration(30, TimeUnit.SECONDS)
    val defaultRequestTimeout: FiniteDuration = Duration(20, TimeUnit.SECONDS)
    val defaultConnectingTimeout: FiniteDuration = Duration(10, TimeUnit.SECONDS)
  }

  case class RequestConfiguration(maxRedirects: Option[Int] = Option(RequestConfiguration.defaultMaxRedirects),
                                  maxConnectionsPerHost: Option[Int] = Option(RequestConfiguration.defaultMaxConnectionsPerHost),
                                  pipelining: Option[Boolean] = Option(RequestConfiguration.defaultPipelining),
                                  idleTimeout: Option[Duration] = Option(RequestConfiguration.defaultIdleTimeout),
                                  requestTimeout: Option[Duration] = Option(RequestConfiguration.defaultRequestTimeout),
                                  connectingTimeout: Option[Duration] = Option(RequestConfiguration.defaultConnectingTimeout))

  case class Request(url: String,
                     params: Map[String, String] = Map.empty,
                     credentials: Option[Credentials] = None,
                     method: HttpMethod = HttpMethods.GET,
                     body: HttpEntity = HttpEntity.Empty,
                     headers: List[HttpHeader] = List.empty,
                     requestConfiguration: Option[RequestConfiguration] = None) {

    def uri: Uri = {
      // Note: This will guarantee we create a valid request (one with a valid uri). Will throw an exception if invalid
      if (params.nonEmpty)
        sanitizeUrl(url).withQuery(params)
      else
        sanitizeUrl(url)
    }
  }

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

  def makeRequest(request: AsyncHttpClientStreamApi.Request, retryConf: RetryConf = RetryConf(), retryOnHttpStatus: Seq[Int] = List.empty)
                   (implicit timeout: Timeout, reporter: AsyncHttpClientStreamApi.ReporterCallback = AsyncHttpClientStreamApi.NoOpReporter): Future[AsyncHttpClientStreamApi.StreamResponse]

}