package ignition.core.http

import java.util.concurrent.TimeoutException

import akka.actor._
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import ignition.core.http.AsyncHttpClientStreamApi.{Request, RequestConfiguration}
import spray.can.Http
import spray.can.Http.HostConnectorSetup
import spray.can.client.{ClientConnectionSettings, HostConnectorSettings}
import spray.http.HttpHeaders.Authorization
import spray.http.StatusCodes.Redirection
import spray.http._

import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.control.NonFatal


trait AsyncSprayHttpClient extends AsyncHttpClientStreamApi {

  implicit def actorRefFactory: ActorRefFactory
  def executionContext: ExecutionContext = actorRefFactory.dispatcher

  override def makeRequest(request: AsyncHttpClientStreamApi.Request, retryConf: RetryConf, retryOnHttpStatus: Seq[Int])
                             (implicit timeout: Timeout, reporter: AsyncHttpClientStreamApi.ReporterCallback): Future[AsyncHttpClientStreamApi.StreamResponse] = {
    val processor = actorRefFactory.actorOf(Props(new RequestProcessorActor(timeout, reporter, retryConf, retryOnHttpStatus)))
    (processor ? request).mapTo[AsyncHttpClientStreamApi.StreamResponse]
  }

  private class RequestProcessorActor(timeout: Timeout,
                                      reporter: AsyncHttpClientStreamApi.ReporterCallback,
                                      retryConf: RetryConf,
                                      retryOnHttpStatus: Seq[Int])
    extends Actor with ActorLogging {


    import context.system

    import scala.language.implicitConversions

    def isRedirection(status: StatusCode): Boolean = status match {
      case r: Redirection => true
      case _ => false
    }

    private implicit def toAuthHeader(credentials: AsyncHttpClientStreamApi.Credentials): List[Authorization] =
      List(Authorization(credentials = BasicHttpCredentials(username = credentials.user, password = credentials.password)))

    private def toSprayRequest(request: Request): HttpRequest = request match {
      case Request(_, params, Some(credentials), method, body, headers, _) =>
          HttpRequest(method = method, uri = request.uri, headers = credentials ++ headers, entity = body)

      case Request(_, params, None, method, body, headers, _) =>
        HttpRequest(method = method, uri = request.uri, entity = body, headers = headers)
    }

    private def toSprayHostConnectorSetup(uri: Uri, conf: Option[AsyncHttpClientStreamApi.RequestConfiguration]): HostConnectorSetup = {
      // Create based on defaults, change some of them
      val ccs: ClientConnectionSettings = ClientConnectionSettings(system)
      val hcs: HostConnectorSettings = HostConnectorSettings(system)

      val updatedCcs = ccs.copy(
        responseChunkAggregationLimit = 0, // makes our client ineffective if non zero
        idleTimeout = conf.flatMap(_.idleTimeout).getOrElse(ccs.idleTimeout),
        connectingTimeout = conf.flatMap(_.connectingTimeout).getOrElse(ccs.connectingTimeout),
        requestTimeout = conf.flatMap(_.requestTimeout).getOrElse(ccs.requestTimeout)
      )

      val maxConnections = conf.flatMap(_.maxConnectionsPerHost).getOrElse {
        // Let's avoid someone shoot his own foot
        if (hcs.maxConnections == 4) // Spray's default is stupidly low
          // Use the API's default, which is more reasonable
          RequestConfiguration.defaultMaxConnectionsPerHost
        else
          // If the conf is the non-default value, then someone know what he's doing. use that configured value
          hcs.maxConnections
      }

      val updatedHcs = hcs.copy(
        connectionSettings = updatedCcs,
        maxRetries = 0, // We have our own retry mechanism
        maxRedirects = 0, // We do our own redirect following
        maxConnections = maxConnections,
        pipelining = conf.flatMap(_.pipelining).getOrElse(hcs.pipelining)
      )

      val host = uri.authority.host
      HostConnectorSetup(host.toString, uri.effectivePort, sslEncryption = uri.scheme == "https", settings = Option(updatedHcs))
    }

    private def executeSprayRequest(request: Request): Unit = {
      val message = (toSprayRequest(request), toSprayHostConnectorSetup(request.uri, request.requestConfiguration))
      IO(Http) ! message
    }

    def handleErrors(commander: ActorRef, request: Request, retry: Retry, storage: ByteStorage, remainingRedirects: Int): Receive = {
      case ev @ Http.SendFailed(_) =>
        log.debug("Communication error, cause: {}", ev)
        reporter.onError(request, ev)
        storage.close()
        context.become(retrying(commander, request, remainingRedirects))
        self ! retry.onError

      case ev @ Timedout(_) =>
        log.debug("Communication error, cause: {}", ev)
        reporter.onError(request, ev)
        storage.close()
        context.become(retrying(commander, request, remainingRedirects))
        self ! retry.onTimeout

      case Status.Failure(NonFatal(exception)) =>
        reporter.onError(request, exception)
        storage.close()
        exception match {
          case ex: Http.RequestTimeoutException =>
            log.warning("Request {} timeout, details: {}", request, ex.getMessage)
            context.become(retrying(commander, request, remainingRedirects))
            self ! retry.onTimeout

          case ex: Http.ConnectionException =>
            log.warning("Connection error on {}, details: {}", request, ex.getMessage)
            context.become(retrying(commander, request, remainingRedirects))
            self ! retry.onError

          case unknownException =>
            log.error(unknownException, "Unknown error on {}", request)
            context.become(retrying(commander, request, remainingRedirects))
            self ! retry.onError
        }

      case unknownMessage =>
        log.debug("Unknown message: {}", unknownMessage)
        reporter.onError(request, unknownMessage)
        storage.close()
        context.become(retrying(commander, request, remainingRedirects))
        self ! retry.onError
    }

    def receive: Receive = {
      case request: Request =>
        log.debug("Starting request {}", request)
        reporter.onRequest(request)
        executeSprayRequest(request)
        val retry = Retry(startTime = org.joda.time.DateTime.now, timeout = timeout.duration, conf = retryConf)
        val storage = new ByteStorage()
        val maxRedirects =
          request.requestConfiguration.flatMap(_.maxRedirects).getOrElse(RequestConfiguration.defaultMaxRedirects)
        context.become(waitingForResponse(sender, request, retry, storage, maxRedirects)
          .orElse(handleErrors(sender, request, retry, storage, maxRedirects)))
    }

    def retrying(commander: ActorRef, request: Request, remainingRedirects: Int): Receive = {
      case retry: Retry =>
        if (retry.shouldGiveUp) {
          reporter.onGiveUp(request)
          log.warning("Error to get {}, no more retries {}, accepting failure", request, retry)
          commander ! Status.Failure(new TimeoutException(s"Failed to get '${request.url}'"))
          context.stop(self)
        } else {
          reporter.onRetry(request)
          log.info("Retrying {}, retry status {}, backing off for {} millis", request, retry, retry.backoff.toMillis)
          system.scheduler.scheduleOnce(retry.backoff) {
            log.debug("Waking from backoff, retrying request {}", request)
            executeSprayRequest(request)
          }(executionContext)
          val storage = new ByteStorage()
          context.become(waitingForResponse(commander, request, retry, storage, remainingRedirects)
            .orElse(handleErrors(commander, request, retry, storage, remainingRedirects)))
        }
    }

    def waitingForResponse(commander: ActorRef, request: Request, retry: Retry, storage: ByteStorage, remainingRedirects: Int): Receive = {
      case response@HttpResponse(status, entity, headers, _) => try {
        storage.write(response.entity.data.toByteArray)
        if (isRedirection(status))
          handleRedirect(commander, storage, retry, request, status, response, remainingRedirects)
        else if (status.isSuccess) {
          reporter.onResponse(request, status.intValue)
          commander ! Status.Success(AsyncHttpClientStreamApi.StreamResponse(status.intValue, storage.getInputStream()))
          context.stop(self)
        } else if (retryOnHttpStatus.contains(status.intValue)) {
          storage.close()
          log.debug("HttpResponse: Status {}, retrying...", status)
          context.become(retrying(commander, request, remainingRedirects))
          self ! retry.onError
        } else {
          val message = s"HTTP response status ${status.intValue}, on request ${request}, ${status.defaultMessage}"
          log.debug("HttpResponse: {}", message)
          reporter.onFailure(request, status.intValue)
          reporter.onGiveUp(request)
          commander ! Status.Failure(new AsyncHttpClientStreamApi.RequestException(message = message,
            response = AsyncHttpClientStreamApi.StreamResponse(status.intValue, storage.getInputStream())))
          context.stop(self)
        }
      } catch {
        case NonFatal(ex) =>
          storage.close()
          log.error(ex, "HttpResponse: Failure on creating HttpResponse")
          reporter.onError(request, ex)
          context.become(retrying(commander, request, remainingRedirects))
          self ! retry.onError
      }

      case chunkStart@ChunkedResponseStart(HttpResponse(status, entity, headers, _)) => try {
        storage.write(entity.data.toByteArray)
        if (isRedirection(status))
          handleRedirect(commander, storage, retry, request, status, chunkStart, remainingRedirects)
        else if (status.isSuccess) {
          context.become(accumulateChunks(commander, request, retry, storage, status, remainingRedirects)
            .orElse(handleErrors(commander, request, retry, storage, remainingRedirects)))
        } else if (retryOnHttpStatus.contains(status.intValue)) {
          storage.close()
          log.debug("ChunkedResponseStart: Status {}, retrying...", status)
          context.become(retrying(commander, request, remainingRedirects))
          self ! retry.onError
        } else {
          val message = s"HTTP response status ${status.intValue}, on request ${request}, ${status.defaultMessage}"
          log.debug("ChunkedResponseStart: {}", message)
          reporter.onFailure(request, status.intValue)
          reporter.onGiveUp(request)
          commander ! Status.Failure(new AsyncHttpClientStreamApi.RequestException(message = message,
            response = AsyncHttpClientStreamApi.StreamResponse(status.intValue, storage.getInputStream())))
          context.stop(self)
        }
      } catch {
        case NonFatal(ex) =>
          log.error(ex, "ChunkedResponseStart: Failure on creating ChunkedHttpResponse")
          reporter.onError(request, ex)
          context.become(retrying(commander, request, remainingRedirects))
          self ! retry.onError
      }
    }

    def accumulateChunks(commander: ActorRef, request: Request, retry: Retry, storage: ByteStorage, status: StatusCode, remainingRedirects: Int): Receive = {
      case message@MessageChunk(data, _) => try {
        storage.write(data.toByteArray)
      } catch {
        case NonFatal(ex) =>
          storage.close()
          log.error(ex, "MessageChunk: Failure on accumulate chunk data")
          reporter.onError(request, ex)
          context.become(retrying(commander, request, remainingRedirects))
          self ! retry.onError
      }

      case chunkEnd: ChunkedMessageEnd =>
        log.debug("ChunkedMessageEnd: all data was received for request {}, status {}", request, status)
        reporter.onResponse(request, status.intValue)
        commander ! Status.Success(AsyncHttpClientStreamApi.StreamResponse(status.intValue, storage.getInputStream()))
        context.stop(self)
    }

    def handleRedirect(commander: ActorRef, oldStorage: ByteStorage, oldRetry: Retry, oldRequest: Request, status: StatusCode, rawResponse: HttpResponsePart, remainingRedirects: Int): Unit = {
      if (remainingRedirects <= 0) {
        val message = s"HandleRedirect: exceeded redirection limit on $oldRequest with status $status"
        log.warning(message)
        reporter.onGiveUp(oldRequest)
        commander ! Status.Failure(new Exception(message))
        context.stop(self)
      } else {
        def makeRequest(headers: List[HttpHeader]): Receive = {
          oldStorage.close()
          val newRemainingRedirects = remainingRedirects - 1
          headers.find(_.is("location")).map(_.value).map { newLocation =>
            log.debug("Making redirect to {}", newLocation)
            val newRequest = oldRequest.copy(url = newLocation)
            executeSprayRequest(newRequest)
            val newRetry = Retry(startTime = org.joda.time.DateTime.now, timeout = timeout.duration, conf = retryConf)
            val newStorage = new ByteStorage()
            waitingForResponse(commander, newRequest, newRetry, newStorage, newRemainingRedirects)
              .orElse(handleErrors(commander, newRequest, newRetry, newStorage, newRemainingRedirects))
          }.getOrElse {
            log.warning("Received redirect for request {} with headers {} without location, retrying...", oldRequest, headers)
            retrying(commander, oldRequest, newRemainingRedirects)
          }
        }
        context.become(rawResponse match {
          case response@HttpResponse(status, entity, headers, _) =>
            makeRequest(headers)
          case chunkStart@ChunkedResponseStart(HttpResponse(status, entity, headers, _)) => {
            case message@MessageChunk(data, _) =>
              // do nothing
            case chunkEnd: ChunkedMessageEnd =>
              context.become(makeRequest(headers))
          }
          case other =>
            throw new Exception(s"Bug, called on $other")
        })
      }
    }

  }

}
