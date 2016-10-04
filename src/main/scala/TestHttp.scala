
object TestHttp extends App{

  def goTest(): Unit = {
    import java.util.concurrent.TimeUnit

    import akka.actor.{ActorRefFactory, ActorSystem}
    import akka.util.Timeout
    import ignition.core.http.AsyncHttpClientStreamApi._
    import ignition.core.http.AsyncSprayHttpClient
    import ignition.core.utils.ExceptionUtils._
    import org.joda.time.DateTime

    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent.duration.Duration
    import scala.io.Source
    import scala.util.{Failure, Success}
    def now = DateTime.now()

    val system = ActorSystem("http")
    val client = new AsyncSprayHttpClient {
      override implicit def actorRefFactory: ActorRefFactory = system
    }
    val url = "https://httpbin.org/delay/10" // "http://127.0.0.1:8081/"
    val conf = RequestConfiguration(requestTimeout = Option(Duration(12, TimeUnit.SECONDS)), idleTimeout = Option(Duration(5, TimeUnit.SECONDS)))
    implicit val reporter = NoOpReporter
    implicit val timeout = Timeout(30, TimeUnit.SECONDS)

    println(s"Starting $now")

    // Should complete ok
    val request1 = client.makeRequest(Request(url, requestConfiguration = Option(conf)))
    request1.onComplete {
      case Success(t) => println(s"request1 finished $now with Success: ${Source.fromInputStream(t.content).mkString}")
      case Failure(t) => println(s"request1 finished $now with failure: ${t.getFullStackTraceString()}")
    }

    //Should time out and keep retrying
    val tightConf = conf.copy(requestTimeout = Option(Duration(3, TimeUnit.SECONDS)))
    val request2 = client.makeRequest(Request(url, requestConfiguration = Option(tightConf)))

    request2.onComplete {
      case Success(t) => println(s"request2 finished $now with Success: ${Source.fromInputStream(t.content).mkString}")
      case Failure(t) => println(s"request2 finished $now with failure: ${t.getFullStackTraceString()}")
    }
  }

  goTest()
}
