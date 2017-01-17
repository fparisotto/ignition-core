package ignition.core.utils

import java.net.{URL, URLDecoder, URLEncoder}

import org.apache.http.client.utils.URIBuilder
import spray.http.Uri
import spray.http.Uri.Query

object URLUtils {

  // Due to ancient standards, Java will encode space as + instead of using percent.
  //
  // See:
  // http://stackoverflow.com/questions/1634271/url-encoding-the-space-character-or-20
  // https://docs.oracle.com/javase/7/docs/api/java/net/URLEncoder.html#encode(java.lang.String,%20java.lang.String)
  def sanitizePathSegment(segment: String) =
    URLEncoder.encode(URLDecoder.decode(segment, "UTF-8"), "UTF-8").replace("+", "%20")

  def parseUri(urlStr: String): Uri = {
    val url = new URL(urlStr)
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

  def addParametersToUrl(url: String, partnerParams: Map[String, String]): String = {
    val builder = new URIBuilder(url.trim)
    partnerParams.foreach { case (k, v) => builder.addParameter(k, v) }
    builder.build().toString
  }
}
