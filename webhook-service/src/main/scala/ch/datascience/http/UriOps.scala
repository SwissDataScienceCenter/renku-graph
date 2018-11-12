package ch.datascience.http

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.Uri.Query

object UriOps {

  implicit class UriOps(uri: Uri) {

    def /(segment: Any): Uri = uri.copy(path = uri.path / segment.toString)

    def &(param: (String, Any)): Uri = {
      val (key, value) = param
      uri.withQuery(
        ((key -> value.toString) +: Query(uri.rawQueryString)).reverse
      )
    }
  }
}
