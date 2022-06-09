package io.renku.knowledgegraph.docs

import io.renku.knowledgegraph.docs.model.Status
import org.http4s
import cats.syntax.all._

object Implicits {
  implicit class StatusOps(status: http4s.Status) {
    lazy val asDocStatus: Status = Status(status.code, status.reason)
  }

  implicit class MediaTypeOps(mediaType: http4s.MediaType) {
    lazy val asDocMediaType: String = mediaType.show
  }
}
