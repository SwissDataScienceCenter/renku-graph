package io.renku.eventlog.status

import cats.MonadThrow
import org.http4s.Response

trait StatusEndpoint[F[_]] {
  def `GET /status`: F[Response[F]]
}

object StatusEndpoint {
  def apply[F[_]: MonadThrow]: F[StatusEndpoint[F]] = ???
}
