package io.renku.knowledgegraph.lineage

import cats.MonadThrow
import cats.syntax.all._
import io.renku.graph.model.projects
import io.renku.graph.model.projects.Path
import io.renku.http.server.security.model.AuthUser
import io.renku.knowledgegraph.lineage.model.Node.Location
import org.http4s.Response

trait Endpoint[F[_]] {
  def `GET /lineage`(projectPath: projects.Path, location: Location, maybeUser: Option[AuthUser]): F[Response[F]]
}

private class EndpointImpl[F[_]: MonadThrow]() extends Endpoint[F] {
  override def `GET /lineage`(projectPath: projects.Path,
                              location:    Location,
                              maybeUser:   Option[AuthUser]
  ): F[Response[F]] = ???
}

object Endpoint {
  def apply[F[_]: MonadThrow](): F[Endpoint[F]] = new EndpointImpl[F]().pure[F].widen[Endpoint[F]]
}
