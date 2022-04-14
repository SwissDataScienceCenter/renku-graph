package io.renku.knowledgegraph.lineage

import cats.MonadThrow
import cats.syntax.all._
import io.renku.graph.model.projects
import org.http4s.Response
import org.http4s.Uri.Path

trait Endpoint[F[_]] {
  def `GET /lineage`(projectId: projects.Id, resourcePath: Path): F[Response[F]]
}

private class EndpointImpl[F[_]: MonadThrow]() extends Endpoint[F] {
  override def `GET /lineage`(projectId: projects.Id, resourcePath: Path): F[Response[F]] = ???
}

object Endpoint {
  def apply[F[_]: MonadThrow](): F[Endpoint[F]] = new EndpointImpl[F]().pure[F].widen[Endpoint[F]]
}
