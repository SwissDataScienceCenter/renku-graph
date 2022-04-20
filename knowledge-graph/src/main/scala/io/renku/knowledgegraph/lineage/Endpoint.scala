package io.renku.knowledgegraph.lineage

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._
import io.circe._
import io.circe.generic.semiauto._
import io.circe.syntax.EncoderOps
import io.renku.graph.model.projects
import io.renku.http.InfoMessage
import io.renku.http.InfoMessage._
import io.renku.http.server.security.model.AuthUser
import io.renku.knowledgegraph.lineage.model.Lineage
import io.renku.knowledgegraph.lineage.model.Node.Location
import io.renku.rdfstore.SparqlQueryTimeRecorder
import org.http4s.Response
import org.http4s.circe.jsonEncoder
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

trait Endpoint[F[_]] {
  def `GET /lineage`(projectPath: projects.Path, location: Location, maybeUser: Option[AuthUser]): F[Response[F]]
}

private class EndpointImpl[F[_]: Async](lineageFinder: LineageFinder[F]) extends Http4sDsl[F] with Endpoint[F] {
  override def `GET /lineage`(projectPath: projects.Path,
                              location:    Location,
                              maybeUser:   Option[AuthUser]
  ): F[Response[F]] = lineageFinder.find(projectPath, location, maybeUser) flatMap toHttpResult

  private def toHttpResult: Option[Lineage] => F[Response[F]] = {
    case None          => NotFound(InfoMessage(s"No lineage for project: file: "))
    case Some(lineage) => Ok(lineage.asJson)
  }

  implicit val lineageEncoder: Encoder[Lineage] = deriveEncoder[Lineage]
}

object Endpoint {
  def apply[F[_]: Async: Parallel: Logger: SparqlQueryTimeRecorder]: F[Endpoint[F]] = for {
    lineageFinder <- LineageFinder[F]
  } yield new EndpointImpl[F](lineageFinder).pure[F].widen[Endpoint[F]]

}
