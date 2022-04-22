package io.renku.knowledgegraph.lineage

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._
import io.circe.Encoder
import io.circe.generic.semiauto._
import io.circe.syntax._
import io.renku.graph.model.projects
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.http.InfoMessage.infoMessageEntityEncoder
import io.renku.knowledgegraph.lineage.model.{Edge, Node}
import io.renku.knowledgegraph.lineage.model.Node.{Label, Type}
import org.http4s.circe.jsonEncoder
import io.renku.http.server.security.model.AuthUser
import io.renku.knowledgegraph.lineage.model.Lineage
import io.renku.knowledgegraph.lineage.model.Node.Location
import io.renku.rdfstore.SparqlQueryTimeRecorder
import org.http4s.Response
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait Endpoint[F[_]] {
  def `GET /lineage`(projectPath: projects.Path, location: Location, maybeUser: Option[AuthUser]): F[Response[F]]
}

private class EndpointImpl[F[_]: Async: Logger](lineageFinder: LineageFinder[F]) extends Http4sDsl[F] with Endpoint[F] {
  override def `GET /lineage`(projectPath: projects.Path,
                              location:    Location,
                              maybeUser:   Option[AuthUser]
  ): F[Response[F]] = lineageFinder.find(projectPath, location, maybeUser) flatMap toHttpResult(projectPath,
                                                                                                location
  ) recoverWith httpResult

  private def toHttpResult(projectPath: projects.Path, location: Location): Option[Lineage] => F[Response[F]] = {
    case None          => NotFound(InfoMessage(show"No lineage for project: $projectPath file: $location"))
    case Some(lineage) => Ok(lineage.asJson)
  }

  private lazy val httpResult: PartialFunction[Throwable, F[Response[F]]] = { case NonFatal(exception) =>
    val errorMessage = ErrorMessage("Lineage generation failed")
    Logger[F].error(exception)(errorMessage.value) >> InternalServerError(errorMessage)
  }

  implicit val lineageEncoder:  Encoder[Lineage]  = deriveEncoder
  implicit val edgeEncoder:     Encoder[Edge]     = deriveEncoder
  implicit val nodeEncoder:     Encoder[Node]     = deriveEncoder
  implicit val locationEncoder: Encoder[Location] = deriveEncoder
  implicit val typeEncoder:     Encoder[Type]     = deriveEncoder
  implicit val labelEncoder:    Encoder[Label]    = deriveEncoder

}

object Endpoint {
  def apply[F[_]: Async: Parallel: Logger: SparqlQueryTimeRecorder]: F[Endpoint[F]] = for {
    lineageFinder <- LineageFinder[F]
  } yield new EndpointImpl[F](lineageFinder)

}
