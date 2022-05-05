package io.renku.knowledgegraph.lineage

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._
import io.circe.Encoder
import io.circe.generic.semiauto._
import io.circe.literal.JsonStringContext
import io.circe.syntax._
import io.renku.graph.model.Schemas.prov
import io.renku.graph.model.projects
import io.renku.http.InfoMessage.infoMessageEntityEncoder
import io.renku.http.server.security.model.AuthUser
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.knowledgegraph.lineage.model.Node.{Location, Type}
import io.renku.knowledgegraph.lineage.model.{Edge, Lineage, Node}
import io.renku.rdfstore.SparqlQueryTimeRecorder
import org.http4s.Response
import org.http4s.circe.jsonEncoder
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
  ): F[Response[F]] =
    lineageFinder.find(projectPath, location, maybeUser) flatMap toHttpResult(projectPath,
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

  implicit val lineageEncoder: Encoder[Lineage] = deriveEncoder
  implicit val edgeEncoder: Encoder[Edge] = Encoder.instance { edge =>
    json"""{
          "source": ${edge.source.value},
          "target": ${edge.target.value}
          }"""
  }
  implicit val nodeEncoder: Encoder[Node] = Encoder.instance { node =>
    json"""{
          "id": ${node.location.value},
          "location": ${node.location.value},
          "label": ${node.label.value},
          "type": ${node.singleWordType}
          }"""
  }

  private implicit class NodeOps(node: Node) {

    private lazy val FileTypes = Set(Type((prov / "Entity").show))

    lazy val singleWordType: String = node.types match {
      case types if types contains Type((prov / "Activity").show)   => "ProcessRun"
      case types if types contains Type((prov / "Collection").show) => "Directory"
      case FileTypes                                                => "File"
    }
  }
}

object Endpoint {
  def apply[F[_]: Async: Parallel: Logger: SparqlQueryTimeRecorder]: F[Endpoint[F]] = for {
    lineageFinder <- LineageFinder[F]
  } yield new EndpointImpl[F](lineageFinder)

}
