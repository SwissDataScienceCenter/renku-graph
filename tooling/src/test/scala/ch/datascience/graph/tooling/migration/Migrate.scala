package ch.datascience.graph.tooling.migration

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder.decodeList
import io.circe.{Decoder, HCursor, Json}
import io.renku.control.Throttler
import io.renku.events.consumers.Project
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.GitLabUrl
import io.renku.graph.model.projects.{Id, Path}
import io.renku.http.client.RestClient
import io.renku.logging.ApplicationLogger
import io.renku.tinytypes.json.TinyTypeDecoders._
import io.renku.webhookservice.crypto.HookTokenCrypto
import io.renku.webhookservice.model.{HookToken, SelfUrl}
import org.http4s.Method.GET
import org.http4s.Status.{Ok, Successful}
import org.http4s._
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.circe.jsonEncoderOf
import org.typelevel.ci._
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

object Migrate extends IOApp {

  import scala.language.postfixOps

  class Migrate(gitLabUrl: GitLabUrl, selfUrl: SelfUrl)(implicit logger: Logger[IO])
      extends RestClient[IO, Migrate.type](Throttler.noThrottling, retryInterval = 500 millis, maxRetries = 1) {

    private def mapToCommits: PartialFunction[(Status, Request[IO], Response[IO]), IO[List[Json]]] = {
      case (Ok, _, response) => response.as[List[Json]]
      case (_, _, response)  => logger.info(response.body.toString()); IO.pure(List.empty[Json]);
    }

    private def getTotalPages(response: Response[IO]) =
      response.headers
        .get(ci"x-total-pages")
        .map(totalPages => IO.pure(totalPages.head.value))
        .getOrElse("1".pure[IO])

    private def getInitialCommit(project: Project): IO[String] = for {
      uri     <- validateUri(s"$gitLabUrl/api/v4/projects/${project.id}/repository/commits/")
      commits <- send(request(GET, uri, None))(mapToCommits)
      initalCommit = commits.head
    } yield initalCommit.hcursor.downField("id").as[String].fold(throw _, identity)

    private def payloadFor(initialCommit: String, project: Project): Json = Json.obj(
      "after" -> Json.fromString(initialCommit),
      "project" -> Json.obj(
        "id"                  -> Json.fromInt(project.id.value),
        "path_with_namespace" -> Json.fromString(project.path.toString)
      )
    )

    private implicit val jsonEntityEncoder: EntityEncoder[IO, Json] = jsonEncoderOf[IO, Json]

    private def sendEventFor(initialCommit: String, project: Project): IO[Unit] = for {
      hookTokenCrypto    <- HookTokenCrypto[IO]()
      encryptedHookToken <- hookTokenCrypto.encrypt(HookToken(project.id))
      payload            <- IO.pure(payloadFor(initialCommit, project))
      tokenHeader        <- IO.pure(Header.Raw(ci"X-Gitlab-Token", encryptedHookToken.value))
      webhookUri         <- validateUri(s"$selfUrl/webhooks/events")
      req = request(Method.POST, webhookUri) withHeaders tokenHeader withEntity payload
      _ <- send(req)(logResponse(project))
    } yield ()

    private def migrate(project: Project): IO[Unit] = (for {
      initialCommit <- getInitialCommit(project)
      _             <- sendEventFor(initialCommit, project)
    } yield ()) recoverWith logError

    private def logError: PartialFunction[Throwable, IO[Unit]] = { case exception =>
      logger.error(exception)(exception.getMessage)
    }

    private def logResponse(project: Project): PartialFunction[(Status, Request[IO], Response[IO]), IO[Unit]] = {
      case (_, _, Successful(response)) =>
        for {
          body <- response.as[Json]
          _    <- logger.info(s"For project ${project.toString} ${body.noSpaces}")
        } yield ()
      case (_, _, response) =>
        for {
          body <- response.as[Json]
          _    <- logger.error(s"For project ${project.toString} ${body.spaces2}")
        } yield ()
    }

    private def getProjectsOn(page: Int): IO[(List[Project], String)] = for {
      uri                   <- validateUri(s"$gitLabUrl/api/v4/projects")
      projectsAndTotalPages <- send(request(GET, uri.withQueryParam("page", page), None))(mapProjects)
    } yield projectsAndTotalPages

    private def mapProjects: PartialFunction[(Status, Request[IO], Response[IO]), IO[(List[Project], String)]] = {
      case (Ok, _, response) =>
        for {
          totalPages <- getTotalPages(response)
          projects   <- response.as[List[Project]]
        } yield (projects, totalPages)
      case (_, _, response) => logger.info(response.body.toString()); throw new Exception(response.body.toString());
    }
    private implicit val projectDecoder: Decoder[Project] = (cursor: HCursor) =>
      for {
        id   <- cursor.downField("id").as[Id]
        path <- cursor.downField("path_with_namespace").as[Path]
      } yield Project(id, path)

    def migrateAllProjects(): IO[Unit] = {
      def migrateProjectOn(page: Int): IO[Unit] = for {
        projectsAndTotalPage <- getProjectsOn(page)
        (projects, totalPages) = projectsAndTotalPage
        _ <- projects.map(project => migrate(project)).sequence
        res <- if (page < totalPages.toInt) {
                 migrateProjectOn(page + 1)
               } else {
                 IO.unit
               }
        _ <- logger.info(s"Done with page $page of $totalPages")
      } yield res

      migrateProjectOn(page = 1)
    }
  }

  override def run(args: List[String]): IO[ExitCode] = for {
    gitlabUrl <- GitLabUrlLoader[IO]()
    selfUrl   <- SelfUrl[IO]()
    logger    = ApplicationLogger
    migration = new Migrate(gitlabUrl, selfUrl)(logger)
    _ <- migration.migrateAllProjects()
  } yield ExitCode.Success
}
