package ch.datascience.webhookservice.migration

import java.util.concurrent.Executors.newFixedThreadPool

import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.control.Throttler
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.http.client._
import ch.datascience.logging.ApplicationLogger
import ch.datascience.webhookservice.crypto.HookTokenCrypto
import ch.datascience.webhookservice.eventprocessing.Project
import ch.datascience.webhookservice.model.HookToken
import ch.datascience.webhookservice.project.SelfUrl
import eu.timepit.refined.auto._
import io.circe.Decoder.decodeList
import io.circe.{Decoder, HCursor, Json}
import org.http4s.Method.GET
import org.http4s.Status.{Ok, Successful}
import org.http4s._
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.circe.jsonEncoderOf
import org.http4s.util.CaseInsensitiveString

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object Migrate extends App {
  private implicit val executionContext: ExecutionContext =
    ExecutionContext fromExecutorService newFixedThreadPool(1)

  protected implicit def contextShift: ContextShift[IO] =
    IO.contextShift(executionContext)

  protected implicit def timer: Timer[IO] =
    IO.timer(executionContext)

  import scala.language.postfixOps

  class Migrate(gitLabUrl: GitLabUrl, selfUrl: SelfUrl)
      extends IORestClient(Throttler.noThrottling, ApplicationLogger, retryInterval = 500 millis, maxRetries = 1) {
    private val logger = ApplicationLogger

    private def mapToCommits: PartialFunction[(Status, Request[IO], Response[IO]), IO[List[Json]]] = {
      case (Ok, _, response) => response.as[List[Json]]
      case (_, _, response)  => logger.info(response.body.toString()); IO.pure(List.empty[Json]);
    }

    private def mapLastPage: PartialFunction[(Status, Request[IO], Response[IO]), IO[String]] = {
      case (Ok, _, response) => getTotalPages(response)
      case (_, _, response)  => logger.info(response.body.toString()); throw new Exception(response.body.toString());
    }

    private def getTotalPages(response: Response[IO]) =
      response.headers
        .get(CaseInsensitiveString("x-total-pages"))
        .map(totalPages => IO.pure(totalPages.toString().split(":").last.trim))
        .getOrElse(throw new Exception("Missing total pages header"))

    private def getInitialCommit(project: Project): IO[String] = for {
      uri      <- validateUri(s"$gitLabUrl/api/v4/projects/${project.id}/repository/commits/")
      lastPage <- send(request(GET, uri, None))(mapLastPage)
      commits  <- send(request(GET, uri.withQueryParam("page", lastPage), None))(mapToCommits)
      initalCommit = commits.reverse.head
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
      tokenHeader        <- IO.pure(Header("X-Gitlab-Token", encryptedHookToken.value))
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
          json <- response.as[Json]
          _    <- logger.info(s"For project ${project.toString} ${json.toString()}")
        } yield ()
      case (_, _, response) =>
        for {
          json <- response.as[Json]
          _    <- logger.error(s"For project ${project.toString} ${json.toString()}")
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
    private implicit val projectDecoder: Decoder[Project] = (cursor: HCursor) => {
      import ch.datascience.tinytypes.json.TinyTypeDecoders._
      for {
        id   <- cursor.downField("id").as[Id]
        path <- cursor.downField("path_with_namespace").as[Path]
      } yield Project(id, path)
    }

    def migrateAllProjects(): IO[Unit] = {
      def migrateProjectOn(page: Int): IO[Unit] = for {
        projectsAndTotalPage <- getProjectsOn(page)
        (projects, totalPages) = projectsAndTotalPage
        _ <- projects.map(project => migrate(project)).sequence
        res <- if (page < totalPages.toInt) {
                 migrateProjectOn(page + 1)
               } else {
                 IO(())
               }
        _ <- logger.info(s"Done with page $page of $totalPages")
      } yield res

      for {
        _ <- migrateProjectOn(page = 1)
      } yield ()
    }
  }

  val program = for {
    gitlabUrl <- GitLabUrl[IO]()
    selfUrl   <- SelfUrl[IO]()
    migration = new Migrate(gitlabUrl, selfUrl)
    _ <- migration.migrateAllProjects()
  } yield ()

  program.unsafeRunSync()
}
