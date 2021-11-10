package io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.circe.Decoder
import io.renku.control.Throttler
import io.renku.graph.config.EventLogUrl
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.projects
import io.renku.http.client.RestClient
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.http.rest.paging.model.Page
import org.http4s.Method.GET
import org.http4s.Status.{NotFound, Ok, Unauthorized}
import org.http4s.circe.jsonOf
import org.http4s.{EntityDecoder, Request, Response, Status}
import org.typelevel.ci._
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.{Duration, FiniteDuration}

private trait ELCommitFetcher[F[_]] {
  def fetchELCommits(projectPath: projects.Path, page: Page): F[PageResult]
}

private object ELCommitFetcher {
  def apply[F[_]: Async: Logger]: F[ELCommitFetcher[F]] = for {
    eventLogUrl <- EventLogUrl[F]()
  } yield new ELCommitFetcherImpl(eventLogUrl)
}

private class ELCommitFetcherImpl[F[_]: Async: Logger](
    eventLogUrl:            EventLogUrl,
    retryInterval:          FiniteDuration = RestClient.SleepAfterConnectionIssue,
    maxRetries:             Int Refined NonNegative = RestClient.MaxRetriesAfterConnectionTimeout,
    requestTimeoutOverride: Option[Duration] = None
) extends RestClient(Throttler.noThrottling,
                     retryInterval = retryInterval,
                     maxRetries = maxRetries,
                     requestTimeoutOverride = requestTimeoutOverride
    )
    with ELCommitFetcher[F] {

  override def fetchELCommits(projectPath: projects.Path, page: Page): F[PageResult] = for {
    uri        <- createUrl(projectPath, page)
    pageResult <- send(request(GET, uri))(mapResponse)
  } yield pageResult

  private def createUrl(projectPath: projects.Path, page: Page) =
    validateUri(s"$eventLogUrl/events").map(
      _.withQueryParam("project-path", projectPath.show)
        .withQueryParam("page", page.show)
    )

  private implicit lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[PageResult]] = {
    case (Ok, _, response)    => (response.as[List[CommitId]] -> maybeNextPage(response)).mapN(PageResult(_, _))
    case (NotFound, _, _)     => PageResult.empty.pure[F]
    case (Unauthorized, _, _) => UnauthorizedException.raiseError
  }

  private implicit val commitIdsEntityDecoder: EntityDecoder[F, List[CommitId]] = jsonOf[F, List[CommitId]]

  private implicit lazy val commitIdsDecoder: Decoder[List[CommitId]] = Decoder.instance { cursor =>
    import io.circe.Decoder.decodeList
    import io.renku.tinytypes.json.TinyTypeDecoders._

    val commitIdDecoder: Decoder[CommitId] = Decoder.instance(_.downField("id").as[CommitId])
    cursor.as(decodeList(commitIdDecoder))
  }

  private def maybeNextPage(response: Response[F]): F[Option[Page]] =
    response.headers
      .get(ci"Next-Page")
      .flatMap(_.head.value.toIntOption)
      .map(Page.from)
      .map(MonadThrow[F].fromEither(_))
      .sequence

}
