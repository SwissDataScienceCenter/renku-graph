package io.renku.triplesgenerator.events.categories.triplesgenerated.projectinfo

import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.circe.Decoder
import io.circe.Decoder.decodeList
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.{GitLabApiUrl, persons, projects}
import io.renku.http.client.{AccessToken, RestClient}
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.RecoverableErrorsRecovery
import org.http4s.Method.GET
import org.http4s.dsl.io.{NotFound, Ok}
import org.http4s.{EntityDecoder, Request, Response, Status}
import org.typelevel.ci._
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.{Duration, FiniteDuration}

private trait ProjectEventsFinder[F[_]] {
  def find(project:     Project, page: Int)(implicit
      maybeAccessToken: Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, (List[PushEvent], PagingInfo)]
}

private class ProjectEventsFinderImpl[F[_]: Async: Logger](
    gitLabApiUrl:           GitLabApiUrl,
    gitLabThrottler:        Throttler[F, GitLab],
    recoveryStrategy:       RecoverableErrorsRecovery = RecoverableErrorsRecovery,
    retryInterval:          FiniteDuration = RestClient.SleepAfterConnectionIssue,
    maxRetries:             Int Refined NonNegative = RestClient.MaxRetriesAfterConnectionTimeout,
    requestTimeoutOverride: Option[Duration] = None
) extends RestClient(gitLabThrottler,
                     retryInterval = retryInterval,
                     maxRetries = maxRetries,
                     requestTimeoutOverride = requestTimeoutOverride
    )
    with ProjectEventsFinder[F] {
  override def find(project: Project, page: Int)(implicit
      maybeAccessToken:      Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, (List[PushEvent], PagingInfo)] =
    EitherT {
      {
        for {
          uri             <- validateUri(s"$gitLabApiUrl/projects/${project.id}/events?action=pushed&page=$page")
          eventsAndPaging <- send(secureRequest(GET, uri))(mapResponse)
        } yield eventsAndPaging
      }.map(_.asRight[ProcessingRecoverableError]).recoverWith(recoveryStrategy.maybeRecoverableError)
    }

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[(List[PushEvent], PagingInfo)]] = {
    case (Ok, _, response) =>
      lazy val maybeNextPage   = response.headers.get(ci"X-Next-Page") >>= (_.head.value.toIntOption)
      lazy val maybeTotalPages = response.headers.get(ci"X-Total-Pages") >>= (_.head.value.toIntOption)
      response.as[List[PushEvent]].map(_ -> PagingInfo(maybeNextPage, maybeTotalPages))
    case (NotFound, _, _) => (List.empty[PushEvent] -> PagingInfo(maybeNextPage = None, maybeTotalPages = None)).pure[F]
  }

  private implicit lazy val eventsDecoder: EntityDecoder[F, List[PushEvent]] = {
    import io.renku.tinytypes.json.TinyTypeDecoders._
    import org.http4s.circe.jsonOf

    implicit val events: Decoder[Option[PushEvent]] = cursor =>
      for {
        projectId  <- cursor.downField("project_id").as[projects.Id]
        commitFrom <- cursor.downField("push_data").downField("commit_from").as[Option[CommitId]]
        commitTo   <- cursor.downField("push_data").downField("commit_to").as[Option[CommitId]]
        authorId   <- cursor.downField("author").downField("id").as[persons.GitLabId]
        authorName <- cursor.downField("author").downField("name").as[persons.Name]
      } yield (commitTo orElse commitFrom).map(PushEvent(projectId, _, authorId, authorName))

    jsonOf[F, List[Option[PushEvent]]].map(_.flatten)
  }
}

private object ProjectEventsFinder {
  def apply[F[_]: Async: Logger](gitLabThrottler: Throttler[F, GitLab]): F[ProjectEventsFinder[F]] = for {
    gitLabUrl <- GitLabUrlLoader[F]()
  } yield new ProjectEventsFinderImpl[F](gitLabUrl.apiV4, gitLabThrottler)
}

private[projectinfo] case class PushEvent(projectId:  projects.Id,
                                          commitId:   CommitId,
                                          authorId:   persons.GitLabId,
                                          authorName: persons.Name
)

private[projectinfo] case class PagingInfo(maybeNextPage: Option[Int], maybeTotalPages: Option[Int]) {

  private val pagesToCheck = 30
  private val step         = maybeTotalPages.map(_ / pagesToCheck).getOrElse(1)

  lazy val findNextPage: Option[Int] =
    if (maybeTotalPages.isEmpty) maybeNextPage
    else
      (maybeNextPage -> maybeTotalPages) mapN {
        case nextPage -> total if total < pagesToCheck => nextPage
        case nextPage -> total if nextPage == total    => nextPage
        case nextPage -> _ if nextPage     % step == 0 => nextPage
        case nextPage -> total if nextPage % step > 0 =>
          val next = nextPage - (nextPage % step) + step
          if (next >= total) total
          else next
      }
}
