package ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails

import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.projects.Path
import ch.datascience.graph.model.users.GitLabId
import ch.datascience.http.client.UrlEncoder.urlEncode
import ch.datascience.http.client.{AccessToken, IORestClient}
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder
import org.http4s.Method.GET
import org.http4s.circe.jsonOf
import org.http4s.dsl.io.{NotFound, Ok}
import org.http4s.util.CaseInsensitiveString
import org.http4s._

import scala.concurrent.ExecutionContext

private trait GitLabProjectMembersFinder[Interpretation[_]] {
  def findProjectMembers(path: Path)(implicit
      maybeAccessToken:        Option[AccessToken]
  ): Interpretation[Set[GitLabProjectMember]]
}

private class IOGitLabProjectMembersFinder(
    gitLabUrl:       GitLabUrl,
    gitLabThrottler: Throttler[IO, GitLab],
    logger:          Logger[IO]
)(implicit
    executionContext: ExecutionContext,
    contextShift:     ContextShift[IO],
    timer:            Timer[IO]
) extends IORestClient(gitLabThrottler, logger)
    with GitLabProjectMembersFinder[IO] {

  override def findProjectMembers(
      path:                    Path
  )(implicit maybeAccessToken: Option[AccessToken]): IO[Set[GitLabProjectMember]] = for {
    users   <- fetch(s"$gitLabUrl/api/v4/projects/${urlEncode(path.value)}/users")
    members <- fetch(s"$gitLabUrl/api/v4/projects/${urlEncode(path.value)}/members")
  } yield users ++ members

  private def fetch(
      url:                     String,
      maybePage:               Option[Int] = None,
      allUsers:                Set[GitLabProjectMember] = Set.empty
  )(implicit maybeAccessToken: Option[AccessToken]): IO[Set[GitLabProjectMember]] = for {
    uri                     <- validateUri(merge(url, maybePage))
    fetchedUsersAndNextPage <- send(request(GET, uri, maybeAccessToken))(mapResponse)
    allUsers                <- addNextPage(url, allUsers, fetchedUsersAndNextPage)
  } yield allUsers

  private def merge(url: String, maybePage: Option[Int] = None) =
    maybePage map (page => s"$url?page=$page") getOrElse url

  private lazy val mapResponse
      : PartialFunction[(Status, Request[IO], Response[IO]), IO[(Set[GitLabProjectMember], Option[Int])]] = {
    case (Ok, _, response) =>
      response
        .as[List[GitLabProjectMember]]
        .map(members => members.toSet -> maybeNextPage(response))
    case (NotFound, _, _) =>
      (Set.empty[GitLabProjectMember] -> Option.empty[Int]).pure[IO]
  }

  private def addNextPage(
      url:                          String,
      allUsers:                     Set[GitLabProjectMember],
      fetchedUsersAndMaybeNextPage: (Set[GitLabProjectMember], Option[Int])
  )(implicit maybeAccessToken:      Option[AccessToken]): IO[Set[GitLabProjectMember]] = fetchedUsersAndMaybeNextPage match {
    case (fetchedUsers, maybeNextPage @ Some(_)) => fetch(url, maybeNextPage, allUsers ++ fetchedUsers)
    case (fetchedUsers, None)                    => (allUsers ++ fetchedUsers).pure[IO]
  }

  private def maybeNextPage(response: Response[IO]): Option[Int] =
    response.headers.get(CaseInsensitiveString("X-Next-Page")).flatMap(_.value.toIntOption)

  private implicit lazy val projectDecoder: EntityDecoder[IO, List[GitLabProjectMember]] = {
    import ch.datascience.graph.model.users

    implicit val decoder: Decoder[GitLabProjectMember] = { cursor =>
      for {
        id       <- cursor.downField("id").as[GitLabId]
        username <- cursor.downField("username").as[users.Username]
        name     <- cursor.downField("name").as[users.Name]
      } yield GitLabProjectMember(id, username, name)
    }

    jsonOf[IO, List[GitLabProjectMember]]
  }
}

private object IOGitLabProjectMembersFinder {

  def apply(gitLabThrottler: Throttler[IO, GitLab], logger: Logger[IO])(implicit
      executionContext:      ExecutionContext,
      contextShift:          ContextShift[IO],
      timer:                 Timer[IO]
  ): IO[GitLabProjectMembersFinder[IO]] = for {
    gitLabUrl <- GitLabUrl[IO]()
  } yield new IOGitLabProjectMembersFinder(gitLabUrl, gitLabThrottler, logger)
}
