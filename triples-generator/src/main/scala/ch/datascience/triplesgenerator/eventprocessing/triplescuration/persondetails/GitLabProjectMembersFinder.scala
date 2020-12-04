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
import org.http4s.{EntityDecoder, Request, Response, Status}

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
  )(implicit maybeAccessToken: Option[AccessToken]): IO[Set[GitLabProjectMember]] =
    for {
      projectsUri <- validateUri(s"$gitLabUrl/api/v4/projects/${urlEncode(path.value)}/users")
      users       <- send(request(GET, projectsUri, maybeAccessToken))(mapResponse)
    } yield users

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Set[GitLabProjectMember]]] = {
    case (Ok, _, response) => response.as[List[GitLabProjectMember]].map(_.toSet)
    case (NotFound, _, _)  => Set.empty[GitLabProjectMember].pure[IO]
  }

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
