package ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails
import cats.data.OptionT
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
//import io.circe.Decoder.decodeList
import org.http4s.Method.GET
import org.http4s.circe.jsonOf
import org.http4s.dsl.io.{NotFound, Ok}
import org.http4s.{EntityDecoder, Request, Response, Status}

import scala.concurrent.ExecutionContext

trait GitLabProjectMembersFinder[Interpretation[_]] {
  def findProjectMembers(path: Path)(implicit
      maybeAccessToken:        Option[AccessToken]
  ): Interpretation[List[GitLabProjectMember]]
}

private class IOGitLabProjectMembersFinder(gitLabUrl:       GitLabUrl,
                                           gitLabThrottler: Throttler[IO, GitLab],
                                           logger:          Logger[IO]
)(implicit
    maybeAccessToken: Option[AccessToken],
    executionContext: ExecutionContext,
    contextShift:     ContextShift[IO],
    timer:            Timer[IO]
) extends IORestClient(gitLabThrottler, logger)
    with GitLabProjectMembersFinder[IO] {

  override def findProjectMembers(path: Path)(implicit
      maybeAccessToken:                 Option[AccessToken]
  ): IO[List[GitLabProjectMember]] =
    for {
      projectsUri <- validateUri(s"$gitLabUrl/api/v4/projects/${urlEncode(path.value)}/users")
      users <-
        send(request(GET, projectsUri, maybeAccessToken))(
          mapTo[List[GitLabProjectMember]]
        ).map(_.getOrElse(Nil))
    } yield users

  private def mapTo[OUT](implicit
      decoder: EntityDecoder[IO, OUT]
  ): PartialFunction[(Status, Request[IO], Response[IO]), IO[Option[OUT]]] = {
    case (Ok, _, response) => response.as[OUT].map(Option.apply)
    case (NotFound, _, _)  => None.pure[IO]
  }

  private implicit lazy val projectDecoder: EntityDecoder[IO, List[GitLabProjectMember]] = {
    import ch.datascience.graph.model.users

    implicit val decoder: Decoder[List[GitLabProjectMember]] = { cursor =>
      for {
        id       <- cursor.downField("id").as[GitLabId]
        username <- cursor.downField("username").as[users.Username]
        name     <- cursor.downField("name").as[users.Name]
      } yield GitLabProjectMember(id, username, name)

    }

    jsonOf[IO, List[GitLabProjectMember]]
  }

  private implicit class IOOptionOps[T](io: IO[Option[T]]) {
    lazy val toOptionT: OptionT[IO, T] = OptionT(io)
  }

}

object GitLabProjectMembersFinder {
  def apply[Interpretation[_]](gitLabUrl: GitLabUrl, gitLabThrottler: Throttler[IO, GitLab], logger: Logger[IO])(
      implicit
      maybeAccessToken: Option[AccessToken],
      executionContext: ExecutionContext,
      contextShift:     ContextShift[Interpretation],
      timer:            Timer[Interpretation]
  ): GitLabProjectMembersFinder[Interpretation] = new IOGitLabProjectMembersFinder(gitLabUrl, gitLabThrottler, logger)
}
