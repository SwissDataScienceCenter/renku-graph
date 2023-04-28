/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.renku.graph.acceptancetests.stubs.gitlab

import GitLabApiStub.State
import GitLabStateGenerators._
import GitLabStateQueries._
import GitLabStateUpdates._
import JsonEncoders._
import cats.data.{EitherT, NonEmptyList, OptionT}
import cats.effect._
import cats.syntax.all._
import com.comcast.ip4s.{Host, Port}
import io.circe.Json
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.acceptancetests.data.Project
import io.renku.graph.acceptancetests.data.Project.Permissions.AccessLevel
import io.renku.graph.acceptancetests.stubs.gitlab.GitLabAuth.AuthedReq.{AuthedProject, AuthedUser}
import io.renku.graph.model
import io.renku.graph.model.{persons, projects}
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.testentities.Person
import io.renku.http.client.AccessToken.ProjectAccessToken
import io.renku.http.client.UserAccessToken
import org.http4s._
import org.http4s.circe.CirceEntityCodec._
import org.http4s.client.Client
import org.http4s.dsl.Http4sDsl
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.{Router, Server}
import org.typelevel.log4cats.Logger

import java.time.{Instant, LocalDate}

/** GitLab Api subset that operates off a in-memory state. The data can be manipulated using `update` and can be
 *  inspected using `query`. 
 */
final class GitLabApiStub[F[_]: Async: Logger](private val stateRef: Ref[F, State])
    extends Http4sDsl[F]
    with Http4sDslUtils {
  private[this] val apiPrefix = "api/v4"
  private[this] val logger    = Logger[F]

  def update(f: State => State): F[Unit] =
    stateRef.update(f)

  def query[A](q: State => A): F[A] =
    stateRef.get.map(q)

  def allRoutes: HttpRoutes[F] =
    Router(
      s"$apiPrefix/user"     -> userRoutes,
      s"$apiPrefix/users"    -> usersRoutes,
      s"$apiPrefix/projects" -> (brokenProjects <+> projectRoutes)
    ) <+> stubIncomplete

  private def userRoutes: HttpRoutes[F] =
    GitLabAuth.authF(stateRef) { authedReq =>
      HttpRoutes.of {
        case GET -> Root  => Ok(authedReq)
        case HEAD -> Root => Ok()
      }
    }

  private def usersRoutes: HttpRoutes[F] =
    GitLabAuth.authOptF(stateRef) { maybeAuthedReq =>
      HttpRoutes.of {
        case GET -> Root / UserGitLabId(id) =>
          query(findPersonById(id)).flatMap(OkOrNotFound(_))
        case req @ GET -> Root / UserGitLabId(id) / "projects" =>
          maybeAuthedReq match {
            case Some(AuthedUser(userId, _)) =>
              if (id == userId) query(projectsFor(id.some)).flatMap(OkWithTotalHeader(req))
              else OkWithTotalHeader(req)(List.empty[Project])
            case Some(AuthedProject(projectId, _)) =>
              query(findProjectById(projectId)).map(_.toList).flatMap(OkWithTotalHeader(req))
            case None =>
              query(projectsFor(id.some)).flatMap(OkWithTotalHeader(req))
          }
      }
    }

  private def projectRoutes: HttpRoutes[F] =
    GitLabAuth.authOptF(stateRef) { maybeAuthedReq =>
      HttpRoutes.of {

        case req @ GET -> Root :? Membership(true) +& MinAccessLevel(AccessLevel.Maintainer) =>
          query(findCallerProjects(maybeAuthedReq)).flatMap(OkWithTotalHeader(req))

        case DELETE -> Root / ProjectId(id) =>
          update(removeProject(id)).map(_ => Response[F](Accepted))

        case GET -> Root / ProjectId(id) =>
          query(findProjectById(id, maybeAuthedReq)).flatMap(OkOrNotFound(_))

        case HEAD -> Root / ProjectId(id) =>
          query(findProjectById(id, maybeAuthedReq)).flatMap(EmptyOkOrNotFound(_))

        case GET -> Root / ProjectPath(path) =>
          query(findProjectByPath(path, maybeAuthedReq)).flatMap(OkOrNotFound(_))

        case HEAD -> Root / ProjectPath(path) =>
          query(findProjectByPath(path, maybeAuthedReq)).flatMap(EmptyOkOrNotFound(_))

        case GET -> Root / ProjectPath(path) / ("users" | "members") =>
          query(findProjectByPath(path, maybeAuthedReq))
            .map(_.toList.flatMap(_.members.toList))
            .flatMap(Ok(_))

        case GET -> Root / ProjectId(id) / "repository" / "commits" =>
          query(commitsFor(id, maybeAuthedReq)).flatMap(Ok(_))

        case GET -> Root / ProjectId(id) / "repository" / "commits" / CommitIdVar(sha) =>
          query(findCommit(id, maybeAuthedReq, sha)).flatMap(OkOrNotFound(_))

        case req @ POST -> Root / ProjectId(projectId) / "access_tokens" =>
          EitherT(req.as[Json].map(_.hcursor.downField("expires_at").as[LocalDate]))
            .map(expiryDate => projectAccessTokenInfos.generateOne.copy(expiryDate = expiryDate))
            .semiflatTap(tokenInfo => update(addProjectAccessToken(projectId, tokenInfo)))
            .semiflatMap(tokenInfo => Created().map(_.withEntity(tokenInfo.asJson)))
            .leftSemiflatMap(err => BadRequest(s"No or invalid Project Access Token creation payload: ${err.message}"))
            .merge

        case GET -> Root / ProjectId(id) / "access_tokens" =>
          query(findProjectAccessTokens(id)).flatMap(Ok(_))

        case DELETE -> Root / ProjectId(id) / "access_tokens" / ProjectAccessTokenId(tokenId) =>
          query(findProjectAccessTokens(id)).map(_.find(_.tokenId == tokenId)).flatMap {
            case Some(_) => NoContent()
            case None    => NotFound()
          }

        case GET -> Root / ProjectId(id) / "hooks" =>
          query(findWebhooks(id)).flatMap(Ok(_))

        case POST -> Root / ProjectId(_) / "hooks" =>
          Created()

        case GET -> Root / ProjectId(id) / "events" :? PageParam(page) :? ActionParam(action) =>
          // action is always "pushed", page is always 1
          if (action != "pushed".some || page != 1.some) {
            val msg =
              s"Unexpected request parameters: got action=$action, page=$page, expected action=pushed, page=1"
            logger.error(msg) *> BadRequest(msg)
          } else query(findPushEvents(id, maybeAuthedReq)).flatMap(Ok(_))

        case DELETE -> Root / ProjectId(id) / "hooks" / IntVar(hookId) =>
          stateRef.modify(removeWebhook(id, hookId)).flatMap {
            case true  => Ok()
            case false => NotFound()
          }
      }
    }

  private def brokenProjects: HttpRoutes[F] =
    HttpRoutes {
      case GET -> Root / ProjectId(id) =>
        OptionT.liftF(query(isProjectBroken(id))).flatMap {
          case true  => OptionT.liftF(InternalServerError())
          case false => OptionT.none
        }
      case GET -> Root / ProjectPath(path) =>
        OptionT.liftF(query(isProjectBroken(path))).flatMap {
          case true  => OptionT.liftF(InternalServerError())
          case false => OptionT.none
        }

      case _ => OptionT.none[F, Response[F]]
    }

  private def stubIncomplete: HttpRoutes[F] =
    HttpRoutes { req =>
      val message = s"GitLabApiStub doesn't have this route implemented: ${req.method} ${req.pathInfo.renderString}"
      OptionT
        .liftF(logger.error(message))
        .semiflatMap(_ => ServiceUnavailable(message))
    }

  def client: Client[F] =
    Client.fromHttpApp(allRoutes.orNotFound)

  def resource(bindAddress: Host, port: Port): Resource[F, Server] =
    Resource.eval(logger.trace(s"Starting GitLab stub on $bindAddress:$port")) *>
      EmberServerBuilder
        .default[F]
        .withHost(bindAddress)
        .withPort(port)
        .withHttpApp(enableLogging[F](allRoutes.orNotFound))
        .build

  def resource(bindAddress: String, port: Int): Resource[F, Server] =
    (Host.fromString(bindAddress), Port.fromInt(port))
      .mapN(resource)
      .getOrElse(sys.error("Invalid host or port"))
}

object GitLabApiStub {
  def apply[F[_]: Async: Logger](initial: State): F[GitLabApiStub[F]] =
    Ref.of[F, State](initial).map(new GitLabApiStub[F](_))

  def empty[F[_]: Async: Logger]: F[GitLabApiStub[F]] =
    apply(State.empty)

  final case class State(
      users:               Map[model.persons.GitLabId, UserAccessToken],
      projectAccessTokens: Map[model.projects.GitLabId, ProjectAccessTokenInfo],
      persons:             List[Person],
      projects:            List[Project],
      webhooks:            List[Webhook],
      commits:             Map[model.projects.GitLabId, NonEmptyList[CommitData]],
      brokenProjects:      Set[model.projects.GitLabId]
  )
  object State {
    val empty: State = State(Map.empty, Map.empty, Nil, Nil, Nil, Map.empty, Set.empty)
  }

  final case class Webhook(webhookId: Int, projectId: projects.GitLabId, url: Uri)
  final case class PushEvent(projectId:  projects.GitLabId,
                             commitId:   CommitId,
                             authorId:   persons.GitLabId,
                             authorName: persons.Name
  )
  final case class CommitData(
      commitId:  CommitId,
      author:    Person,
      committer: Person,
      date:      Instant,
      message:   String,
      parents:   List[CommitId]
  )
  final case class ProjectAccessTokenInfo(tokenId: Int, name: String, token: ProjectAccessToken, expiryDate: LocalDate)
}
