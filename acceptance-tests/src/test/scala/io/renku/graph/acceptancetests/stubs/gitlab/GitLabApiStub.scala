/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

import cats.effect._
import cats.data.{NonEmptyList, OptionT}
import cats.syntax.all._
import fs2.Stream
import io.renku.graph.acceptancetests.data.Project
import io.renku.graph.acceptancetests.stubs.gitlab.GitLabApiStub.State
import io.renku.graph.model.persons.{GitLabId, Name}
import io.renku.graph.model.testentities.Person
import io.renku.http.client.AccessToken
import org.http4s._
import org.http4s.dsl.Http4sDsl
import org.http4s.circe.CirceEntityCodec._
import org.typelevel.log4cats.Logger
import JsonCodec._
import com.comcast.ip4s.{Host, Port}
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.client.Client
import io.renku.graph.acceptancetests.stubs.gitlab.GitLabStateQueries._
import io.renku.graph.acceptancetests.stubs.gitlab.GitLabStateUpdates._
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.projects.Id
import org.http4s.server.Router

import java.time.Instant

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
    GitLabAuth.authF(stateRef) { authenticatedUser =>
      HttpRoutes.of { case GET -> Root =>
        Ok(authenticatedUser)
      }
    }

  private def usersRoutes: HttpRoutes[F] =
    GitLabAuth.authOptF(stateRef) { authenticatedUser =>
      HttpRoutes.of {
        case GET -> Root / GitLabIdVar(id) =>
          query(findPersonById(id)).flatMap(OkOrNotFound(_))

        case GET -> Root / GitLabIdVar(userId) / "projects" =>
          if (userId.some == authenticatedUser.map(_.id)) query(projectsFor(userId.some)).flatMap(Ok(_))
          else Ok(List.empty[Project])
      }
    }

  private def projectRoutes: HttpRoutes[F] =
    GitLabAuth.authOptF(stateRef) { authenticatedUser =>
      HttpRoutes.of {
        case GET -> Root / ProjectId(id) =>
          query(findProject(id, authenticatedUser.map(_.id))).flatMap(OkOrNotFound(_))

        case GET -> Root / ProjectPath(path) =>
          query(findProject(path, authenticatedUser.map(_.id))).flatMap(OkOrNotFound(_))

        case GET -> Root / ProjectPath(path) / ("users" | "members") =>
          for {
            project <- query(findProject(path, authenticatedUser.map(_.id)))
            members = project.toList.flatMap(_.entitiesProject.members.toList)
            resp <- Ok(members)
          } yield resp

        case GET -> Root / ProjectId(id) / "repository" / "commits" =>
          query(commitsFor(id, authenticatedUser.map(_.id))).flatMap(Ok(_))

        case GET -> Root / ProjectId(id) / "repository" / "commits" / CommitIdVar(sha) =>
          query(findCommit(id, authenticatedUser.map(_.id), sha)).flatMap(OkOrNotFound(_))

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
          } else query(findPushEvents(id, authenticatedUser.map(_.id))).flatMap(Ok(_))

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
        .semiflatMap(_ => InternalServerError(message))
    }

  def client: Client[F] =
    Client.fromHttpApp(allRoutes.orNotFound)

  def run(bindAddress: Host, port: Port): Stream[F, Nothing] =
    Stream.eval(logger.info(s"Starting GitLab stub on $bindAddress:$port")).drain ++
      BlazeServerBuilder[F]
        .bindHttp(port.value, bindAddress.show)
        .withoutBanner
        .withHttpApp(enableLogging[F](allRoutes.orNotFound))
        .serve
        .drain

  def run(bindAddress: String, port: Int): Stream[F, ExitCode] =
    (Host.fromString(bindAddress), Port.fromInt(port))
      .mapN(run)
      .getOrElse(sys.error("Invalid host or port"))
}

object GitLabApiStub {
  def apply[F[_]: Async: Logger](initial: State): F[GitLabApiStub[F]] =
    Ref.of[F, State](initial).map(new GitLabApiStub[F](_))

  def empty[F[_]: Async: Logger]: F[GitLabApiStub[F]] =
    apply(State.empty)

  final case class State(
      users:          Map[GitLabId, AccessToken],
      persons:        List[Person],
      projects:       List[Project],
      webhooks:       List[Webhook],
      commits:        Map[Id, NonEmptyList[CommitData]],
      brokenProjects: Set[Id]
  )
  object State {
    val empty: State = State(Map.empty, Nil, Nil, Nil, Map.empty, Set.empty)
  }

  final case class Webhook(webhookId: Int, projectId: Id, url: Uri)
  final case class PushEvent(projectId: Id, commitId: CommitId, authorId: GitLabId, authorName: Name)
  final case class CommitData(
      commitId:  CommitId,
      author:    Person,
      committer: Person,
      date:      Instant,
      message:   String,
      parents:   List[CommitId]
  )
}
