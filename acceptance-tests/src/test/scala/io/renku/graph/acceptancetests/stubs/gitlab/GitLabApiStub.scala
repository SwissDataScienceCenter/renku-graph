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
import StateSyntax._
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.projects.Id
import org.http4s.server.Router

import java.time.Instant

final class GitLabApiStub[F[_]: Async: Logger](private val stateRef: Ref[F, State])
    extends Http4sDsl[F]
    with Http4sDslUtils {
  private[this] val apiPrefix = "api/v4"

  def update(f: State => State): F[Unit] =
    stateRef.update(f)

  def allRoutes: HttpRoutes[F] =
    Router(
      s"$apiPrefix/user"     -> userRoutes,
      s"$apiPrefix/users"    -> usersRoutes,
      s"$apiPrefix/projects" -> projectRoutes
    ) <+> stubIncomplete

  def userRoutes: HttpRoutes[F] =
    withState { state =>
      GitLabAuth.auth(state) { authenticatedUser =>
        HttpRoutes.of { case GET -> Root =>
          Ok(authenticatedUser)
        }
      }
    }

  def usersRoutes: HttpRoutes[F] =
    withState { state =>
      GitLabAuth.auth(state) { authenticatedUser =>
        HttpRoutes.of {
          case GET -> Root / GitLabIdVar(id) =>
            OkOrNotFound(state.findPersonById(id))

          case GET -> Root / GitLabIdVar(userId) / "projects" =>
            if (userId == authenticatedUser.id) Ok(state.projectsFor(userId.some))
            else Ok(List.empty[Project])
        }
      }
    }

  def projectRoutes: HttpRoutes[F] =
    withState { state =>
      GitLabAuth.authOpt(state) { authenticatedUser =>
        HttpRoutes.of {
          case GET -> Root / ProjectId(id) =>
            OkOrNotFound(state.findProject(id, authenticatedUser.map(_.id)))

          case GET -> Root / ProjectPath(path) =>
            OkOrNotFound(state.findProject(path, authenticatedUser.map(_.id)))

          case GET -> Root / ProjectPath(path) / ("users" | "members") =>
            Ok(
              state
                .findProject(path, authenticatedUser.map(_.id))
                .toList
                .flatMap(_.entitiesProject.members.toList)
            )

          case GET -> Root / ProjectId(id) / "repository" / "commits" =>
            Ok(state.commitsFor(id, authenticatedUser.map(_.id)))

          case GET -> Root / ProjectId(id) / "repository" / "commits" / CommitIdVar(sha) =>
            OkOrNotFound(state.findCommit(id, authenticatedUser.map(_.id), sha))

          case GET -> Root / ProjectId(id) / "hooks" =>
            Ok(state.findWebhooks(id))

          case GET -> Root / ProjectId(id) / "events" :? PageParam(page) :? ActionParam(action) =>
            // action is always "pushed", page is always 1
            if (action != "pushed".some || page != 1.some)
              BadRequest(s"(action=$action, page=$page) vs. expected (action=pushed,page=1)")
            else Ok(state.findPushEvents(id, authenticatedUser.map(_.id)))

          case DELETE -> Root / ProjectId(id) / "hooks" / IntVar(hookId) =>
            stateRef.modify(_.removeWebhook(id, hookId)).flatMap {
              case true  => Ok()
              case false => NotFound()
            }
        }
      }
    }

  def stubIncomplete: HttpRoutes[F] =
    HttpRoutes { req =>
      val message = s"GitLabApiStub doesn't have this route implemented: ${req.pathInfo.renderString}"
      OptionT
        .liftF(Logger[F].error(message))
        .semiflatMap(_ => InternalServerError(message))
    }

  def client: Client[F] =
    Client.fromHttpApp(allRoutes.orNotFound)

  def run(bindAddress: Host, port: Port): Stream[F, Nothing] =
    Stream.eval(Logger[F].info(s"Starting GitLab stub on $bindAddress:$port")).drain ++
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

  private def withState(cont: State => HttpRoutes[F]): HttpRoutes[F] =
    HttpRoutes(req => OptionT.liftF(stateRef.get).flatMap(s => cont(s).run(req)))
}

object GitLabApiStub {
  def apply[F[_]: Async: Logger](initial: State): F[GitLabApiStub[F]] =
    Ref.of[F, State](initial).map(new GitLabApiStub[F](_))

  def empty[F[_]: Async: Logger]: F[GitLabApiStub[F]] =
    apply(State.empty)

  final case class State(
      users:    Map[GitLabId, AccessToken],
      persons:  List[Person],
      projects: List[Project],
      webhooks: List[Webhook],
      commits:  Map[Id, NonEmptyList[CommitData]]
  )
  object State {
    val empty: State = State(Map.empty, Nil, Nil, Nil, Map.empty)
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
