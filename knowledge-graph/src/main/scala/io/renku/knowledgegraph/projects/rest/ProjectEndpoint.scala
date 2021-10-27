/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.projects.rest

import cats.effect._
import cats.syntax.all._
import cats.{MonadThrow, Parallel}
import io.renku.config.{GitLab, renku}
import io.renku.control.Throttler
import io.renku.graph.model.projects
import io.renku.http.InfoMessage._
import io.renku.http.rest.Links.{Href, Link, Rel, _links}
import io.renku.http.server.security.model.AuthUser
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.knowledgegraph.projects.model.Permissions._
import io.renku.knowledgegraph.projects.model._
import io.renku.logging.ExecutionTimeRecorder
import io.renku.rdfstore.SparqlQueryTimeRecorder
import org.http4s.Response
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait ProjectEndpoint[F[_]] {
  def getProject(path: projects.Path, maybeAuthUser: Option[AuthUser]): F[Response[F]]
}

class ProjectEndpointImpl[F[_]: MonadThrow: Logger](
    projectFinder:         ProjectFinder[F],
    renkuResourcesUrl:     renku.ResourcesUrl,
    executionTimeRecorder: ExecutionTimeRecorder[F]
) extends Http4sDsl[F]
    with ProjectEndpoint[F] {

  import executionTimeRecorder._
  import io.circe.literal._
  import io.circe.syntax._
  import io.circe.{Encoder, Json}
  import org.http4s.circe._

  def getProject(path: projects.Path, maybeAuthUser: Option[AuthUser]): F[Response[F]] =
    measureExecutionTime {
      projectFinder
        .findProject(path, maybeAuthUser)
        .flatMap(toHttpResult(path))
        .recoverWith(httpResult(path))
    } map logExecutionTimeWhen(finishedSuccessfully(path))

  private def toHttpResult(
      path: projects.Path
  ): Option[Project] => F[Response[F]] = {
    case None          => NotFound(InfoMessage(s"No '$path' project found"))
    case Some(project) => Ok(project.asJson)
  }

  private def httpResult(
      path: projects.Path
  ): PartialFunction[Throwable, F[Response[F]]] = { case NonFatal(exception) =>
    val errorMessage = ErrorMessage(s"Finding '$path' project failed")
    Logger[F].error(exception)(errorMessage.value)
    InternalServerError(errorMessage)
  }

  private def finishedSuccessfully(projectPath: projects.Path): PartialFunction[Response[F], String] = {
    case response if response.status == Ok || response.status == NotFound =>
      s"Finding '$projectPath' details finished"
  }

  private implicit lazy val projectEncoder: Encoder[Project] = Encoder.instance[Project] { project =>
    json"""{
      "identifier": ${project.id.value},
      "path":       ${project.path.value},
      "name":       ${project.name.value},
      "visibility": ${project.visibility.value},
      "created":    ${project.created},
      "updatedAt":  ${project.updatedAt.value},
      "urls":       ${project.urls},
      "forking":    ${project.forking},
      "tags":       ${project.tags.map(_.value).toList},
      "starsCount": ${project.starsCount.value},
      "permissions":${project.permissions},
      "statistics": ${project.statistics},
      "version":    ${project.version.value}
    }""" deepMerge _links(
      Link(Rel.Self        -> Href(renkuResourcesUrl / "projects" / project.path)),
      Link(Rel("datasets") -> Href(renkuResourcesUrl / "projects" / project.path / "datasets"))
    ) deepMerge (project.maybeDescription.map(desc => json"""{"description": ${desc.value}}""") getOrElse Json.obj())
  }

  private implicit lazy val creatorEncoder: Encoder[Creator] = Encoder.instance[Creator] { creator =>
    json"""{
      "name":  ${creator.name.value}
    }""" deepMerge (creator.maybeEmail.map(email => json"""{"email": ${email.value}}""") getOrElse Json.obj())
  }

  private implicit lazy val urlsEncoder: Encoder[Urls] = Encoder.instance[Urls] { urls =>
    json"""{
      "ssh":    ${urls.ssh.value},
      "http":   ${urls.http.value},
      "web":    ${urls.web.value}
    }""" deepMerge (urls.maybeReadme.map(readme => json"""{"readme": ${readme.value}}""") getOrElse Json.obj())
  }

  private implicit lazy val forkingEncoder: Encoder[Forking] = Encoder.instance[Forking] { forks =>
    json"""{
      "forksCount": ${forks.forksCount.value}
    }""" deepMerge (forks.maybeParent.map(parent => json"""{"parent": $parent}""") getOrElse Json.obj())
  }

  private implicit lazy val parentProjectEncoder: Encoder[ParentProject] = Encoder.instance[ParentProject] { parent =>
    json"""{
      "path":    ${parent.path.value},
      "name":    ${parent.name.value},
      "created": ${parent.created}
    }"""
  }

  private implicit lazy val creationEncoder: Encoder[Creation] = Encoder.instance[Creation] { created =>
    json"""{
      "dateCreated": ${created.date.value}
    }""" deepMerge (created.maybeCreator.map(creator => json"""{"creator": $creator}""") getOrElse Json.obj())
  }

  private implicit lazy val permissionsEncoder: Encoder[Permissions] = Encoder.instance[Permissions] {
    case ProjectAndGroupPermissions(projectAccessLevel, groupAccessLevel) => json"""{
      "projectAccess": ${projectAccessLevel.accessLevel},
      "groupAccess":   ${groupAccessLevel.accessLevel}
    }"""
    case ProjectPermissions(accessLevel) => json"""{
      "projectAccess": ${accessLevel.accessLevel}
    }"""
    case GroupPermissions(accessLevel) => json"""{
      "groupAccess": ${accessLevel.accessLevel}
    }"""
  }

  private implicit lazy val accessLevelEncoder: Encoder[AccessLevel] = Encoder.instance[AccessLevel] { level =>
    json"""{
      "level": {
        "name":  ${level.name.value},
        "value": ${level.value.value}
      }
    }"""
  }

  private implicit lazy val statisticsEncoder: Encoder[Statistics] = Encoder.instance[Statistics] { stats =>
    json"""{
      "commitsCount":     ${stats.commitsCount.value},
      "storageSize":      ${stats.storageSize.value},
      "repositorySize":   ${stats.repositorySize.value},
      "lfsObjectsSize":   ${stats.lsfObjectsSize.value},
      "jobArtifactsSize": ${stats.jobArtifactsSize.value}
    }"""
  }
}

object ProjectEndpoint {

  def apply[F[_]: Parallel: Async: Logger](
      gitLabThrottler: Throttler[F, GitLab],
      timeRecorder:    SparqlQueryTimeRecorder[F]
  ): F[ProjectEndpoint[F]] = for {
    projectFinder         <- ProjectFinder[F](gitLabThrottler, timeRecorder)
    renkuResourceUrl      <- renku.ResourcesUrl[F]()
    executionTimeRecorder <- ExecutionTimeRecorder[F]()
  } yield new ProjectEndpointImpl[F](
    projectFinder,
    renkuResourceUrl,
    executionTimeRecorder
  )
}
