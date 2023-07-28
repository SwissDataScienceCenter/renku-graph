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

package io.renku.knowledgegraph.projects.details

import cats.MonadThrow
import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.config.renku
import io.renku.graph.model.GitLabUrl
import io.renku.http.rest.Links.{Link, Rel, _links}
import io.renku.json.JsonOps._
import io.renku.knowledgegraph
import io.renku.knowledgegraph.projects.images.ImagesEncoder
import model.Permissions._
import model._

private trait ProjectJsonEncoder {
  def encode(project: model.Project)(implicit gitLabUrl: GitLabUrl): Json
}

private object ProjectJsonEncoder {
  def apply[F[_]: MonadThrow]: F[ProjectJsonEncoder] = renku.ApiUrl[F]().map(new ProjectJsonEncoderImpl(_))
}

private class ProjectJsonEncoderImpl(renkuApiUrl: renku.ApiUrl) extends ProjectJsonEncoder with ImagesEncoder {

  override def encode(project: model.Project)(implicit gitLabUrl: GitLabUrl): Json = project.asJson

  private implicit def encoder(implicit gitLabUrl: GitLabUrl): Encoder[Project] = Encoder.instance[Project] { project =>
    json"""{
      "identifier":   ${project.id},
      "path":         ${project.slug},
      "name":         ${project.name},
      "visibility":   ${project.visibility},
      "created":      ${project.created},
      "dateModified": ${project.dateModified},
      "urls":         ${project.urls},
      "forking":      ${project.forking},
      "keywords":     ${project.keywords.toList.sorted},
      "starsCount":   ${project.starsCount},
      "permissions":  ${project.permissions},
      "images":       ${project.images -> project.slug},
      "statistics":   ${project.statistics}
    }""" deepMerge _links(
      Link(Rel.Self        -> Endpoint.href(renkuApiUrl, project.slug)),
      Link(Rel("datasets") -> knowledgegraph.projects.datasets.Endpoint.href(renkuApiUrl, project.slug))
    ).addIfDefined("description" -> project.maybeDescription)
      .addIfDefined("version" -> project.maybeVersion)
  }

  private implicit lazy val creatorEncoder: Encoder[Creator] = Encoder.instance[Creator] { creator =>
    json"""{
      "name": ${creator.name}
    }"""
      .addIfDefined("email" -> creator.maybeEmail)
      .addIfDefined("affiliation" -> creator.maybeAffiliation)
  }

  private implicit lazy val urlsEncoder: Encoder[Urls] = Encoder.instance[Urls] { urls =>
    json"""{
      "ssh":  ${urls.ssh},
      "http": ${urls.http},
      "web":  ${urls.web}
    }""" addIfDefined ("readme" -> urls.maybeReadme)
  }

  private implicit lazy val forkingEncoder: Encoder[Forking] = Encoder.instance[Forking] { forks =>
    json"""{
      "forksCount": ${forks.forksCount}
    }""" addIfDefined ("parent" -> forks.maybeParent)
  }

  private implicit lazy val parentProjectEncoder: Encoder[ParentProject] = Encoder.instance[ParentProject] { parent =>
    json"""{
      "path":    ${parent.slug},
      "name":    ${parent.name},
      "created": ${parent.created}
    }"""
  }

  private implicit lazy val creationEncoder: Encoder[Creation] = Encoder.instance[Creation] { created =>
    json"""{
      "dateCreated": ${created.date}
    }""" addIfDefined ("creator" -> created.maybeCreator)
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
      "commitsCount":     ${stats.commitsCount},
      "storageSize":      ${stats.storageSize},
      "repositorySize":   ${stats.repositorySize},
      "lfsObjectsSize":   ${stats.lsfObjectsSize},
      "jobArtifactsSize": ${stats.jobArtifactsSize}
    }"""
  }
}
