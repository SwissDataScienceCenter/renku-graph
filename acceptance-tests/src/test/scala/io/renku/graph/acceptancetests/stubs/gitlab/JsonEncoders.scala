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

import io.circe._
import io.circe.literal._
import io.circe.syntax._
import io.renku.graph.acceptancetests.data.Project
import io.renku.graph.acceptancetests.data.Project.Permissions._
import io.renku.graph.acceptancetests.data.Project.{Permissions, Statistics}
import io.renku.graph.acceptancetests.stubs.gitlab.GitLabApiStub._
import io.renku.graph.acceptancetests.stubs.gitlab.GitLabAuth.AuthedReq
import io.renku.graph.acceptancetests.stubs.gitlab.GitLabAuth.AuthedReq.{AuthedProject, AuthedUser}
import io.renku.graph.model.entities.Project.ProjectMember
import io.renku.graph.model.testentities.{Parent, Person}
import org.http4s.Uri

import java.time.Instant

trait JsonEncoders {
  implicit val uriEncoder: Encoder[Uri] =
    Encoder.encodeString.contramap(_.renderString)

  implicit val personEncoder: Encoder[Person] = Encoder.instance { person =>
    Map("id" -> person.maybeGitLabId.asJson, "username" -> person.name.asJson, "name" -> person.name.asJson).asJson
  }

  implicit val projectMemberEncoder: Encoder[ProjectMember] = Encoder.instance { pm =>
    Map("id"           -> pm.gitLabId.asJson,
        "username"     -> pm.username.asJson,
        "name"         -> pm.name.asJson,
        "access_level" -> 40.asJson,
        "state"        -> "active".asJson
    ).asJson
  }

  implicit val pushEventEncoder: Encoder[PushEvent] = Encoder.instance { ev =>
    Map(
      "project_id" -> ev.projectId.asJson,
      "push_data" -> Json.obj(
        "commit_from" -> Json.Null,
        "commit_to"   -> ev.commitId.asJson
      ),
      "author" ->
        Json.obj(
          "id"   -> ev.authorId.asJson,
          "name" -> ev.authorName.asJson
        )
    ).asJson
  }

  implicit val commitDataEncoder: Encoder[CommitData] =
    Encoder.instance { c =>
      Json.obj(
        "id"              -> c.commitId.asJson,
        "author_name"     -> c.author.name.asJson,
        "author_email"    -> c.author.maybeEmail.asJson,
        "committer_name"  -> c.committer.name.asJson,
        "committer_email" -> c.committer.maybeEmail.asJson,
        "message"         -> c.message.asJson,
        "committed_date"  -> c.date.asJson,
        "parent_ids"      -> c.parents.asJson
      )
    }

  implicit val personalAccessTokenCreationEncoder: Encoder[ProjectAccessTokenInfo] =
    Encoder.instance { case ProjectAccessTokenInfo(tokenId, name, token, expiryDate) =>
      json"""{
        "id":         $tokenId,
        "name":       $name,
        "token":      ${token.value},
        "created_at": ${Instant.now()},
        "expires_at": $expiryDate
      }"""
    }

  implicit val webhookEncoder: Encoder[Webhook] =
    Encoder.instance(h => Map("id" -> h.webhookId.asJson, "url" -> h.url.asJson).asJson)

  implicit val accessLevelEncoder: Encoder[AccessLevel] =
    Encoder.instance(level => Json.obj("access_level" -> level.value.value.asJson))

  implicit val permissionEncoder: Encoder[Permissions] =
    Encoder.instance {
      case ProjectAndGroupPermissions(project, group) =>
        Json.obj(
          "project_access" -> project.accessLevel.asJson,
          "group_access"   -> group.accessLevel.asJson
        )
      case ProjectPermissions(project) =>
        Json.obj("project_access" -> project.accessLevel.asJson, "group_access" -> Json.Null)
      case GroupPermissions(group) =>
        Json.obj("project_access" -> Json.Null, "group_access" -> group.accessLevel.asJson)
    }

  implicit def authUserEncoder[AR <: AuthedReq]: Encoder[AR] =
    Encoder.encodeMap[String, Json].contramap {
      case AuthedUser(userId, _)       => Map("id" -> userId.asJson)
      case AuthedProject(projectId, _) => Map("id" -> projectId.asJson)
    }

  implicit val projectStatisticsEncoder: Encoder[Statistics] =
    Encoder.instance(stats =>
      Json.obj(
        "commit_count"       -> stats.commitsCount.value.asJson,
        "storage_size"       -> stats.storageSize.value.asJson,
        "repository_size"    -> stats.repositorySize.value.asJson,
        "lfs_objects_size"   -> stats.lsfObjectsSize.value.asJson,
        "job_artifacts_size" -> stats.jobArtifactsSize.value.asJson
      )
    )

  implicit val projectEncoder: Encoder[Project] =
    Encoder.instance { project =>
      Json
        .obj(
          "id"                  -> project.id.asJson,
          "description"         -> project.entitiesProject.maybeDescription.map(_.value.asJson).getOrElse(Json.Null),
          "visibility"          -> project.entitiesProject.visibility.value.toLowerCase.asJson,
          "ssh_url_to_repo"     -> project.urls.ssh.value.asJson,
          "http_url_to_repo"    -> project.urls.http.value.asJson,
          "web_url"             -> project.urls.web.value.asJson,
          "readme_url"          -> project.urls.maybeReadme.map(_.value).asJson,
          "forks_count"         -> project.entitiesProject.forksCount.value.asJson,
          "topics"              -> project.entitiesProject.keywords.map(_.value).asJson,
          "name"                -> project.name.value.asJson,
          "star_count"          -> project.starsCount.value.asJson,
          "path_with_namespace" -> project.path.value.asJson,
          "created_at"          -> project.entitiesProject.dateCreated.value.asJson,
          "creator_id"          -> project.maybeCreator.map(_.gitLabId).asJson,
          "last_activity_at"    -> project.updatedAt.value.asJson,
          "permissions"         -> project.permissions.asJson,
          "statistics"          -> project.statistics.asJson,
          "forked_from_project" -> (project.entitiesProject match {
            case withParent: Parent =>
              Json.obj("path_with_namespace" -> withParent.parent.path.value.asJson)
            case _ => Json.Null
          })
        )
        .dropNullValues
    }
}

object JsonEncoders extends JsonEncoders
