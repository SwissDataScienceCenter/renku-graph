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

package ch.datascience.graph.model.entities

import cats.syntax.all._
import ch.datascience.graph.model._
import ch.datascience.graph.model.projects._
import io.circe.DecodingFailure
import io.renku.jsonld.JsonLDDecoder

import scala.util.Try

sealed trait Project {
  val resourceId:   ResourceId
  val path:         Path
  val name:         Name
  val agent:        CliVersion
  val dateCreated:  DateCreated
  val maybeCreator: Option[Person]
  val visibility:   Visibility
  val members:      Set[Person]
  val version:      SchemaVersion
}

final case class ProjectWithoutParent(resourceId:   ResourceId,
                                      path:         Path,
                                      name:         Name,
                                      agent:        CliVersion,
                                      dateCreated:  DateCreated,
                                      maybeCreator: Option[Person],
                                      visibility:   Visibility,
                                      members:      Set[Person],
                                      version:      SchemaVersion
) extends Project

final case class ProjectWithParent(resourceId:       ResourceId,
                                   path:             Path,
                                   name:             Name,
                                   agent:            CliVersion,
                                   dateCreated:      DateCreated,
                                   maybeCreator:     Option[Person],
                                   visibility:       Visibility,
                                   members:          Set[Person],
                                   version:          SchemaVersion,
                                   parentResourceId: ResourceId
) extends Project

object Project {

  import ch.datascience.graph.model.Schemas._
  import io.renku.jsonld.syntax._
  import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder}

  private val entityTypes = EntityTypes.of(prov / "Location", schema / "Project")

  implicit def encoder[P <: Project](implicit gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[P] =
    JsonLDEncoder.instance {
      case project: ProjectWithParent =>
        JsonLD.entity(
          project.resourceId.asEntityId,
          entityTypes,
          schema / "name"             -> project.name.asJsonLD,
          schema / "agent"            -> project.agent.asJsonLD,
          schema / "dateCreated"      -> project.dateCreated.asJsonLD,
          schema / "creator"          -> project.maybeCreator.asJsonLD,
          renku / "projectVisibility" -> project.visibility.asJsonLD,
          schema / "member"           -> project.members.toList.asJsonLD,
          schema / "schemaVersion"    -> project.version.asJsonLD,
          prov / "wasDerivedFrom"     -> project.parentResourceId.asEntityId.asJsonLD
        )
      case project: Project =>
        JsonLD.entity(
          project.resourceId.asEntityId,
          entityTypes,
          schema / "name"             -> project.name.asJsonLD,
          schema / "agent"            -> project.agent.asJsonLD,
          schema / "dateCreated"      -> project.dateCreated.asJsonLD,
          schema / "creator"          -> project.maybeCreator.asJsonLD,
          renku / "projectVisibility" -> project.visibility.asJsonLD,
          schema / "member"           -> project.members.toList.asJsonLD,
          schema / "schemaVersion"    -> project.version.asJsonLD
        )
    }

  implicit def decoder(
      gitLabInfo:          GitLabProjectInfo,
      potentialMembers:    Set[Person]
  )(implicit renkuBaseUrl: RenkuBaseUrl): JsonLDDecoder[Project] = JsonLDDecoder.entity(entityTypes) { cursor =>
    def matchByNameOrUsername(member: ProjectMember): users.Name => Boolean =
      name => name == member.name || name.value == member.username.value

    def byNameUsernameOrAlternateName(member: ProjectMember): Person => Boolean =
      person =>
        matchByNameOrUsername(member)(person.name) ||
          person.alternativeNames.exists(matchByNameOrUsername(member))

    def toPerson(projectMember: ProjectMember): Person = Person(
      users.ResourceId((renkuBaseUrl / "persons" / projectMember.name).show),
      projectMember.name,
      None,
      None,
      projectMember.gitLabId.some
    )

    val maybeCreator: Option[Person] = gitLabInfo.maybeCreator.map(creator =>
      potentialMembers
        .find(byNameUsernameOrAlternateName(creator))
        .map(_.copy(maybeGitLabId = Some(creator.gitLabId)))
        .getOrElse(toPerson(creator))
    )

    val members: Set[Person] = gitLabInfo.members.map(member =>
      potentialMembers
        .find(byNameUsernameOrAlternateName(member))
        .map(_.copy(maybeGitLabId = Some(member.gitLabId)))
        .getOrElse(toPerson(member))
    )

    def checkProjectsMatching(resourceId: ResourceId) = resourceId
      .as[Try, projects.Path]
      .toEither
      .leftMap(_ => DecodingFailure(s"Cannot extract project path from $resourceId", Nil))
      .flatMap {
        case path if path == gitLabInfo.path => Right(())
        case path =>
          Left(DecodingFailure(s"Project '$path' found in JsonLD does not match '${gitLabInfo.path}'", Nil))
      }

    for {
      resourceId    <- cursor.downEntityId.as[ResourceId]
      agent         <- cursor.downField(schema / "agent").as[CliVersion]
      schemaVersion <- cursor.downField(schema / "schemaVersion").as[SchemaVersion]
      _             <- checkProjectsMatching(resourceId)
    } yield gitLabInfo.maybeParentPath match {
      case Some(parentPath) =>
        ProjectWithParent(
          resourceId,
          gitLabInfo.path,
          gitLabInfo.name,
          agent,
          gitLabInfo.dateCreated,
          maybeCreator,
          gitLabInfo.visibility,
          members,
          schemaVersion,
          ResourceId(renkuBaseUrl, parentPath)
        )
      case None =>
        ProjectWithoutParent(resourceId,
                             gitLabInfo.path,
                             gitLabInfo.name,
                             agent,
                             gitLabInfo.dateCreated,
                             maybeCreator,
                             gitLabInfo.visibility,
                             members,
                             schemaVersion
        )
    }
  }

  final case class GitLabProjectInfo(name:            Name,
                                     path:            Path,
                                     dateCreated:     DateCreated,
                                     maybeCreator:    Option[ProjectMember],
                                     members:         Set[ProjectMember],
                                     visibility:      Visibility,
                                     maybeParentPath: Option[Path]
  )
  final case class ProjectMember(name: users.Name, username: users.Username, gitLabId: users.GitLabId)
}
