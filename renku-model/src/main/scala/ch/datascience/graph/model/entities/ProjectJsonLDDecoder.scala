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
import ch.datascience.graph.model.Schemas.schema
import ch.datascience.graph.model._
import ch.datascience.graph.model.entities.Project.{GitLabProjectInfo, ProjectMember, entityTypes}
import ch.datascience.graph.model.projects.{DateCreated, ResourceId}
import io.circe.DecodingFailure
import io.renku.jsonld.{Cursor, JsonLDDecoder}

object ProjectJsonLDDecoder {

  def apply(gitLabInfo: GitLabProjectInfo)(implicit renkuBaseUrl: RenkuBaseUrl): JsonLDDecoder[Project] =
    JsonLDDecoder.entity(entityTypes) { implicit cursor =>
      for {
        agent         <- cursor.downField(schema / "agent").as[CliVersion]
        schemaVersion <- cursor.downField(schema / "schemaVersion").as[SchemaVersion]
        dateCreated   <- cursor.downField(schema / "dateCreated").as[DateCreated]
        earliestDate = List(dateCreated, gitLabInfo.dateCreated).min
        allJsonLdPersons <- findAllPersons(gitLabInfo)
        activities       <- findAllActivities(gitLabInfo)
        datasets         <- findAllDatasets(gitLabInfo)
        resourceId       <- ResourceId(renkuBaseUrl, gitLabInfo.path).asRight
        project <-
          newProject(gitLabInfo, resourceId, earliestDate, agent, schemaVersion, allJsonLdPersons, activities, datasets)
      } yield project
    }

  private def findAllPersons(gitLabInfo: GitLabProjectInfo)(implicit cursor: Cursor) = cursor.top
    .map(_.cursor.as[List[Person]])
    .sequence
    .map(_ getOrElse Nil)
    .map(_.toSet)
    .leftMap(failure =>
      DecodingFailure(
        s"Finding Person entities for project ${gitLabInfo.path} failed: ${failure.getMessage()}",
        Nil
      )
    )

  private def findAllActivities(gitLabInfo: GitLabProjectInfo)(implicit cursor: Cursor) = cursor.top
    .map(_.cursor.as[List[Activity]])
    .sequence
    .map(_ getOrElse Nil)
    .map(_.sortBy(_.startTime))
    .leftMap(failure =>
      DecodingFailure(
        s"Finding Activity entities for project ${gitLabInfo.path} failed: ${failure.getMessage()}",
        Nil
      )
    )

  private def findAllDatasets(gitLabInfo: GitLabProjectInfo)(implicit cursor: Cursor) = cursor.top
    .map(_.cursor.as[List[Dataset[Dataset.Provenance]]])
    .sequence
    .map(_ getOrElse Nil)
    .leftMap(failure =>
      DecodingFailure(
        s"Finding Dataset entities for project ${gitLabInfo.path} failed: ${failure.getMessage()}",
        Nil
      )
    )

  private def matchByNameOrUsername(member: ProjectMember): users.Name => Boolean =
    name => name == member.name || name.value == member.username.value

  private def byNameUsernameOrAlternateName(member: ProjectMember): Person => Boolean =
    person =>
      matchByNameOrUsername(member)(person.name) ||
        person.alternativeNames.exists(matchByNameOrUsername(member))

  private def toPerson(projectMember: ProjectMember)(implicit renkuBaseUrl: RenkuBaseUrl): Person = Person(
    users.ResourceId((renkuBaseUrl / "persons" / projectMember.name).show),
    projectMember.name,
    None,
    None,
    projectMember.gitLabId.some
  )

  private def newProject(gitLabInfo:       GitLabProjectInfo,
                         resourceId:       ResourceId,
                         dateCreated:      DateCreated,
                         agent:            CliVersion,
                         schemaVersion:    SchemaVersion,
                         allJsonLdPersons: Set[Person],
                         activities:       List[Activity],
                         datasets:         List[Dataset[Dataset.Provenance]]
  )(implicit renkuBaseUrl:                 RenkuBaseUrl) = {
    gitLabInfo.maybeParentPath match {
      case Some(parentPath) =>
        ProjectWithParent
          .from(
            resourceId,
            gitLabInfo.path,
            gitLabInfo.name,
            agent,
            dateCreated,
            maybeCreator(allJsonLdPersons)(gitLabInfo),
            gitLabInfo.visibility,
            members(allJsonLdPersons)(gitLabInfo),
            schemaVersion,
            activities,
            datasets,
            parentResourceId = ResourceId(renkuBaseUrl, parentPath)
          )
      case None =>
        ProjectWithoutParent
          .from(
            resourceId,
            gitLabInfo.path,
            gitLabInfo.name,
            agent,
            dateCreated,
            maybeCreator(allJsonLdPersons)(gitLabInfo),
            gitLabInfo.visibility,
            members(allJsonLdPersons)(gitLabInfo),
            schemaVersion,
            activities,
            datasets
          )
    }
  }.toEither
    .leftMap(errors => DecodingFailure(errors.intercalate("; "), Nil))

  private def maybeCreator(
      allJsonLdPersons: Set[Person]
  )(gitLabInfo:         GitLabProjectInfo)(implicit renkuBaseUrl: RenkuBaseUrl): Option[Person] =
    gitLabInfo.maybeCreator.map { creator =>
      allJsonLdPersons
        .find(byNameUsernameOrAlternateName(creator))
        .map(_.copy(maybeGitLabId = Some(creator.gitLabId)))
        .getOrElse(toPerson(creator))
    }

  private def members(
      allJsonLdPersons: Set[Person]
  )(gitLabInfo:         GitLabProjectInfo)(implicit renkuBaseUrl: RenkuBaseUrl): Set[Person] =
    gitLabInfo.members.map(member =>
      allJsonLdPersons
        .find(byNameUsernameOrAlternateName(member))
        .map(_.copy(maybeGitLabId = Some(member.gitLabId)))
        .getOrElse(toPerson(member))
    )
}
