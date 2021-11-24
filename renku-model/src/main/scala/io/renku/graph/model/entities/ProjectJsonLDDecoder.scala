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

package io.renku.graph.model.entities

import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.graph.model.Schemas.schema
import io.renku.graph.model._
import io.renku.graph.model.entities.Project.ProjectMember.{ProjectMemberNoEmail, ProjectMemberWithEmail}
import io.renku.graph.model.entities.Project.{GitLabProjectInfo, ProjectMember, entityTypes}
import io.renku.graph.model.projects.{DateCreated, Description, ResourceId}
import io.renku.graph.model.views.StringTinyTypeJsonLDDecoders.decodeBlankStringToNone
import io.renku.jsonld.{Cursor, JsonLDDecoder}

object ProjectJsonLDDecoder {

  def apply(gitLabInfo: GitLabProjectInfo)(implicit renkuBaseUrl: RenkuBaseUrl): JsonLDDecoder[Project] =
    JsonLDDecoder.entity(entityTypes) { implicit cursor =>
      for {
        agent         <- cursor.downField(schema / "agent").as[CliVersion]
        schemaVersion <- cursor.downField(schema / "schemaVersion").as[SchemaVersion]
        dateCreated   <- cursor.downField(schema / "dateCreated").as[DateCreated]
        maybeDescription <-
          cursor.downField(schema / "description").as[Option[Description]].map(_ orElse gitLabInfo.maybeDescription)
        allPersons <- findAllPersons(gitLabInfo)
        activities <- findAllActivities(gitLabInfo)
        datasets   <- findAllDatasets(gitLabInfo)
        resourceId <- ResourceId(gitLabInfo.path).asRight
        earliestDate = List(dateCreated, gitLabInfo.dateCreated).min
        project <-
          newProject(gitLabInfo,
                     resourceId,
                     earliestDate,
                     maybeDescription,
                     agent,
                     schemaVersion,
                     allPersons,
                     activities,
                     datasets
          )
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

  private def newProject(gitLabInfo:       GitLabProjectInfo,
                         resourceId:       ResourceId,
                         dateCreated:      DateCreated,
                         maybeDescription: Option[Description],
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
            maybeDescription,
            agent,
            dateCreated,
            maybeCreator(allJsonLdPersons)(gitLabInfo),
            gitLabInfo.visibility,
            members(allJsonLdPersons)(gitLabInfo),
            schemaVersion,
            activities,
            datasets,
            parentResourceId = ResourceId(parentPath)
          )
      case None =>
        ProjectWithoutParent
          .from(
            resourceId,
            gitLabInfo.path,
            gitLabInfo.name,
            maybeDescription,
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
        .find(byEmailOrUsername(creator))
        .map(merge(creator))
        .getOrElse(toPerson(creator))
    }

  private def members(
      allJsonLdPersons: Set[Person]
  )(gitLabInfo:         GitLabProjectInfo)(implicit renkuBaseUrl: RenkuBaseUrl): Set[Person] =
    gitLabInfo.members.map(member =>
      allJsonLdPersons
        .find(byEmailOrUsername(member))
        .map(merge(member))
        .getOrElse(toPerson(member))
    )

  private lazy val byEmailOrUsername: ProjectMember => Person => Boolean = {
    case member: ProjectMemberWithEmail =>
      person =>
        person.maybeEmail match {
          case Some(personEmail) => personEmail == member.email
          case None              => person.name.value == member.username.value
        }
    case member: ProjectMemberNoEmail => person => person.name.value == member.username.value
  }

  private def merge(member: Project.ProjectMember)(implicit renkuBaseUrl: RenkuBaseUrl): Person => Person =
    member match {
      case ProjectMemberWithEmail(name, _, gitLabId, email) =>
        person =>
          person.copy(resourceId = users.ResourceId(gitLabId),
                      name = name,
                      maybeEmail = email.some,
                      maybeGitLabId = gitLabId.some
          )
      case ProjectMemberNoEmail(name, _, gitLabId) =>
        person => person.copy(name = name, maybeGitLabId = gitLabId.some)
    }

  private def toPerson(projectMember: ProjectMember)(implicit renkuBaseUrl: RenkuBaseUrl): Person =
    projectMember match {
      case ProjectMemberNoEmail(name, _, gitLabId) =>
        Person(users.ResourceId(gitLabId), name, maybeGitLabId = gitLabId.some)
      case ProjectMemberWithEmail(name, _, gitLabId, email) =>
        Person(users.ResourceId(gitLabId), name, maybeGitLabId = gitLabId.some, maybeEmail = email.some)
    }
}
