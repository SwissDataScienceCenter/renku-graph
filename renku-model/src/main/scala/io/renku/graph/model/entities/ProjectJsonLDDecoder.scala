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

package io.renku.graph.model.entities

import cats.data.Validated
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.graph.model.Schemas.{renku, schema}
import io.renku.graph.model._
import io.renku.graph.model.entities.Project.ProjectMember.{ProjectMemberNoEmail, ProjectMemberWithEmail}
import io.renku.graph.model.entities.Project.{GitLabProjectInfo, ProjectMember, entityTypes}
import io.renku.graph.model.projects.{DateCreated, Description, Keyword, ResourceId}
import io.renku.graph.model.views.StringTinyTypeJsonLDDecoders.decodeBlankStringToNone
import io.renku.jsonld.{Cursor, JsonLDDecoder}

object ProjectJsonLDDecoder {

  def apply(gitLabInfo: GitLabProjectInfo)(implicit renkuBaseUrl: RenkuBaseUrl): JsonLDDecoder[Project] =
    JsonLDDecoder.entity(entityTypes) { implicit cursor =>
      val maybeDescriptionR = cursor
        .downField(schema / "description")
        .as[Option[Description]]
        .map(_ orElse gitLabInfo.maybeDescription)
      val keywordsR = cursor.downField(schema / "keywords").as[List[Option[Keyword]]].map(_.flatten.toSet).map {
        case kwrds if kwrds.isEmpty => gitLabInfo.keywords
        case kwrds                  => kwrds
      }

      for {
        maybeAgent       <- cursor.downField(schema / "agent").as[Option[CliVersion]]
        maybeVersion     <- cursor.downField(schema / "schemaVersion").as[Option[SchemaVersion]]
        maybeDateCreated <- cursor.downField(schema / "dateCreated").as[Option[DateCreated]]
        maybeDescription <- maybeDescriptionR
        keywords         <- keywordsR
        allPersons       <- findAllPersons(gitLabInfo)
        activities       <- cursor.downField(renku / "hasActivity").as[List[Activity]].map(_.sortBy(_.startTime))
        datasets         <- cursor.downField(renku / "hasDataset").as[List[Dataset[Dataset.Provenance]]]
        resourceId       <- ResourceId(gitLabInfo.path).asRight
        project <- newProject(
                     gitLabInfo,
                     resourceId,
                     dateCreated = (gitLabInfo.dateCreated :: maybeDateCreated.toList).min,
                     maybeDescription,
                     maybeAgent,
                     keywords,
                     maybeVersion,
                     allPersons,
                     activities,
                     datasets
                   )
      } yield project
    }

  private def findAllPersons(gitLabInfo: GitLabProjectInfo)(implicit cursor: Cursor, renkuBaseUrl: RenkuBaseUrl) =
    cursor.focusTop
      .as[List[Person]]
      .map(_.toSet)
      .leftMap(failure =>
        DecodingFailure(s"Finding Person entities for project ${gitLabInfo.path} failed: ${failure.getMessage()}", Nil)
      )

  private def newProject(gitLabInfo:       GitLabProjectInfo,
                         resourceId:       ResourceId,
                         dateCreated:      DateCreated,
                         maybeDescription: Option[Description],
                         maybeAgent:       Option[CliVersion],
                         keywords:         Set[Keyword],
                         maybeVersion:     Option[SchemaVersion],
                         allJsonLdPersons: Set[Person],
                         activities:       List[Activity],
                         datasets:         List[Dataset[Dataset.Provenance]]
  )(implicit renkuBaseUrl:                 RenkuBaseUrl): Either[DecodingFailure, Project] = {
    (maybeAgent, maybeVersion, gitLabInfo.maybeParentPath) match {
      case (Some(agent), Some(version), Some(parentPath)) =>
        RenkuProject.WithParent
          .from(
            resourceId,
            gitLabInfo.path,
            gitLabInfo.name,
            maybeDescription,
            agent,
            dateCreated,
            maybeCreator(allJsonLdPersons)(gitLabInfo),
            gitLabInfo.visibility,
            keywords,
            members(allJsonLdPersons)(gitLabInfo),
            version,
            activities,
            datasets,
            parentResourceId = ResourceId(parentPath)
          )
          .widen[Project]
      case (Some(agent), Some(version), None) =>
        RenkuProject.WithoutParent
          .from(
            resourceId,
            gitLabInfo.path,
            gitLabInfo.name,
            maybeDescription,
            agent,
            dateCreated,
            maybeCreator(allJsonLdPersons)(gitLabInfo),
            gitLabInfo.visibility,
            keywords,
            members(allJsonLdPersons)(gitLabInfo),
            version,
            activities,
            datasets
          )
          .widen[Project]
      case (None, None, Some(parentPath)) =>
        NonRenkuProject
          .WithParent(
            resourceId,
            gitLabInfo.path,
            gitLabInfo.name,
            maybeDescription,
            dateCreated,
            maybeCreator(allJsonLdPersons)(gitLabInfo),
            gitLabInfo.visibility,
            keywords,
            members(allJsonLdPersons)(gitLabInfo),
            parentResourceId = ResourceId(parentPath)
          )
          .validNel[String]
          .widen[Project]
      case (None, None, None) =>
        NonRenkuProject
          .WithoutParent(
            resourceId,
            gitLabInfo.path,
            gitLabInfo.name,
            maybeDescription,
            dateCreated,
            maybeCreator(allJsonLdPersons)(gitLabInfo),
            gitLabInfo.visibility,
            keywords,
            members(allJsonLdPersons)(gitLabInfo)
          )
          .validNel[String]
          .widen[Project]
      case (maybeAgent, maybeVersion, maybeParent) =>
        Validated.invalidNel[String, Project](
          s"Invalid project data " +
            s"agent: $maybeAgent, " +
            s"schemaVersion: $maybeVersion, " +
            s"parent: $maybeParent"
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
        _.add(gitLabId).copy(name = name, maybeEmail = email.some)
      case ProjectMemberNoEmail(name, _, gitLabId) =>
        _.add(gitLabId).copy(name = name)
    }

  private def toPerson(projectMember: ProjectMember)(implicit renkuBaseUrl: RenkuBaseUrl): Person =
    projectMember match {
      case ProjectMemberNoEmail(name, _, gitLabId) =>
        Person.WithGitLabId(persons.ResourceId(gitLabId),
                            gitLabId,
                            name,
                            maybeEmail = None,
                            maybeOrcidId = None,
                            maybeAffiliation = None
        )
      case ProjectMemberWithEmail(name, _, gitLabId, email) =>
        Person.WithGitLabId(persons.ResourceId(gitLabId),
                            gitLabId,
                            name,
                            email.some,
                            maybeOrcidId = None,
                            maybeAffiliation = None
        )
    }
}
