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
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.Schemas.{prov, schema}
import ch.datascience.graph.model._
import ch.datascience.graph.model.entities.Project.ProjectMember
import ch.datascience.graph.model.testentities._
import io.circe.DecodingFailure
import io.renku.jsonld.JsonLDDecoder._
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityTypes, JsonLD}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Random

class ProjectSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "Project.decode" should {

    "turn JsonLD Project entity without parent into the Project object" in {
      forAll(gitLabProjectInfos.map(_.copy(maybeParentPath = None)), cliVersions, projectSchemaVersions) {
        (projectInfo, cliVersion, schemaVersion) =>
          val resourceId    = projects.ResourceId(renkuBaseUrl, projectInfo.path)
          val projectEntity = projectJsonLD(resourceId, cliVersion, schemaVersion)
          val maybeCreator  = projectInfo.maybeCreator.map(_.toPayloadPerson)
          val members       = projectInfo.members.map(_.toPayloadPerson)
          val jsonLd        = projectEntity.flatten.fold(throw _, identity)

          jsonLd.cursor
            .as[List[entities.Project]](
              decodeList(entities.Project.decoder(projectInfo, (maybeCreator ++ members).toSet))
            ) shouldBe List(
            entities.ProjectWithoutParent(
              resourceId,
              projectInfo.path,
              projectInfo.name,
              cliVersion,
              projectInfo.dateCreated,
              maybeCreator.map(copyGitLabId(from = projectInfo.maybeCreator)),
              projectInfo.visibility,
              members.map(copyGitLabId(fromMatching = projectInfo.members)),
              schemaVersion
            )
          ).asRight
      }
    }

    "turn JsonLD Project entity with parent into the Project object" in {
      forAll(gitLabProjectInfos.map(_.copy(maybeParentPath = projectPaths.generateSome)),
             cliVersions,
             projectSchemaVersions
      ) { (projectInfo, cliVersion, schemaVersion) =>
        val resourceId    = projects.ResourceId(renkuBaseUrl, projectInfo.path)
        val projectEntity = projectJsonLD(resourceId, cliVersion, schemaVersion)
        val maybeCreator  = projectInfo.maybeCreator.map(_.toPayloadPerson)
        val members       = projectInfo.members.map(_.toPayloadPerson)
        val jsonLd        = projectEntity.flatten.fold(throw _, identity)

        jsonLd.cursor
          .as[List[entities.Project]](
            decodeList(entities.Project.decoder(projectInfo, (maybeCreator ++ members).toSet))
          ) shouldBe List(
          entities.ProjectWithParent(
            resourceId,
            projectInfo.path,
            projectInfo.name,
            cliVersion,
            projectInfo.dateCreated,
            maybeCreator.map(copyGitLabId(from = projectInfo.maybeCreator)),
            projectInfo.visibility,
            members.map(copyGitLabId(fromMatching = projectInfo.members)),
            schemaVersion,
            projects.ResourceId(renkuBaseUrl, projectInfo.maybeParentPath.getOrElse(fail("No parent project")))
          )
        ).asRight
      }
    }

    "turn JsonLD Project entity into the Project object " +
      "- case when not all GitLab persons exist in the potential members" in {
        forAll(gitLabProjectInfos.map(_.copy(maybeParentPath = None)), cliVersions, projectSchemaVersions) {
          (projectInfo, cliVersion, schemaVersion) =>
            val resourceId    = projects.ResourceId(renkuBaseUrl, projectInfo.path)
            val projectEntity = projectJsonLD(resourceId, cliVersion, schemaVersion)
            val jsonLd        = projectEntity.flatten.fold(throw _, identity)

            val potentialMembers = {
              val allMembers = Random.shuffle((projectInfo.maybeCreator ++ projectInfo.members).toList)
              allMembers.take(allMembers.size / 2).map(_.toPayloadPerson)
            }

            jsonLd.cursor
              .as[List[entities.Project]](
                decodeList(entities.Project.decoder(projectInfo, potentialMembers.toSet))
              ) shouldBe List(
              entities.ProjectWithoutParent(
                resourceId,
                projectInfo.path,
                projectInfo.name,
                cliVersion,
                projectInfo.dateCreated,
                projectInfo.maybeCreator.map(toPerson(tryMatchFrom = potentialMembers)),
                projectInfo.visibility,
                projectInfo.members.map(toPerson(tryMatchFrom = potentialMembers)),
                schemaVersion
              )
            ).asRight
        }
      }

    "return a DecodingFailure when the projectInfo is for a different project" in {
      val projectInfo       = gitLabProjectInfos.map(_.copy(maybeParentPath = None)).generateOne
      val projectResourceId = projectResourceIds.generateOne
      val projectEntity =
        projectJsonLD(projectResourceId, cliVersions.generateOne, projectSchemaVersions.generateOne)

      val Left(error) = projectEntity.cursor.as[entities.Project](entities.Project.decoder(projectInfo, Set.empty))
      error shouldBe a[DecodingFailure]
      error
        .getMessage() shouldBe s"Project '${projectResourceId
        .toUnsafe[projects.Path]}' found in JsonLD does not match '${projectInfo.path}'"
    }
  }

  private def projectJsonLD(resourceId: projects.ResourceId, cliVersion: CliVersion, schemaVersion: SchemaVersion) =
    JsonLD.entity(
      resourceId.asEntityId,
      EntityTypes.of(prov / "Location", schema / "Project"),
      schema / "agent"         -> cliVersion.asJsonLD,
      schema / "schemaVersion" -> schemaVersion.asJsonLD
    )

  private def toPerson(tryMatchFrom: List[entities.Person])(gitLabPerson: ProjectMember) =
    tryMatchFrom
      .find(byNameOrUsername(_)(gitLabPerson))
      .map(copyGitLabId(from = gitLabPerson.some))
      .getOrElse(gitLabPerson.toPerson)

  private def copyGitLabId(from: Option[ProjectMember])(person: entities.Person): entities.Person =
    person.copy(maybeGitLabId = from.map(_.gitLabId))

  private def copyGitLabId(fromMatching: Set[ProjectMember])(person: entities.Person): entities.Person =
    person.copy(
      maybeGitLabId = fromMatching
        .find(byNameOrUsername(person))
        .map(_.gitLabId)
    )

  private def byNameOrUsername(person: entities.Person): ProjectMember => Boolean =
    member => member.hasNameOrUsername(person.name) || person.alternativeNames.exists(member.hasNameOrUsername)

  private implicit class ProjectMemberOps(gitLabPerson: ProjectMember) {

    def hasNameOrUsername(name: users.Name): Boolean =
      gitLabPerson.name == name || gitLabPerson.username.value == name.value

    def toPayloadPerson: entities.Person =
      if (Random.nextBoolean())
        personEntities.generateOne
          .copy(
            name = nameFromUsernameOrName(gitLabPerson),
            maybeGitLabId = None
          )
          .to[entities.Person]
      else
        personEntities.generateOne
          .copy(
            name = userNames.generateOne,
            maybeGitLabId = None
          )
          .to[entities.Person]
          .copy(alternativeNames = List(nameFromUsernameOrName(gitLabPerson)))

    private def nameFromUsernameOrName(gitLabPerson: ProjectMember) =
      if (Random.nextBoolean()) gitLabPerson.name
      else users.Name(gitLabPerson.username.value)

    def toPerson: entities.Person = entities
      .Person(
        users.ResourceId((renkuBaseUrl / "persons" / gitLabPerson.name).show),
        name = gitLabPerson.name,
        maybeEmail = None,
        maybeAffiliation = None,
        maybeGitLabId = gitLabPerson.gitLabId.some
      )
  }
}
