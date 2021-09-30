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
import ch.datascience.graph.model.Schemas.{prov, renku, schema}
import ch.datascience.graph.model._
import ch.datascience.graph.model.entities.Project.ProjectMember
import ch.datascience.graph.model.projects.DateCreated
import ch.datascience.graph.model.testentities._
import io.circe.DecodingFailure
import io.renku.jsonld.JsonLDDecoder._
import io.renku.jsonld.JsonLDEncoder.encodeOption
import io.renku.jsonld._
import io.renku.jsonld.generators.JsonLDGenerators.entityIds
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Random

class ProjectSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "from" should {

    "sync project's person entities across activities and datasets" in {
      val onlyGLMember  = personEntities(withGitLabId, withoutEmail).generateOne
      val gLAndKGMember = personEntities(withGitLabId, withEmail).generateOne
      val creator       = personEntities(withGitLabId, withEmail).generateOne

      Set(
        projectEntities(anyVisibility)
          .modify(_.copy(maybeCreator = Some(creator), members = Set(onlyGLMember, gLAndKGMember))),
        projectWithParentEntities(anyVisibility)
          .modify(_.copy(maybeCreator = Some(creator), members = Set(onlyGLMember, gLAndKGMember)))
      ) foreach { projectGen =>
        val project = projectGen
          .withActivities(activityEntities(planEntities()).modify(_.copy(author = creator.copy(maybeGitLabId = None))))
          .withDatasets(
            datasetEntities(ofAnyProvenance).modify(
              provenanceLens.modify(creatorsLens.modify(_ => Set(gLAndKGMember.copy(maybeGitLabId = None))))
            )
          )
          .generateOne
          .to[entities.Project]

        project.maybeCreator             shouldBe creator.to[entities.Person].some
        project.members                  shouldBe Set(onlyGLMember, gLAndKGMember).map(_.to[entities.Person])
        project.activities.map(_.author) shouldBe List(creator.to[entities.Person])
        project.datasets.toSet.flatMap((ds: entities.Dataset[entities.Dataset.Provenance]) =>
          ds.provenance.creators
        ) shouldBe Set(gLAndKGMember.to[entities.Person])
      }
    }
  }

  "decode" should {

    "turn JsonLD Project entity without parent into the Project object" in {
      forAll(gitLabProjectInfos.map(_.copy(maybeParentPath = None)), cliVersions, projectSchemaVersions) {
        (projectInfo, cliVersion, schemaVersion) =>
          val resourceId   = projects.ResourceId(renkuBaseUrl, projectInfo.path)
          val activities   = activityEntities(planEntities()).generateList(projectInfo.dateCreated).sortBy(_.startTime)
          val datasets     = datasetEntities(ofAnyProvenance).generateList(projectInfo.dateCreated)
          val maybeCreator = projectInfo.maybeCreator.map(_.toPayloadPerson)
          val members      = projectInfo.members.map(_.toPayloadPerson)
          val jsonLD =
            cliLikeJsonLD(resourceId,
                          cliVersion,
                          schemaVersion,
                          projectInfo.dateCreated,
                          activities,
                          datasets,
                          (maybeCreator ++ members).toSet
            )

          jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo))) shouldBe List(
            entities.ProjectWithoutParent(
              resourceId,
              projectInfo.path,
              projectInfo.name,
              cliVersion,
              projectInfo.dateCreated,
              maybeCreator.map(copyGitLabId(from = projectInfo.maybeCreator)),
              projectInfo.visibility,
              members.map(copyGitLabId(fromMatching = projectInfo.members)),
              schemaVersion,
              activities.map(_.to[entities.Activity]),
              datasets.map(_.to[entities.Dataset[entities.Dataset.Provenance]])
            )
          ).asRight
      }
    }

    "turn JsonLD Project entity with parent into the Project object" in {
      forAll(gitLabProjectInfos.map(_.copy(maybeParentPath = projectPaths.generateSome)),
             cliVersions,
             projectSchemaVersions
      ) { (projectInfo, cliVersion, schemaVersion) =>
        val resourceId   = projects.ResourceId(renkuBaseUrl, projectInfo.path)
        val activities   = activityEntities(planEntities()).generateList(projectInfo.dateCreated).sortBy(_.startTime)
        val datasets     = datasetEntities(ofAnyProvenance).generateList(projectInfo.dateCreated)
        val maybeCreator = projectInfo.maybeCreator.map(_.toPayloadPerson)
        val members      = projectInfo.members.map(_.toPayloadPerson)
        val jsonLD =
          cliLikeJsonLD(resourceId,
                        cliVersion,
                        schemaVersion,
                        projectInfo.dateCreated,
                        activities,
                        datasets,
                        (maybeCreator ++ members).toSet
          )

        jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo))) shouldBe List(
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
            activities.map(_.to[entities.Activity]),
            datasets.map(_.to[entities.Dataset[entities.Dataset.Provenance]]),
            projects.ResourceId(renkuBaseUrl, projectInfo.maybeParentPath.getOrElse(fail("No parent project")))
          )
        ).asRight
      }
    }

    "turn JsonLD Project entity into the Project object " +
      "- case when not all GitLab persons exist in the potential members" in {
        forAll(gitLabProjectInfos.map(_.copy(maybeParentPath = None)), cliVersions, projectSchemaVersions) {
          (projectInfo, cliVersion, schemaVersion) =>
            val resourceId = projects.ResourceId(renkuBaseUrl, projectInfo.path)
            val potentialMembers = {
              val allMembers = Random.shuffle((projectInfo.maybeCreator ++ projectInfo.members).toList)
              allMembers.take(allMembers.size / 2).map(_.toPayloadPerson)
            }
            val jsonLD = cliLikeJsonLD(resourceId,
                                       cliVersion,
                                       schemaVersion,
                                       projectInfo.dateCreated,
                                       activities = Nil,
                                       datasets = Nil,
                                       potentialMembers.toSet
            )

            jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo))) shouldBe List(
              entities.ProjectWithoutParent(
                resourceId,
                projectInfo.path,
                projectInfo.name,
                cliVersion,
                projectInfo.dateCreated,
                projectInfo.maybeCreator.map(toPerson(tryMatchFrom = potentialMembers)),
                projectInfo.visibility,
                projectInfo.members.map(toPerson(tryMatchFrom = potentialMembers)),
                schemaVersion,
                activities = Nil,
                datasets = Nil
              )
            ).asRight
        }
      }

    "return a DecodingFailure when there's a Person entity that cannot be decoded" in {
      val projectInfo = gitLabProjectInfos.map(_.copy(maybeParentPath = None)).generateOne
      val resourceId  = projects.ResourceId(renkuBaseUrl, projectInfo.path)
      val jsonLD = cliLikeJsonLD(resourceId,
                                 cliVersions.generateOne,
                                 projectSchemaVersions.generateOne,
                                 projectInfo.dateCreated,
                                 activities = Nil,
                                 datasets = Nil,
                                 persons = Set.empty
      )

      val Left(error) = JsonLD
        .arr(jsonLD,
             JsonLD.entity(userResourceIds.generateOne.asEntityId,
                           entities.Person.entityTypes,
                           Map.empty[Property, JsonLD]
             )
        )
        .flatten
        .fold(throw _, identity)
        .cursor
        .as(decodeList(entities.Project.decoder(projectInfo)))

      error            shouldBe a[DecodingFailure]
      error.getMessage() should startWith(s"Finding Person entities for project ${projectInfo.path} failed: ")
    }

    "return a DecodingFailure when there's an Activity entity that cannot be decoded" in {
      val projectInfo = gitLabProjectInfos.map(_.copy(maybeParentPath = None)).generateOne
      val resourceId  = projects.ResourceId(renkuBaseUrl, projectInfo.path)
      val jsonLD = cliLikeJsonLD(resourceId,
                                 cliVersions.generateOne,
                                 projectSchemaVersions.generateOne,
                                 projectInfo.dateCreated,
                                 activities = Nil,
                                 datasets = Nil,
                                 persons = Set.empty
      )

      val Left(error) = JsonLD
        .arr(jsonLD, JsonLD.entity(entityIds.generateOne, entities.Activity.entityTypes, Map.empty[Property, JsonLD]))
        .flatten
        .fold(throw _, identity)
        .cursor
        .as(decodeList(entities.Project.decoder(projectInfo)))

      error            shouldBe a[DecodingFailure]
      error.getMessage() should startWith(s"Finding Activity entities for project ${projectInfo.path} failed: ")
    }

    "return a DecodingFailure when there's a Dataset entity that cannot be decoded" in {
      val projectInfo = gitLabProjectInfos.map(_.copy(maybeParentPath = None)).generateOne
      val resourceId  = projects.ResourceId(renkuBaseUrl, projectInfo.path)
      val jsonLD = cliLikeJsonLD(resourceId,
                                 cliVersions.generateOne,
                                 projectSchemaVersions.generateOne,
                                 projectInfo.dateCreated,
                                 activities = Nil,
                                 datasets = Nil,
                                 persons = Set.empty
      )

      val Left(error) = JsonLD
        .arr(jsonLD, JsonLD.entity(entityIds.generateOne, entities.Dataset.entityTypes, Map.empty[Property, JsonLD]))
        .flatten
        .fold(throw _, identity)
        .cursor
        .as(decodeList(entities.Project.decoder(projectInfo)))

      error            shouldBe a[DecodingFailure]
      error.getMessage() should startWith(s"Finding Dataset entities for project ${projectInfo.path} failed: ")
    }

    "return a DecodingFailure when there's an Activity entity created before project creation" in {
      val projectInfo = gitLabProjectInfos.map(_.copy(maybeParentPath = None)).generateOne
      val resourceId  = projects.ResourceId(renkuBaseUrl, projectInfo.path)
      val activity    = activityEntities(planEntities()).withDateBefore(projectInfo.dateCreated).generateOne
      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersions.generateOne,
        projectSchemaVersions.generateOne,
        projectInfo.dateCreated,
        activities = List(activity),
        datasets = Nil,
        persons = Set.empty
      )

      val Left(error) = jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo)))

      error shouldBe a[DecodingFailure]
      error.getMessage() shouldBe s"Activity ${activity.to[entities.Activity].resourceId} " +
        s"startTime ${activity.startTime} is older than project ${projectInfo.dateCreated}"
    }

    "return a DecodingFailure when there's an internal Dataset entity created before project without parent" in {
      val projectInfo = gitLabProjectInfos.map(_.copy(maybeParentPath = None)).generateOne
      val resourceId  = projects.ResourceId(renkuBaseUrl, projectInfo.path)
      val dataset     = datasetEntities(provenanceInternal).withDateBefore(projectInfo.dateCreated).generateOne
      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersions.generateOne,
        projectSchemaVersions.generateOne,
        projectInfo.dateCreated,
        activities = Nil,
        datasets = List(dataset),
        persons = Set.empty
      )

      val Left(error) = jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo)))

      error shouldBe a[DecodingFailure]
      error
        .getMessage() shouldBe s"Dataset ${dataset.identification.identifier} " +
        s"date ${dataset.provenance.date} is older than project ${projectInfo.dateCreated}"
    }

    "decode project when there's an internal or modified Dataset entity created before project with parent" in {
      val parentPath    = projectPaths.generateOne
      val projectInfo   = gitLabProjectInfos.map(_.copy(maybeParentPath = parentPath.some)).generateOne
      val cliVersion    = cliVersions.generateOne
      val schemaVersion = projectSchemaVersions.generateOne
      val resourceId    = projects.ResourceId(renkuBaseUrl, projectInfo.path)
      val maybeCreator  = projectInfo.maybeCreator.map(_.toPayloadPerson)
      val members       = projectInfo.members.map(_.toPayloadPerson)
      val dataset1      = datasetEntities(provenanceInternal).withDateBefore(projectInfo.dateCreated).generateOne
      val dataset2      = datasetEntities(provenanceModified).withDateBefore(projectInfo.dateCreated).generateOne
      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersion,
        schemaVersion,
        projectInfo.dateCreated,
        activities = Nil,
        datasets = List(dataset1, dataset2),
        persons = (maybeCreator ++ members).toSet
      )

      jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo))) shouldBe List(
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
          Nil,
          List(dataset1, dataset2).map(_.to[entities.Dataset[entities.Dataset.Provenance]]),
          projects.ResourceId(renkuBaseUrl, parentPath)
        )
      ).asRight
    }

    "return a DecodingFailure when there's a modified Dataset entity created before project without parent" in {
      val projectInfo = gitLabProjectInfos.map(_.copy(maybeParentPath = None)).generateOne
      val resourceId  = projects.ResourceId(renkuBaseUrl, projectInfo.path)
      val dataset     = datasetEntities(provenanceModified).withDateBefore(projectInfo.dateCreated).generateOne
      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersions.generateOne,
        projectSchemaVersions.generateOne,
        projectInfo.dateCreated,
        activities = Nil,
        datasets = List(dataset),
        persons = Set.empty
      )

      val Left(error) = jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo)))

      error shouldBe a[DecodingFailure]
      error
        .getMessage() shouldBe s"Dataset ${dataset.identification.identifier} " +
        s"date ${dataset.provenance.date} is older than project ${projectInfo.dateCreated}"
    }

    "decode project when there's a Dataset (not internal or modified) created before project creation" in {
      val projectInfo   = gitLabProjectInfos.map(_.copy(maybeParentPath = None)).generateOne
      val cliVersion    = cliVersions.generateOne
      val schemaVersion = projectSchemaVersions.generateOne
      val resourceId    = projects.ResourceId(renkuBaseUrl, projectInfo.path)
      val maybeCreator  = projectInfo.maybeCreator.map(_.toPayloadPerson)
      val members       = projectInfo.members.map(_.toPayloadPerson)
      val dataset1      = datasetEntities(provenanceImportedExternal).withDateBefore(projectInfo.dateCreated).generateOne
      val dataset2 =
        datasetEntities(provenanceImportedInternalAncestorExternal).withDateBefore(projectInfo.dateCreated).generateOne
      val dataset3 =
        datasetEntities(provenanceImportedInternalAncestorInternal).withDateBefore(projectInfo.dateCreated).generateOne
      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersion,
        schemaVersion,
        projectInfo.dateCreated,
        activities = Nil,
        datasets = List(dataset1, dataset2, dataset3),
        persons = (maybeCreator ++ members).toSet
      )

      jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo))) shouldBe List(
        entities.ProjectWithoutParent(
          resourceId,
          projectInfo.path,
          projectInfo.name,
          cliVersion,
          projectInfo.dateCreated,
          maybeCreator.map(copyGitLabId(from = projectInfo.maybeCreator)),
          projectInfo.visibility,
          members.map(copyGitLabId(fromMatching = projectInfo.members)),
          schemaVersion,
          Nil,
          List(dataset1, dataset2, dataset3).map(_.to[entities.Dataset[entities.Dataset.Provenance]])
        )
      ).asRight
    }

    "decode project should pick the earliest dateCreated between gitlabProjectInfo and the CLI dateCreated " in {
      val gitlabDate    = projectCreatedDates().generateOne
      val cliDate       = projectCreatedDates().generateOne
      val earliestDate  = List(gitlabDate, cliDate).min
      val projectInfo   = gitLabProjectInfos.map(_.copy(maybeParentPath = None, dateCreated = gitlabDate)).generateOne
      val cliVersion    = cliVersions.generateOne
      val schemaVersion = projectSchemaVersions.generateOne
      val resourceId    = projects.ResourceId(renkuBaseUrl, projectInfo.path)
      val maybeCreator  = projectInfo.maybeCreator.map(_.toPayloadPerson)
      val members       = projectInfo.members.map(_.toPayloadPerson)

      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersion,
        schemaVersion,
        cliDate,
        activities = Nil,
        datasets = Nil,
        persons = (maybeCreator ++ members).toSet
      )

      jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo))) shouldBe List(
        entities.ProjectWithoutParent(
          resourceId,
          projectInfo.path,
          projectInfo.name,
          cliVersion,
          earliestDate,
          maybeCreator.map(copyGitLabId(from = projectInfo.maybeCreator)),
          projectInfo.visibility,
          members.map(copyGitLabId(fromMatching = projectInfo.members)),
          schemaVersion,
          Nil,
          Nil
        )
      ).asRight
    }
  }

  "encode" should {

    "produce JsonLD with all the relevant properties" in {
      forAll(projectEntitiesWithDatasetsAndActivities.map(_.to[entities.Project])) { project =>
        val maybeParentId = project match {
          case p: entities.ProjectWithParent => p.parentResourceId.some
          case _ => Option.empty[projects.ResourceId]
        }

        project.asJsonLD.toJson shouldBe JsonLD
          .arr(
            JsonLD.entity(
              EntityId.of(project.resourceId.show),
              entities.Project.entityTypes,
              schema / "name"             -> project.name.asJsonLD,
              renku / "projectPath"       -> project.path.asJsonLD,
              renku / "projectNamespaces" -> project.namespaces.asJsonLD,
              schema / "agent"            -> project.agent.asJsonLD,
              schema / "dateCreated"      -> project.dateCreated.asJsonLD,
              schema / "creator"          -> project.maybeCreator.asJsonLD,
              renku / "projectVisibility" -> project.visibility.asJsonLD,
              schema / "member"           -> project.members.toList.asJsonLD,
              schema / "schemaVersion"    -> project.version.asJsonLD,
              renku / "hasActivity"       -> project.activities.asJsonLD,
              renku / "hasPlan"           -> project.plans.toList.asJsonLD,
              renku / "hasDataset"        -> project.datasets.asJsonLD,
              prov / "wasDerivedFrom"     -> maybeParentId.map(_.asEntityId).asJsonLD
            ) :: project.datasets.flatMap(_.publicationEvents.map(_.asJsonLD)): _*
          )
          .toJson
      }
    }
  }

  private def cliLikeJsonLD(resourceId:    projects.ResourceId,
                            cliVersion:    CliVersion,
                            schemaVersion: SchemaVersion,
                            dateCreated:   DateCreated,
                            activities:    List[Activity],
                            datasets:      List[Dataset[Dataset.Provenance]],
                            persons:       Set[entities.Person]
  ) = JsonLD
    .arr(
      JsonLD.entity(
        resourceId.asEntityId,
        EntityTypes.of(prov / "Location", schema / "Project"),
        schema / "agent"         -> cliVersion.asJsonLD,
        schema / "schemaVersion" -> schemaVersion.asJsonLD,
        schema / "dateCreated"   -> dateCreated.asJsonLD,
        renku / "hasActivity"    -> activities.asJsonLD,
        renku / "hasPlan"        -> activities.map(_.plan).distinct.asJsonLD,
        renku / "hasDataset"     -> datasets.asJsonLD
      ) ::
        datasets.flatMap(_.publicationEvents.map(_.asJsonLD)) :::
        persons.toList.map(_.asJsonLD(personWithMultipleNamesEncoder)): _*
    )
    .flatten
    .fold(throw _, identity)

  private lazy val personWithMultipleNamesEncoder: JsonLDEncoder[entities.Person] =
    JsonLDEncoder.instance { person =>
      JsonLD.entity(
        person.resourceId.asEntityId,
        entities.Person.entityTypes,
        schema / "email"       -> person.maybeEmail.asJsonLD,
        schema / "name"        -> ((person.alternativeNames - person.name).toList ::: person.name :: Nil).asJsonLD,
        schema / "affiliation" -> person.maybeAffiliation.asJsonLD,
        schema / "sameAs"      -> person.maybeGitLabId.asJsonLD(encodeOption(entities.Person.gitLabIdEncoder))
      )
    }

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
      else {
        val name = userNames.generateOne
        personEntities.generateOne
          .copy(
            name = name,
            maybeGitLabId = None
          )
          .to[entities.Person]
          .copy(alternativeNames = Set(name, nameFromUsernameOrName(gitLabPerson)))
      }

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
