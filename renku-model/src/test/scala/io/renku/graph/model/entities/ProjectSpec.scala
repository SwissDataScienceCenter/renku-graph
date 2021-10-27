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

import cats.data.Validated
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.generators.jsonld.JsonLDGenerators.entityIds
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.Schemas.{prov, renku, schema}
import io.renku.graph.model._
import io.renku.graph.model.entities.Project.ProjectMember
import io.renku.graph.model.projects.DateCreated
import io.renku.graph.model.testentities._
import io.renku.jsonld.JsonLDDecoder._
import io.renku.jsonld.JsonLDEncoder.encodeOption
import io.renku.jsonld._
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.{LocalDate, ZoneOffset}
import scala.util.Random

class ProjectSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "from" should {

    "sync project's person entities across activities and datasets" in {
      val onlyGLMember = personEntities(withGitLabId, withoutEmail).generateOne
        .to[entities.Person]
        .copy(resourceId = userResourceIds.generateOne)
      val gLAndKGMember = personEntities(withGitLabId, withEmail).generateOne
        .to[entities.Person]
        .copy(resourceId = userResourceIds.generateOne)
      val creator = personEntities(withGitLabId, withEmail).generateOne
        .to[entities.Person]
        .copy(resourceId = userResourceIds.generateOne)
      val activities = activitiesWith(creator.copy(maybeGitLabId = None))
      val datasets   = datasetsWith(Set(gLAndKGMember.copy(maybeGitLabId = None)))

      Set(
        projectEntities(anyVisibility).map { project =>
          entities.ProjectWithoutParent
            .from(
              project.resourceId,
              project.path,
              project.name,
              project.agent,
              project.dateCreated,
              Some(creator),
              project.visibility,
              Set(onlyGLMember, gLAndKGMember),
              project.version,
              activities(project.dateCreated),
              datasets(project.dateCreated)
            )
        }.generateOne,
        projectWithParentEntities(anyVisibility).map { project =>
          entities.ProjectWithParent
            .from(
              project.resourceId,
              project.path,
              project.name,
              project.agent,
              project.dateCreated,
              Some(creator),
              project.visibility,
              Set(onlyGLMember, gLAndKGMember),
              project.version,
              activities(project.dateCreated),
              datasets(project.dateCreated),
              project.parent.resourceId
            )
        }.generateOne
      ) foreach {
        case Validated.Valid(entitiesProject) =>
          entitiesProject.maybeCreator                                  shouldBe creator.some
          entitiesProject.members                                       shouldBe Set(onlyGLMember, gLAndKGMember)
          entitiesProject.activities.map(_.author)                      shouldBe List(creator)
          entitiesProject.datasets.flatMap(_.provenance.creators).toSet shouldBe Set(gLAndKGMember)
        case invalid => fail(invalid.toString)
      }
    }
  }

  "decode" should {

    "turn JsonLD Project entity without parent into the Project object" in {
      forAll(gitLabProjectInfos.map(_.copy(maybeParentPath = None)), cliVersions, projectSchemaVersions) {
        (projectInfo, cliVersion, schemaVersion) =>
          val resourceId = projects.ResourceId(projectInfo.path)
          val activities = activityEntities(planEntities())
            .modify(_.copy(author = personEntities(withoutGitLabId).generateOne))
            .generateList(projectInfo.dateCreated)
            .sortBy(_.startTime)
          val datasets = datasetEntities(provenanceNonModified)
            .modify(provenanceLens.modify(creatorsLens.modify(_ => Set(personEntities(withoutGitLabId).generateOne))))
            .generateList(projectInfo.dateCreated)
          val maybeCreator = projectInfo.maybeCreator.map(_.toPayloadPerson)
          val members      = projectInfo.members.map(_.toPayloadPerson)
          val jsonLD = cliLikeJsonLD(resourceId,
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
        val resourceId = projects.ResourceId(projectInfo.path)
        val activities = activityEntities(planEntities())
          .modify(_.copy(author = personEntities(withoutGitLabId).generateOne))
          .generateList(projectInfo.dateCreated)
          .sortBy(_.startTime)
        val datasets = datasetEntities(provenanceNonModified)
          .modify(provenanceLens.modify(creatorsLens.modify(_ => Set(personEntities(withoutGitLabId).generateOne))))
          .generateList(projectInfo.dateCreated)
        val maybeCreator = projectInfo.maybeCreator.map(_.toPayloadPerson)
        val members      = projectInfo.members.map(_.toPayloadPerson)
        val jsonLD = cliLikeJsonLD(resourceId,
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
            projects.ResourceId(projectInfo.maybeParentPath.getOrElse(fail("No parent project")))
          )
        ).asRight
      }
    }

    "turn JsonLD Project entity into the Project object " +
      "- case when not all GitLab persons exist in the potential members" in {
        forAll(gitLabProjectInfos.map(_.copy(maybeParentPath = None)), cliVersions, projectSchemaVersions) {
          (projectInfo, cliVersion, schemaVersion) =>
            val resourceId = projects.ResourceId(projectInfo.path)
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
      val resourceId  = projects.ResourceId(projectInfo.path)
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
      val resourceId  = projects.ResourceId(projectInfo.path)
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
      val resourceId  = projects.ResourceId(projectInfo.path)
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
      val resourceId  = projects.ResourceId(projectInfo.path)
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
      val resourceId  = projects.ResourceId(projectInfo.path)
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
      val resourceId    = projects.ResourceId(projectInfo.path)
      val maybeCreator  = projectInfo.maybeCreator.map(_.toPayloadPerson)
      val members       = projectInfo.members.map(_.toPayloadPerson)
      val dataset1      = datasetEntities(provenanceInternal).withDateBefore(projectInfo.dateCreated).generateOne
      val (dataset2, dateset2Modified) =
        datasetAndModificationEntities(provenanceInternal).map { case (orig, modified) =>
          val newOrigDate = timestamps(max = projectInfo.dateCreated.value).generateAs[datasets.DateCreated]
          val newModificationDate =
            timestamps(min = newOrigDate.instant, max = projectInfo.dateCreated.value).generateAs[datasets.DateCreated]
          (
            orig.copy(provenance = orig.provenance.copy(date = newOrigDate)),
            modified.copy(provenance = modified.provenance.copy(date = newModificationDate))
          )
        }.generateOne
      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersion,
        schemaVersion,
        projectInfo.dateCreated,
        activities = Nil,
        datasets = List(dataset1, dataset2, dateset2Modified),
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
          List(dataset1, dataset2, dateset2Modified).map(_.to[entities.Dataset[entities.Dataset.Provenance]]),
          projects.ResourceId(parentPath)
        )
      ).asRight
    }

    "return a DecodingFailure when there's a modified Dataset entity created before project without parent" in {
      val projectInfo = gitLabProjectInfos.map(_.copy(maybeParentPath = None)).generateOne
      val resourceId  = projects.ResourceId(projectInfo.path)
      val (dataset, datesetModified) =
        datasetAndModificationEntities(provenanceImportedExternal).map { case (orig, modified) =>
          val newOrigDate = timestamps(max = projectInfo.dateCreated.value)
            .map(LocalDate.ofInstant(_, ZoneOffset.UTC))
            .generateAs[datasets.DatePublished]
          val newModificationDate =
            timestamps(min = newOrigDate.instant, max = projectInfo.dateCreated.value).generateAs[datasets.DateCreated]
          (
            orig.copy(provenance = orig.provenance.copy(date = newOrigDate), parts = Nil),
            modified.copy(provenance = modified.provenance.copy(date = newModificationDate), parts = Nil)
          )
        }.generateOne
      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersions.generateOne,
        projectSchemaVersions.generateOne,
        projectInfo.dateCreated,
        activities = Nil,
        datasets = List(dataset, datesetModified),
        persons = Set.empty
      )

      val Left(error) = jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo)))

      error shouldBe a[DecodingFailure]
      error
        .getMessage() shouldBe s"Dataset ${datesetModified.identification.identifier} " +
        s"date ${datesetModified.provenance.date} is older than project ${projectInfo.dateCreated}"
    }

    "decode project when there's a Dataset (neither internal nor modified) created before project creation" in {
      val projectInfo   = gitLabProjectInfos.map(_.copy(maybeParentPath = None)).generateOne
      val cliVersion    = cliVersions.generateOne
      val schemaVersion = projectSchemaVersions.generateOne
      val resourceId    = projects.ResourceId(projectInfo.path)
      val maybeCreator  = projectInfo.maybeCreator.map(_.toPayloadPerson)
      val members       = projectInfo.members.map(_.toPayloadPerson)
      val dataset1 = datasetEntities(provenanceImportedExternal).withDateBefore(projectInfo.dateCreated).generateOne
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

    "return a DecodingFailure when there's a modified Dataset that is derived from a non-existing dataset" in {
      Set(
        gitLabProjectInfos.map(_.copy(maybeParentPath = None)).generateOne,
        gitLabProjectInfos.map(_.copy(maybeParentPath = projectPaths.generateSome)).generateOne
      ) foreach { projectInfo =>
        val resourceId = projects.ResourceId(projectInfo.path)
        val (original, modified) =
          datasetAndModificationEntities(provenanceInternal, projectInfo.dateCreated).generateOne
        val (_, broken) = datasetAndModificationEntities(provenanceInternal, projectInfo.dateCreated).generateOne
        val jsonLD = cliLikeJsonLD(
          resourceId,
          cliVersions.generateOne,
          projectSchemaVersions.generateOne,
          projectInfo.dateCreated,
          activities = Nil,
          datasets = List(original, modified, broken),
          persons = Set.empty
        )

        val Left(error) = jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo)))

        error shouldBe a[DecodingFailure]
        error.getMessage() shouldBe
          show"Dataset ${broken.identification.identifier} is derived from non-existing dataset ${broken.provenance.derivedFrom}"
      }
    }

    "decode project should pick the earliest from dateCreated found in gitlabProjectInfo and the CLI" in {
      val gitlabDate    = projectCreatedDates().generateOne
      val cliDate       = projectCreatedDates().generateOne
      val earliestDate  = List(gitlabDate, cliDate).min
      val projectInfo   = gitLabProjectInfos.map(_.copy(maybeParentPath = None, dateCreated = gitlabDate)).generateOne
      val cliVersion    = cliVersions.generateOne
      val schemaVersion = projectSchemaVersions.generateOne
      val resourceId    = projects.ResourceId(projectInfo.path)
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
    from
      .map(member => person.copy(maybeGitLabId = member.gitLabId.some))
      .getOrElse(person)

  private def copyGitLabId(fromMatching: Set[ProjectMember])(
      person:                            entities.Person
  ): entities.Person = copyGitLabId(fromMatching.find(byNameOrUsername(person)))(person)

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
        users.ResourceId(gitLabPerson.gitLabId).show,
        name = gitLabPerson.name,
        maybeEmail = None,
        maybeAffiliation = None,
        maybeGitLabId = gitLabPerson.gitLabId.some
      )
  }

  private def activitiesWith(author: entities.Person): projects.DateCreated => List[entities.Activity] = dateCreated =>
    List(
      activityEntities(planEntities())(dateCreated).generateOne.to[entities.Activity].copy(author = author)
    )

  private def datasetsWith(
      creators: Set[entities.Person]
  ): projects.DateCreated => List[entities.Dataset[entities.Dataset.Provenance]] = dateCreated =>
    List {
      val ds = datasetEntities(provenanceNonModified)(renkuBaseUrl)(dateCreated).generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]
      ds.copy(provenance = ds.provenance match {
        case p: entities.Dataset.Provenance.Internal                         => p.copy(creators = creators)
        case p: entities.Dataset.Provenance.ImportedExternal                 => p.copy(creators = creators)
        case p: entities.Dataset.Provenance.ImportedInternalAncestorInternal => p.copy(creators = creators)
        case p: entities.Dataset.Provenance.ImportedInternalAncestorExternal => p.copy(creators = creators)
        case p: entities.Dataset.Provenance.Modified                         => p.copy(creators = creators)
      })
    }
}
