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
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.generators.jsonld.JsonLDGenerators.entityIds
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.Schemas.{prov, renku, schema}
import io.renku.graph.model._
import io.renku.graph.model.entities.Project.ProjectMember
import io.renku.graph.model.entities.Project.ProjectMember.{ProjectMemberNoEmail, ProjectMemberWithEmail}
import io.renku.graph.model.projects.{DateCreated, Description}
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

  "ProjectMember.add" should {
    "add the given email to the Project without an email" in {
      val member = projectMembersNoEmail.generateOne
      val email  = userEmails.generateOne

      (member add email) shouldBe ProjectMember.ProjectMemberWithEmail(member.name,
                                                                       member.username,
                                                                       member.gitLabId,
                                                                       email
      )
    }
  }

  "decode" should {

    "turn JsonLD Project entity without parent into the Project object" in {
      forAll(gitLabProjectInfos.map(_.copy(maybeParentPath = None)), cliVersions, projectSchemaVersions) {
        (projectInfo, cliVersion, schemaVersion) =>
          val creator    = projectMembersWithEmail.generateOne
          val member1    = projectMembersNoEmail.generateOne
          val member2    = projectMembersWithEmail.generateOne
          val member3    = projectMembersWithEmail.generateOne
          val info       = projectInfo.copy(maybeCreator = creator.some, members = Set(member1, member2, member3))
          val resourceId = projects.ResourceId(info.path)
          val activity1  = activityWith(member2.toCLIPayloadPerson)(info.dateCreated)
          val activity2 =
            activityWith(personEntities(withoutGitLabId).generateOne.to[entities.Person])(info.dateCreated)
          val dataset1 = datasetWith(Set(creator, member3).map(_.toCLIPayloadPerson))(info.dateCreated)
          val dataset2 =
            datasetWith(Set(personEntities(withoutGitLabId).generateOne.to[entities.Person]))(info.dateCreated)

          val jsonLD = cliLikeJsonLD(resourceId,
                                     cliVersion,
                                     schemaVersion,
                                     info.maybeDescription,
                                     info.dateCreated,
                                     activity1 :: activity2 :: Nil,
                                     dataset1 :: dataset2 :: Nil
          )

          val mergedCreator = dataset1.provenance.creators.find(byEmail(creator)).map(merge(_, creator))
          val mergedMember2 = merge(activity1.author, member2)
          val mergedMember3 = dataset1.provenance.creators.find(byEmail(member3)).map(merge(_, member3))

          jsonLD.cursor.as(decodeList(entities.Project.decoder(info))) shouldBe List(
            entities.ProjectWithoutParent(
              resourceId,
              info.path,
              info.name,
              info.maybeDescription,
              cliVersion,
              info.dateCreated,
              maybeCreator = mergedCreator,
              info.visibility,
              members = Set(
                entities
                  .Person(users.ResourceId(member1.gitLabId), member1.name, maybeGitLabId = member1.gitLabId.some)
                  .some,
                mergedMember2.some,
                mergedMember3
              ).flatten,
              schemaVersion,
              (activity1.copy(author = mergedMember2) :: activity2 :: Nil).sortBy(_.startTime),
              addTo(dataset1, Set(mergedCreator, mergedMember3).flatten) :: dataset2 :: Nil
            )
          ).asRight
      }
    }

    "turn JsonLD Project entity with parent into the Project object" in {
      forAll(gitLabProjectInfos.map(_.copy(maybeParentPath = projectPaths.generateSome)),
             cliVersions,
             projectSchemaVersions
      ) { (projectInfo, cliVersion, schemaVersion) =>
        val creator    = projectMembersWithEmail.generateOne
        val member1    = projectMembersNoEmail.generateOne
        val member2    = projectMembersWithEmail.generateOne
        val member3    = projectMembersWithEmail.generateOne
        val info       = projectInfo.copy(maybeCreator = creator.some, members = Set(member1, member2, member3))
        val resourceId = projects.ResourceId(info.path)
        val activity1  = activityWith(member2.toCLIPayloadPerson)(info.dateCreated)
        val activity2 =
          activityWith(personEntities(withoutGitLabId).generateOne.to[entities.Person])(info.dateCreated)
        val dataset1 = datasetWith(Set(creator, member3).map(_.toCLIPayloadPerson))(info.dateCreated)
        val dataset2 =
          datasetWith(Set(personEntities(withoutGitLabId).generateOne.to[entities.Person]))(info.dateCreated)

        val jsonLD = cliLikeJsonLD(resourceId,
                                   cliVersion,
                                   schemaVersion,
                                   info.maybeDescription,
                                   info.dateCreated,
                                   activity1 :: activity2 :: Nil,
                                   dataset1 :: dataset2 :: Nil
        )

        val mergedCreator = dataset1.provenance.creators.find(byEmail(creator)).map(merge(_, creator))
        val mergedMember2 = merge(activity1.author, member2)
        val mergedMember3 = dataset1.provenance.creators.find(byEmail(member3)).map(merge(_, member3))

        jsonLD.cursor.as(decodeList(entities.Project.decoder(info))) shouldBe List(
          entities.ProjectWithParent(
            resourceId,
            info.path,
            info.name,
            info.maybeDescription,
            cliVersion,
            info.dateCreated,
            mergedCreator,
            info.visibility,
            members = Set(
              member1.toPerson.some,
              mergedMember2.some,
              mergedMember3
            ).flatten,
            schemaVersion,
            (activity1.copy(author = mergedMember2) :: activity2 :: Nil).sortBy(_.startTime),
            addTo(dataset1, Set(mergedCreator, mergedMember3).flatten) :: dataset2 :: Nil,
            projects.ResourceId(info.maybeParentPath.getOrElse(fail("No parent project")))
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
                                 projectInfo.maybeDescription,
                                 projectInfo.dateCreated,
                                 activities = Nil,
                                 datasets = Nil
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
                                 projectInfo.maybeDescription,
                                 projectInfo.dateCreated,
                                 activities = Nil,
                                 datasets = Nil
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
                                 projectInfo.maybeDescription,
                                 projectInfo.dateCreated,
                                 activities = Nil,
                                 datasets = Nil
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
        projectInfo.maybeDescription,
        projectInfo.dateCreated,
        activities = List(activity.to[entities.Activity]),
        datasets = Nil
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
        projectInfo.maybeDescription,
        projectInfo.dateCreated,
        activities = Nil,
        datasets = List(dataset.to[entities.Dataset[entities.Dataset.Provenance.Internal]])
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
      val dataset1 =
        datasetEntities(provenanceInternal).withDateBefore(projectInfo.dateCreated).generateOne.copy(parts = Nil)
      val (dataset2, dateset2Modified) = datasetAndModificationEntities(provenanceInternal).map {
        case (orig, modified) =>
          val newOrigDate = timestamps(max = projectInfo.dateCreated.value).generateAs[datasets.DateCreated]
          val newModificationDate =
            timestamps(min = newOrigDate.instant, max = projectInfo.dateCreated.value).generateAs[datasets.DateCreated]
          (
            orig.copy(provenance = orig.provenance.copy(date = newOrigDate), parts = Nil),
            modified.copy(provenance = modified.provenance.copy(date = newModificationDate), parts = Nil)
          )
      }.generateOne
      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersion,
        schemaVersion,
        projectInfo.maybeDescription,
        projectInfo.dateCreated,
        activities = Nil,
        datasets = List(dataset1, dataset2, dateset2Modified).map(_.to[entities.Dataset[entities.Dataset.Provenance]])
      )

      jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo))) shouldBe List(
        entities.ProjectWithParent(
          resourceId,
          projectInfo.path,
          projectInfo.name,
          projectInfo.maybeDescription,
          cliVersion,
          projectInfo.dateCreated,
          projectInfo.maybeCreator.map(_.toPerson),
          projectInfo.visibility,
          projectInfo.members.map(_.toPerson),
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
        projectInfo.maybeDescription,
        projectInfo.dateCreated,
        activities = Nil,
        datasets = List(dataset, datesetModified).map(_.to[entities.Dataset[entities.Dataset.Provenance]])
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
      val dataset1 = datasetEntities(provenanceImportedExternal).withDateBefore(projectInfo.dateCreated).generateOne
      val dataset2 =
        datasetEntities(provenanceImportedInternalAncestorExternal).withDateBefore(projectInfo.dateCreated).generateOne
      val dataset3 =
        datasetEntities(provenanceImportedInternalAncestorInternal).withDateBefore(projectInfo.dateCreated).generateOne
      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersion,
        schemaVersion,
        projectInfo.maybeDescription,
        projectInfo.dateCreated,
        activities = Nil,
        datasets = List(dataset1, dataset2, dataset3).map(_.to[entities.Dataset[entities.Dataset.Provenance]])
      )

      jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo))) shouldBe List(
        entities.ProjectWithoutParent(
          resourceId,
          projectInfo.path,
          projectInfo.name,
          projectInfo.maybeDescription,
          cliVersion,
          projectInfo.dateCreated,
          projectInfo.maybeCreator.map(_.toPerson),
          projectInfo.visibility,
          projectInfo.members.map(_.toPerson),
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
          projectInfo.maybeDescription,
          projectInfo.dateCreated,
          activities = Nil,
          datasets = List(original, modified, broken).map(_.to[entities.Dataset[entities.Dataset.Provenance]])
        )

        val Left(error) = jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo)))

        error shouldBe a[DecodingFailure]
        error.getMessage() shouldBe
          show"Dataset ${broken.identification.identifier} is derived from non-existing dataset ${broken.provenance.derivedFrom}"
      }
    }

    "pick the earliest from dateCreated found in gitlabProjectInfo and the CLI" in {
      val gitlabDate    = projectCreatedDates().generateOne
      val cliDate       = projectCreatedDates().generateOne
      val earliestDate  = List(gitlabDate, cliDate).min
      val projectInfo   = gitLabProjectInfos.map(_.copy(maybeParentPath = None, dateCreated = gitlabDate)).generateOne
      val cliVersion    = cliVersions.generateOne
      val schemaVersion = projectSchemaVersions.generateOne
      val resourceId    = projects.ResourceId(projectInfo.path)

      val jsonLD = cliLikeJsonLD(resourceId,
                                 cliVersion,
                                 schemaVersion,
                                 projectInfo.maybeDescription,
                                 cliDate,
                                 activities = Nil,
                                 datasets = Nil
      )

      jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo))) shouldBe List(
        entities.ProjectWithoutParent(
          resourceId,
          projectInfo.path,
          projectInfo.name,
          projectInfo.maybeDescription,
          cliVersion,
          earliestDate,
          projectInfo.maybeCreator.map(_.toPerson),
          projectInfo.visibility,
          projectInfo.members.map(_.toPerson),
          schemaVersion,
          activities = Nil,
          datasets = Nil
        )
      ).asRight
    }

    "favor the project description from the CLI over the gitlab description" in {
      val gitlabDate    = projectCreatedDates().generateOne
      val cliDate       = projectCreatedDates().generateOne
      val earliestDate  = List(gitlabDate, cliDate).min
      val projectInfo   = gitLabProjectInfos.map(_.copy(maybeParentPath = None, dateCreated = gitlabDate)).generateOne
      val description   = projectDescriptions.generateSome
      val cliVersion    = cliVersions.generateOne
      val schemaVersion = projectSchemaVersions.generateOne
      val resourceId    = projects.ResourceId(projectInfo.path)

      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersion,
        schemaVersion,
        description,
        cliDate,
        activities = Nil,
        datasets = Nil
      )

      jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo))) shouldBe List(
        entities.ProjectWithoutParent(
          resourceId,
          projectInfo.path,
          projectInfo.name,
          description,
          cliVersion,
          earliestDate,
          projectInfo.maybeCreator.map(_.toPerson),
          projectInfo.visibility,
          projectInfo.members.map(_.toPerson),
          schemaVersion,
          activities = Nil,
          datasets = Nil
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
              schema / "description"      -> project.maybeDescription.asJsonLD,
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

  private def cliLikeJsonLD(resourceId:              projects.ResourceId,
                            cliVersion:              CliVersion,
                            schemaVersion:           SchemaVersion,
                            maybeProjectDescription: Option[Description],
                            dateCreated:             DateCreated,
                            activities:              List[entities.Activity],
                            datasets:                List[entities.Dataset[entities.Dataset.Provenance]]
  ) = {
    val descriptionJsonLD = maybeProjectDescription match {
      case Some(desc) => desc.asJsonLD
      case None =>
        if (Random.nextBoolean()) blankStrings().generateOne.asJsonLD
        else maybeProjectDescription.asJsonLD
    }
    JsonLD
      .arr(
        JsonLD.entity(
          resourceId.asEntityId,
          EntityTypes.of(prov / "Location", schema / "Project"),
          schema / "agent"         -> cliVersion.asJsonLD,
          schema / "schemaVersion" -> schemaVersion.asJsonLD,
          schema / "description"   -> descriptionJsonLD,
          schema / "dateCreated"   -> dateCreated.asJsonLD,
          renku / "hasActivity"    -> activities.asJsonLD,
          renku / "hasPlan"        -> activities.map(_.association.plan).distinct.asJsonLD,
          renku / "hasDataset"     -> datasets.asJsonLD
        ) :: datasets.flatMap(_.publicationEvents.map(_.asJsonLD)): _*
      )
      .flatten
      .fold(throw _, identity)
  }

  private implicit class ProjectMemberOps(gitLabPerson: ProjectMember) {

    lazy val toCLIPayloadPerson: entities.Person = gitLabPerson match {
      case member: ProjectMemberNoEmail =>
        personEntities.generateOne
          .copy(
            name = nameFromUsernameOrName(member),
            maybeEmail = None,
            maybeGitLabId = None
          )
          .to[entities.Person]
      case member: ProjectMemberWithEmail =>
        personEntities.generateOne
          .copy(
            name = nameFromUsernameOrName(member),
            maybeEmail = member.email.some,
            maybeGitLabId = None
          )
          .to[entities.Person]
    }

    lazy val toPerson: entities.Person = gitLabPerson match {
      case ProjectMemberNoEmail(name, _, gitLabId) =>
        entities.Person(users.ResourceId(gitLabId), name, maybeGitLabId = gitLabId.some)
      case ProjectMemberWithEmail(name, _, gitLabId, email) =>
        entities.Person(users.ResourceId(gitLabId), name, maybeGitLabId = gitLabId.some, maybeEmail = email.some)
    }

    private def nameFromUsernameOrName(member: ProjectMember) =
      if (Random.nextBoolean()) member.name
      else users.Name(member.username.value)
  }

  private def activityWith(author: entities.Person): projects.DateCreated => entities.Activity = dateCreated =>
    activityEntities(planEntities())(dateCreated).generateOne.to[entities.Activity].copy(author = author)

  private def datasetWith(
      creators: Set[entities.Person]
  ): projects.DateCreated => entities.Dataset[entities.Dataset.Provenance] = dateCreated => {
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

  private def addTo(
      dataset:  entities.Dataset[entities.Dataset.Provenance],
      creators: Set[entities.Person]
  ): entities.Dataset[entities.Dataset.Provenance] =
    dataset.copy(provenance = dataset.provenance match {
      case p: entities.Dataset.Provenance.Internal                         => p.copy(creators = creators)
      case p: entities.Dataset.Provenance.ImportedExternal                 => p.copy(creators = creators)
      case p: entities.Dataset.Provenance.ImportedInternalAncestorInternal => p.copy(creators = creators)
      case p: entities.Dataset.Provenance.ImportedInternalAncestorExternal => p.copy(creators = creators)
      case p: entities.Dataset.Provenance.Modified                         => p.copy(creators = creators)
    })

  private def byEmail(member: ProjectMemberWithEmail): entities.Person => Boolean =
    _.maybeEmail.contains(member.email)

  private def merge(person: entities.Person, member: ProjectMemberWithEmail): entities.Person =
    person.copy(resourceId = users.ResourceId(member.gitLabId),
                name = member.name,
                maybeEmail = member.email.some,
                maybeGitLabId = member.gitLabId.some
    )
}
