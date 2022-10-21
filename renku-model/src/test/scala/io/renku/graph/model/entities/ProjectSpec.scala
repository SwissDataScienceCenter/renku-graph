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

import cats.data.NonEmptyList
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.Schemas.{prov, renku, schema}
import io.renku.graph.model._
import io.renku.graph.model.entities.Project.ProjectMember
import io.renku.graph.model.entities.Project.ProjectMember.{ProjectMemberNoEmail, ProjectMemberWithEmail}
import io.renku.graph.model.persons.Name
import io.renku.graph.model.projects.{DateCreated, Description, Keyword}
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
      val email  = personEmails.generateOne

      (member add email) shouldBe ProjectMember.ProjectMemberWithEmail(member.name,
                                                                       member.username,
                                                                       member.gitLabId,
                                                                       email
      )
    }
  }

  "decode" should {
    implicit val graph: GraphClass = GraphClass.Default

    "match persons in plan.creators" in {
      forAll(gitLabProjectInfos.map(_.copy(maybeParentPath = None)), cliVersions, projectSchemaVersions) {
        (projectInfo, cliVersion, schemaVersion) =>
          val creator = projectMembersWithEmail.generateOne
          val member2 = projectMembersWithEmail.generateOne

          val info               = projectInfo.copy(maybeCreator = creator.some, members = Set(member2))
          val resourceId         = projects.ResourceId(info.path)
          val creatorAsCliPerson = creator.toCLIPayloadPerson(creator.chooseSomeName)
          val (activity, plan) = activityWith(member2.toCLIPayloadPerson(member2.chooseSomeName))(info.dateCreated)
            .bimap(identity, PlanLens.planCreators.set(List(creatorAsCliPerson)))

          val jsonLD = cliLikeJsonLD(
            resourceId,
            cliVersion,
            schemaVersion,
            info.maybeDescription,
            info.keywords,
            creatorAsCliPerson.some,
            info.dateCreated,
            activity :: Nil,
            datasets = Nil,
            plan :: Nil
          )
          val mergedCreator    = merge(creatorAsCliPerson, creator)
          val mergedMember2    = merge(activity.author, member2)
          val expectedActivity = ActivityLens.activityAuthor.set(mergedMember2)(activity)
          val expectedPlan     = PlanLens.planCreators.set(List(mergedCreator))(plan)

          jsonLD.cursor.as(decodeList(entities.Project.decoder(info))) shouldBe List(
            entities.RenkuProject.WithoutParent(
              resourceId,
              info.path,
              info.name,
              info.maybeDescription,
              cliVersion,
              info.dateCreated,
              maybeCreator = mergedCreator.some,
              info.visibility,
              info.keywords,
              members = Set(mergedMember2.some).flatten,
              schemaVersion,
              expectedActivity :: Nil,
              datasets = Nil,
              expectedPlan :: Nil
            )
          ).asRight
      }
    }

    "turn JsonLD Project entity without parent into the Project object" in {
      forAll(gitLabProjectInfos.map(_.copy(maybeParentPath = None)), cliVersions, projectSchemaVersions) {
        (projectInfo, cliVersion, schemaVersion) =>
          val creator    = projectMembersWithEmail.generateOne
          val member1    = projectMembersNoEmail.generateOne
          val member2    = projectMembersWithEmail.generateOne
          val member3    = projectMembersWithEmail.generateOne
          val info       = projectInfo.copy(maybeCreator = creator.some, members = Set(member1, member2, member3))
          val resourceId = projects.ResourceId(info.path)
          val creatorAsCliPerson = creator.toCLIPayloadPerson(creator.chooseSomeName)
          val (activity1, plan1) = activityWith(member2.toCLIPayloadPerson(member2.chooseSomeName))(info.dateCreated)
          val (activity2, plan2) =
            activityWith(personEntities(withoutGitLabId).generateOne.to[entities.Person])(info.dateCreated)
          val (activity3, plan3) = activityWithAssociationAgent(creatorAsCliPerson)(info.dateCreated)
          val dataset1 = datasetWith(
            NonEmptyList.of(creatorAsCliPerson, member3.toCLIPayloadPerson(member3.chooseSomeName))
          )(info.dateCreated)
          val dataset2 = datasetWith(NonEmptyList.of(personEntities(withoutGitLabId).generateOne.to[entities.Person]))(
            info.dateCreated
          )

          val jsonLD = cliLikeJsonLD(
            resourceId,
            cliVersion,
            schemaVersion,
            info.maybeDescription,
            info.keywords,
            None,
            info.dateCreated,
            activity1 :: activity2 :: activity3 :: Nil,
            dataset1 :: dataset2 :: Nil,
            plan1 :: plan2 :: plan3 :: Nil
          )

          val mergedCreator = merge(creatorAsCliPerson, creator)
          val mergedMember2 = merge(activity1.author, member2)
          val mergedMember3 = dataset1.provenance.creators.find(byEmail(member3)).map(merge(_, member3))

          val expectedActivities =
            (activity1.copy(author = mergedMember2) ::
              activity2 ::
              replaceAgent(activity3, mergedCreator) :: Nil)
              .sortBy(_.startTime)
          jsonLD.cursor.as(decodeList(entities.Project.decoder(info))) shouldBe List(
            entities.RenkuProject.WithoutParent(
              resourceId,
              info.path,
              info.name,
              info.maybeDescription,
              cliVersion,
              info.dateCreated,
              maybeCreator = mergedCreator.some,
              info.visibility,
              info.keywords,
              members = Set(member1.toPerson.some, mergedMember2.some, mergedMember3).flatten,
              schemaVersion,
              expectedActivities,
              addTo(dataset1,
                    mergedMember3.fold(NonEmptyList.of(mergedCreator))(_ :: NonEmptyList.of(mergedCreator))
              ) :: dataset2 :: Nil,
              plan1 :: plan2 :: plan3 :: Nil
            )
          ).asRight
      }
    }

    "turn JsonLD Project entity with parent into the Project object" in {
      forAll(gitLabProjectInfos.map(_.copy(maybeParentPath = projectPaths.generateSome)),
             cliVersions,
             projectSchemaVersions
      ) { (projectInfo, cliVersion, schemaVersion) =>
        val creator            = projectMembersWithEmail.generateOne
        val member1            = projectMembersNoEmail.generateOne
        val member2            = projectMembersWithEmail.generateOne
        val member3            = projectMembersWithEmail.generateOne
        val info               = projectInfo.copy(maybeCreator = creator.some, members = Set(member1, member2, member3))
        val resourceId         = projects.ResourceId(info.path)
        val creatorAsCliPerson = creator.toCLIPayloadPerson(creator.chooseSomeName)
        val (activity1, plan1) = activityWith(member2.toCLIPayloadPerson(member2.chooseSomeName))(info.dateCreated)
        val (activity2, plan2) =
          activityWith(personEntities(withoutGitLabId).generateOne.to[entities.Person])(info.dateCreated)
        val (activity3, plan3) = activityWithAssociationAgent(creatorAsCliPerson)(info.dateCreated)
        val dataset1 = datasetWith(
          NonEmptyList.of(creatorAsCliPerson, member3.toCLIPayloadPerson(member3.chooseSomeName))
        )(info.dateCreated)
        val dataset2 =
          datasetWith(NonEmptyList.of(personEntities(withoutGitLabId).generateOne.to[entities.Person]))(
            info.dateCreated
          )

        val jsonLD = cliLikeJsonLD(
          resourceId,
          cliVersion,
          schemaVersion,
          info.maybeDescription,
          info.keywords,
          None,
          info.dateCreated,
          activity1 :: activity2 :: activity3 :: Nil,
          dataset1 :: dataset2 :: Nil,
          plan1 :: plan2 :: plan3 :: Nil
        )

        val mergedCreator = merge(creatorAsCliPerson, creator)
        val mergedMember2 = merge(activity1.author, member2)
        val mergedMember3 = dataset1.provenance.creators.find(byEmail(member3)).map(merge(_, member3))

        val expectedActivities = (activity1.copy(author = mergedMember2) ::
          activity2 ::
          replaceAgent(activity3, mergedCreator) :: Nil)
          .sortBy(_.startTime)
        jsonLD.cursor.as(decodeList(entities.Project.decoder(info))) shouldBe List(
          entities.RenkuProject.WithParent(
            resourceId,
            info.path,
            info.name,
            info.maybeDescription,
            cliVersion,
            info.dateCreated,
            mergedCreator.some,
            info.visibility,
            info.keywords,
            members = Set(member1.toPerson.some, mergedMember2.some, mergedMember3).flatten,
            schemaVersion,
            expectedActivities,
            addTo(dataset1,
                  mergedMember3.fold(NonEmptyList.of(mergedCreator))(_ :: NonEmptyList.of(mergedCreator))
            ) :: dataset2 :: Nil,
            plan1 :: plan2 :: plan3 :: Nil,
            projects.ResourceId(info.maybeParentPath.getOrElse(fail("No parent project")))
          )
        ).asRight
      }
    }

    "turn non-renku JsonLD Project entity without parent into the NonRenkuProject object" in {
      forAll(gitLabProjectInfos.map(_.copy(maybeParentPath = None))) { projectInfo =>
        val creator    = projectMembersWithEmail.generateOne
        val members    = projectMembers.generateSet()
        val info       = projectInfo.copy(maybeCreator = creator.some, members = members)
        val resourceId = projects.ResourceId(info.path)

        val jsonLD = minimalCliLikeJsonLD(resourceId)

        jsonLD.cursor.as(decodeList(entities.Project.decoder(info))) shouldBe List(
          entities.NonRenkuProject.WithoutParent(
            resourceId,
            info.path,
            info.name,
            info.maybeDescription,
            info.dateCreated,
            creator.some.map(_.toPerson),
            info.visibility,
            info.keywords,
            members.map(_.toPerson)
          )
        ).asRight
      }
    }

    "turn non-renku JsonLD Project entity with parent into the NonRenkuProject object" in {
      forAll(gitLabProjectInfos.map(_.copy(maybeParentPath = projectPaths.generateSome))) { projectInfo =>
        val creator    = projectMembersWithEmail.generateOne
        val members    = projectMembers.generateSet()
        val info       = projectInfo.copy(maybeCreator = creator.some, members = members)
        val resourceId = projects.ResourceId(info.path)

        val jsonLD = minimalCliLikeJsonLD(resourceId)

        jsonLD.cursor.as(decodeList(entities.Project.decoder(info))) shouldBe List(
          entities.NonRenkuProject.WithParent(
            resourceId,
            info.path,
            info.name,
            info.maybeDescription,
            info.dateCreated,
            creator.some.map(_.toPerson),
            info.visibility,
            info.keywords,
            members.map(_.toPerson),
            projects.ResourceId(info.maybeParentPath.getOrElse(fail("No parent project")))
          )
        ).asRight
      }
    }

    "return a DecodingFailure when there's a Person entity that cannot be decoded" in {
      val projectInfo = gitLabProjectInfos.map(_.copy(maybeParentPath = None)).generateOne
      val resourceId  = projects.ResourceId(projectInfo.path)
      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersions.generateOne,
        projectSchemaVersions.generateOne,
        projectInfo.maybeDescription,
        projectInfo.keywords,
        None,
        projectInfo.dateCreated,
        activities = Nil,
        datasets = Nil,
        plans = Nil
      )

      val Left(error) = JsonLD
        .arr(jsonLD,
             JsonLD.entity(personResourceIds.generateOne.asEntityId,
                           entities.Person.entityTypes,
                           Map.empty[Property, JsonLD]
             )
        )
        .flatten
        .fold(throw _, identity)
        .cursor
        .as(decodeList(entities.Project.decoder(projectInfo)))

      error            shouldBe a[DecodingFailure]
      error.getMessage() should include(s"Finding Person entities for project ${projectInfo.path} failed: ")
    }

    "return a DecodingFailure when there's a Dataset entity that cannot be decoded" in {
      val projectInfo = gitLabProjectInfos.map(_.copy(maybeParentPath = None)).generateOne
      val resourceId  = projects.ResourceId(projectInfo.path)
      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersions.generateOne,
        projectSchemaVersions.generateOne,
        projectInfo.maybeDescription,
        projectInfo.keywords,
        projectInfo.maybeCreator.map(c => c.toCLIPayloadPerson(c.chooseSomeName)),
        projectInfo.dateCreated,
        datasets = datasetEntities(provenanceInternal)
          .withDateBefore(projectInfo.dateCreated)
          .generateFixedSizeList(1)
          .map(_.to[entities.Dataset[entities.Dataset.Provenance.Internal]].copy())
      )

      val Left(error) = jsonLD.flatten
        .fold(throw _, identity)
        .cursor
        .as(decodeList(entities.Project.decoder(projectInfo)))

      error            shouldBe a[DecodingFailure]
      error.getMessage() should (include("Dataset") and include("is older than project"))
    }

    "return a DecodingFailure when there's an Activity entity created before project creation" in {
      val projectInfo       = gitLabProjectInfos.map(_.copy(maybeParentPath = None)).generateOne
      val resourceId        = projects.ResourceId(projectInfo.path)
      val dateBeforeProject = timestamps(max = projectInfo.dateCreated.value.minusSeconds(1)).generateOne
      val activity = activityEntities(
        stepPlanEntities().modify(_.replacePlanDateCreated(plans.DateCreated(dateBeforeProject)))
      ).modify(
        _.replaceStartTime(
          timestamps(min = dateBeforeProject, max = projectInfo.dateCreated.value).generateAs[activities.StartTime]
        )
      )(projects.DateCreated(dateBeforeProject))
        .generateOne
      val entitiesActivity = activity.to[entities.Activity]
      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersions.generateOne,
        projectSchemaVersions.generateOne,
        projectInfo.maybeDescription,
        projectInfo.keywords,
        projectInfo.maybeCreator.map(c => c.toCLIPayloadPerson(c.chooseSomeName)),
        projectInfo.dateCreated,
        activities = entitiesActivity :: Nil,
        plans = activity.plan.to[entities.Plan] :: Nil
      )

      val Left(error) = jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo)))

      error shouldBe a[DecodingFailure]
      error.getMessage() should include(
        s"Activity ${entitiesActivity.resourceId} " +
          s"date ${activity.startTime} is older than project ${projectInfo.dateCreated}"
      )
    }

    "return a DecodingFailure when there's an internal Dataset entity created before project without parent" in {
      val projectInfo     = gitLabProjectInfos.map(_.copy(maybeParentPath = None)).generateOne
      val resourceId      = projects.ResourceId(projectInfo.path)
      val dataset         = datasetEntities(provenanceInternal).withDateBefore(projectInfo.dateCreated).generateOne
      val entitiesDataset = dataset.to[entities.Dataset[entities.Dataset.Provenance.Internal]]
      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersions.generateOne,
        projectSchemaVersions.generateOne,
        projectInfo.maybeDescription,
        projectInfo.keywords,
        projectInfo.maybeCreator.map(c => c.toCLIPayloadPerson(c.chooseSomeName)),
        projectInfo.dateCreated,
        datasets = entitiesDataset :: Nil
      )

      val Left(error) = jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo)))

      error shouldBe a[DecodingFailure]
      error.getMessage() should endWith(
        s"Dataset ${entitiesDataset.resourceId} " +
          s"date ${dataset.provenance.date} is older than project ${projectInfo.dateCreated}"
      )
    }

    "return a DecodingFailure when there's a Plan entity created before project without parent" in {
      val projectInfo = gitLabProjectInfos.map(_.copy(maybeParentPath = None)).generateOne
      val resourceId  = projects.ResourceId(projectInfo.path)
      val plan = stepPlanEntities()(planCommands)(projectInfo.dateCreated).generateOne
        .replacePlanDateCreated(timestamps(max = projectInfo.dateCreated.value).generateAs[plans.DateCreated])
      val entitiesPlan = plan.to[entities.Plan]
      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersions.generateOne,
        projectSchemaVersions.generateOne,
        projectInfo.maybeDescription,
        projectInfo.keywords,
        projectInfo.maybeCreator.map(c => c.toCLIPayloadPerson(c.chooseSomeName)),
        projectInfo.dateCreated,
        plans = entitiesPlan :: Nil
      )

      val Left(error) = jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo)))

      error shouldBe a[DecodingFailure]
      error.getMessage() should endWith(
        s"Plan ${entitiesPlan.resourceId} " +
          s"date ${entitiesPlan.dateCreated} is older than project ${projectInfo.dateCreated}"
      )
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
        projectInfo.keywords,
        maybeCreator = None,
        dateCreated = projectInfo.dateCreated,
        datasets = List(dataset1, dataset2, dateset2Modified).map(_.to[entities.Dataset[entities.Dataset.Provenance]])
      )

      jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo))) shouldBe List(
        entities.RenkuProject.WithParent(
          resourceId,
          projectInfo.path,
          projectInfo.name,
          projectInfo.maybeDescription,
          cliVersion,
          projectInfo.dateCreated,
          projectInfo.maybeCreator.map(_.toPerson),
          projectInfo.visibility,
          projectInfo.keywords,
          projectInfo.members.map(_.toPerson),
          schemaVersion,
          activities = Nil,
          List(dataset1, dataset2, dateset2Modified).map(_.to[entities.Dataset[entities.Dataset.Provenance]]),
          plans = Nil,
          projects.ResourceId(parentPath)
        )
      ).asRight
    }

    "return a DecodingFailure when there's a modified Dataset entity created before project without parent" in {
      val projectInfo = gitLabProjectInfos.map(_.copy(maybeParentPath = None)).generateOne
      val resourceId  = projects.ResourceId(projectInfo.path)
      val (dataset, modifiedDataset) =
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
      val entitiesModifiedDataset = modifiedDataset.to[entities.Dataset[entities.Dataset.Provenance.Modified]]

      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersions.generateOne,
        projectSchemaVersions.generateOne,
        projectInfo.maybeDescription,
        projectInfo.keywords,
        projectInfo.maybeCreator.map(c => c.toCLIPayloadPerson(c.chooseSomeName)),
        projectInfo.dateCreated,
        datasets =
          dataset.to[entities.Dataset[entities.Dataset.Provenance.ImportedExternal]] :: entitiesModifiedDataset :: Nil
      )

      val Left(error) = jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo)))

      error shouldBe a[DecodingFailure]
      error.getMessage() should endWith(
        s"Dataset ${entitiesModifiedDataset.resourceId} " +
          s"date ${entitiesModifiedDataset.provenance.date} is older than project ${projectInfo.dateCreated}"
      )
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
        datasetEntities(provenanceImportedInternalAncestorInternal())
          .withDateBefore(projectInfo.dateCreated)
          .generateOne
      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersion,
        schemaVersion,
        projectInfo.maybeDescription,
        projectInfo.keywords,
        maybeCreator = None,
        projectInfo.dateCreated,
        datasets = List(dataset1, dataset2, dataset3).map(_.to[entities.Dataset[entities.Dataset.Provenance]])
      )

      jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo))) shouldBe List(
        entities.RenkuProject.WithoutParent(
          resourceId,
          projectInfo.path,
          projectInfo.name,
          projectInfo.maybeDescription,
          cliVersion,
          projectInfo.dateCreated,
          projectInfo.maybeCreator.map(_.toPerson),
          projectInfo.visibility,
          projectInfo.keywords,
          projectInfo.members.map(_.toPerson),
          schemaVersion,
          activities = Nil,
          List(dataset1, dataset2, dataset3).map(_.to[entities.Dataset[entities.Dataset.Provenance]]),
          plans = Nil
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
          projectInfo.keywords,
          projectInfo.maybeCreator.map(c => c.toCLIPayloadPerson(c.chooseSomeName)),
          projectInfo.dateCreated,
          datasets = List(original, modified, broken).map(_.to[entities.Dataset[entities.Dataset.Provenance]])
        )

        val Left(error) = jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo)))

        error shouldBe a[DecodingFailure]
        error.getMessage() should endWith(
          show"Dataset ${broken.identification.identifier} is derived from non-existing dataset ${broken.provenance.derivedFrom}"
        )
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

      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersion,
        schemaVersion,
        projectInfo.maybeDescription,
        projectInfo.keywords,
        maybeCreator = None,
        cliDate
      )

      jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo))) shouldBe List(
        entities.RenkuProject.WithoutParent(
          resourceId,
          projectInfo.path,
          projectInfo.name,
          projectInfo.maybeDescription,
          cliVersion,
          earliestDate,
          projectInfo.maybeCreator.map(_.toPerson),
          projectInfo.visibility,
          projectInfo.keywords,
          projectInfo.members.map(_.toPerson),
          schemaVersion,
          activities = Nil,
          datasets = Nil,
          plans = Nil
        )
      ).asRight
    }

    "favor the CLI description and keywords over the gitlab values" in {
      val gitlabDate   = projectCreatedDates().generateOne
      val cliDate      = projectCreatedDates().generateOne
      val earliestDate = List(gitlabDate, cliDate).min
      val projectInfo = gitLabProjectInfos.generateOne.copy(maybeParentPath = None,
                                                            dateCreated = gitlabDate,
                                                            maybeDescription = projectDescriptions.generateSome,
                                                            keywords = projectKeywords.generateSet(min = 1)
      )
      val description   = projectDescriptions.generateSome
      val keywords      = projectKeywords.generateSet(min = 1)
      val cliVersion    = cliVersions.generateOne
      val schemaVersion = projectSchemaVersions.generateOne
      val resourceId    = projects.ResourceId(projectInfo.path)

      val jsonLD =
        cliLikeJsonLD(resourceId, cliVersion, schemaVersion, description, keywords, maybeCreator = None, cliDate)

      jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo))) shouldBe List(
        entities.RenkuProject.WithoutParent(
          resourceId,
          projectInfo.path,
          projectInfo.name,
          description,
          cliVersion,
          earliestDate,
          projectInfo.maybeCreator.map(_.toPerson),
          projectInfo.visibility,
          keywords,
          projectInfo.members.map(_.toPerson),
          schemaVersion,
          activities = Nil,
          datasets = Nil,
          plans = Nil
        )
      ).asRight
    }

    "fallback to GitLab's description and/or keywords if they are absent in the CLI payload" in {
      val gitlabDate   = projectCreatedDates().generateOne
      val cliDate      = projectCreatedDates().generateOne
      val earliestDate = List(gitlabDate, cliDate).min
      val projectInfo = gitLabProjectInfos.generateOne.copy(maybeParentPath = None,
                                                            dateCreated = gitlabDate,
                                                            maybeDescription = projectDescriptions.generateSome,
                                                            keywords = projectKeywords.generateSet(min = 1)
      )
      val cliVersion    = cliVersions.generateOne
      val schemaVersion = projectSchemaVersions.generateOne
      val resourceId    = projects.ResourceId(projectInfo.path)

      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersion,
        schemaVersion,
        maybeDescription = None,
        keywords = Set.empty,
        maybeCreator = None,
        cliDate
      )

      jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo))) shouldBe List(
        entities.RenkuProject.WithoutParent(
          resourceId,
          projectInfo.path,
          projectInfo.name,
          projectInfo.maybeDescription,
          cliVersion,
          earliestDate,
          projectInfo.maybeCreator.map(_.toPerson),
          projectInfo.visibility,
          projectInfo.keywords,
          projectInfo.members.map(_.toPerson),
          schemaVersion,
          activities = Nil,
          datasets = Nil,
          plans = Nil
        )
      ).asRight
    }

    "return no description and/or keywords if they are absent in both the CLI payload and gitlab" in {
      val gitlabDate   = projectCreatedDates().generateOne
      val cliDate      = projectCreatedDates().generateOne
      val earliestDate = List(gitlabDate, cliDate).min
      val projectInfo = gitLabProjectInfos.generateOne.copy(maybeParentPath = None,
                                                            dateCreated = gitlabDate,
                                                            maybeDescription = projectDescriptions.generateNone,
                                                            keywords = Set.empty
      )
      val cliVersion    = cliVersions.generateOne
      val schemaVersion = projectSchemaVersions.generateOne
      val resourceId    = projects.ResourceId(projectInfo.path)

      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersion,
        schemaVersion,
        maybeDescription = None,
        keywords = Set.empty,
        maybeCreator = None,
        cliDate
      )

      jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo))) shouldBe List(
        entities.RenkuProject.WithoutParent(
          resourceId,
          projectInfo.path,
          projectInfo.name,
          maybeDescription = None,
          cliVersion,
          earliestDate,
          projectInfo.maybeCreator.map(_.toPerson),
          projectInfo.visibility,
          keywords = Set.empty,
          projectInfo.members.map(_.toPerson),
          schemaVersion,
          activities = Nil,
          datasets = Nil,
          plans = Nil
        )
      ).asRight
    }
  }

  "encode for the Default Graph" should {

    implicit val graph: GraphClass = GraphClass.Default

    "produce JsonLD with all the relevant properties of a Renku Project" in {
      forAll(renkuProjectEntitiesWithDatasetsAndActivities.map(_.to[entities.RenkuProject])) { project =>
        val maybeParentId = project match {
          case p: entities.RenkuProject.WithParent => p.parentResourceId.some
          case _ => Option.empty[projects.ResourceId]
        }

        project.asJsonLD.toJson shouldBe JsonLD
          .arr(
            JsonLD.entity(
              EntityId.of(project.resourceId.show),
              entities.Project.entityTypes,
              schema / "name"             -> project.name.asJsonLD,
              renku / "projectPath"       -> project.path.asJsonLD,
              renku / "projectNamespace"  -> project.path.toNamespace.asJsonLD,
              renku / "projectNamespaces" -> project.namespaces.asJsonLD,
              schema / "description"      -> project.maybeDescription.asJsonLD,
              schema / "agent"            -> project.agent.asJsonLD,
              schema / "dateCreated"      -> project.dateCreated.asJsonLD,
              schema / "creator"          -> project.maybeCreator.asJsonLD,
              renku / "projectVisibility" -> project.visibility.asJsonLD,
              schema / "keywords"         -> project.keywords.asJsonLD,
              schema / "member"           -> project.members.toList.asJsonLD,
              schema / "schemaVersion"    -> project.version.asJsonLD,
              renku / "hasActivity"       -> project.activities.asJsonLD,
              renku / "hasPlan"           -> project.plans.asJsonLD,
              renku / "hasDataset"        -> project.datasets.asJsonLD,
              prov / "wasDerivedFrom"     -> maybeParentId.map(_.asEntityId).asJsonLD
            ) :: project.datasets.flatMap(_.publicationEvents.map(_.asJsonLD)): _*
          )
          .toJson
      }
    }

    "produce JsonLD with all the relevant properties or a non-Renku Project" in {
      forAll(anyNonRenkuProjectEntities.map(_.to[entities.NonRenkuProject])) { project =>
        val maybeParentId = project match {
          case p: entities.NonRenkuProject.WithParent => p.parentResourceId.some
          case _ => Option.empty[projects.ResourceId]
        }

        project.asJsonLD.toJson shouldBe JsonLD
          .arr(
            JsonLD.entity(
              EntityId.of(project.resourceId.show),
              entities.Project.entityTypes,
              schema / "name"             -> project.name.asJsonLD,
              renku / "projectPath"       -> project.path.asJsonLD,
              renku / "projectNamespace"  -> project.path.toNamespace.asJsonLD,
              renku / "projectNamespaces" -> project.namespaces.asJsonLD,
              schema / "description"      -> project.maybeDescription.asJsonLD,
              schema / "dateCreated"      -> project.dateCreated.asJsonLD,
              schema / "creator"          -> project.maybeCreator.asJsonLD,
              renku / "projectVisibility" -> project.visibility.asJsonLD,
              schema / "keywords"         -> project.keywords.asJsonLD,
              schema / "member"           -> project.members.toList.asJsonLD,
              prov / "wasDerivedFrom"     -> maybeParentId.map(_.asEntityId).asJsonLD
            )
          )
          .toJson
      }
    }
  }

  "encode for the Project Graph" should {

    import persons.ResourceId.entityIdEncoder
    implicit val graph: GraphClass = GraphClass.Project

    "produce JsonLD with all the relevant properties and only links to Person entities" in {
      forAll(
        renkuProjectEntitiesWithDatasetsAndActivities
          .modify(replaceMembers(personEntities(withoutGitLabId).generateFixedSizeSet(ofSize = 1)))
          .map(_.to[entities.RenkuProject])
      ) { project =>
        val maybeParentId = project match {
          case p: entities.RenkuProject.WithParent => p.parentResourceId.some
          case _ => Option.empty[projects.ResourceId]
        }

        project.asJsonLD.toJson shouldBe JsonLD
          .arr(
            JsonLD.entity(
              EntityId.of(project.resourceId.show),
              entities.Project.entityTypes,
              schema / "name"             -> project.name.asJsonLD,
              renku / "projectPath"       -> project.path.asJsonLD,
              renku / "projectNamespace"  -> project.path.toNamespace.asJsonLD,
              renku / "projectNamespaces" -> project.namespaces.asJsonLD,
              schema / "description"      -> project.maybeDescription.asJsonLD,
              schema / "agent"            -> project.agent.asJsonLD,
              schema / "dateCreated"      -> project.dateCreated.asJsonLD,
              schema / "creator"          -> project.maybeCreator.map(_.resourceId.asEntityId).asJsonLD,
              renku / "projectVisibility" -> project.visibility.asJsonLD,
              schema / "keywords"         -> project.keywords.asJsonLD,
              schema / "member"           -> project.members.map(_.resourceId.asEntityId).toList.asJsonLD,
              schema / "schemaVersion"    -> project.version.asJsonLD,
              renku / "hasActivity"       -> project.activities.asJsonLD,
              renku / "hasPlan"           -> project.plans.asJsonLD,
              renku / "hasDataset"        -> project.datasets.asJsonLD,
              prov / "wasDerivedFrom"     -> maybeParentId.map(_.asEntityId).asJsonLD
            ) :: project.datasets.flatMap(_.publicationEvents.map(_.asJsonLD)): _*
          )
          .toJson
      }
    }

    "produce JsonLD with all the relevant properties or a non-Renku Project" in {
      forAll(
        anyNonRenkuProjectEntities
          .modify(replaceMembers(personEntities(withoutGitLabId).generateFixedSizeSet(ofSize = 1)))
          .map(_.to[entities.NonRenkuProject])
      ) { project =>
        val maybeParentId = project match {
          case p: entities.NonRenkuProject.WithParent => p.parentResourceId.some
          case _ => Option.empty[projects.ResourceId]
        }

        project.asJsonLD.toJson shouldBe JsonLD
          .arr(
            JsonLD.entity(
              EntityId.of(project.resourceId.show),
              entities.Project.entityTypes,
              schema / "name"             -> project.name.asJsonLD,
              renku / "projectPath"       -> project.path.asJsonLD,
              renku / "projectNamespace"  -> project.path.toNamespace.asJsonLD,
              renku / "projectNamespaces" -> project.namespaces.asJsonLD,
              schema / "description"      -> project.maybeDescription.asJsonLD,
              schema / "dateCreated"      -> project.dateCreated.asJsonLD,
              schema / "creator"          -> project.maybeCreator.map(_.resourceId.asEntityId).asJsonLD,
              renku / "projectVisibility" -> project.visibility.asJsonLD,
              schema / "keywords"         -> project.keywords.asJsonLD,
              schema / "member"           -> project.members.map(_.resourceId.asEntityId).toList.asJsonLD,
              prov / "wasDerivedFrom"     -> maybeParentId.map(_.asEntityId).asJsonLD
            )
          )
          .toJson
      }
    }
  }

  "entityFunctions.findAllPersons" should {

    "return Project's creator, members, activities' authors and datasets' creators" in {

      val project = renkuProjectEntitiesWithDatasetsAndActivities.generateOne.to[entities.RenkuProject]

      EntityFunctions[entities.Project].findAllPersons(project) shouldBe
        project.maybeCreator.toSet ++
        project.members ++
        project.activities.flatMap(EntityFunctions[entities.Activity].findAllPersons).toSet ++
        project.datasets.flatMap(EntityFunctions[entities.Dataset[entities.Dataset.Provenance]].findAllPersons).toSet ++
        project.plans.flatMap(EntityFunctions[entities.Plan].findAllPersons).toSet
    }
  }

  "entityFunctions.encoder" should {

    "return encoder that honors the given GraphClass" in {

      val project = anyRenkuProjectEntities.generateOne.to[entities.Project]

      implicit val graph: GraphClass = graphClasses.generateOne
      val functionsEncoder = EntityFunctions[entities.Project].encoder(graph)

      project.asJsonLD(functionsEncoder) shouldBe project.asJsonLD
    }
  }

  private def cliLikeJsonLD(resourceId:       projects.ResourceId,
                            cliVersion:       CliVersion,
                            schemaVersion:    SchemaVersion,
                            maybeDescription: Option[Description],
                            keywords:         Set[Keyword],
                            maybeCreator:     Option[entities.Person],
                            dateCreated:      DateCreated,
                            activities:       List[entities.Activity] = Nil,
                            datasets:         List[entities.Dataset[entities.Dataset.Provenance]] = Nil,
                            plans:            List[entities.Plan] = Nil
  )(implicit graph:                           GraphClass): JsonLD = {

    val descriptionJsonLD = maybeDescription match {
      case Some(desc) => desc.asJsonLD
      case None =>
        if (Random.nextBoolean()) blankStrings().generateOne.asJsonLD
        else maybeDescription.asJsonLD
    }
    JsonLD
      .arr(
        JsonLD.entity(
          resourceId.asEntityId,
          EntityTypes of (prov / "Location", schema / "Project"),
          schema / "agent"         -> cliVersion.asJsonLD,
          schema / "schemaVersion" -> schemaVersion.asJsonLD,
          schema / "description"   -> descriptionJsonLD,
          schema / "keywords"      -> (keywords.map(_.value) + blankStrings().generateOne).asJsonLD,
          schema / "creator"       -> maybeCreator.asJsonLD,
          schema / "dateCreated"   -> dateCreated.asJsonLD,
          renku / "hasActivity"    -> activities.asJsonLD,
          renku / "hasDataset"     -> datasets.asJsonLD,
          renku / "hasPlan"        -> plans.asJsonLD
        ) :: datasets.flatMap(_.publicationEvents.map(_.asJsonLD)): _*
      )
      .flatten
      .fold(throw _, identity)
  }

  private def minimalCliLikeJsonLD(resourceId: projects.ResourceId) =
    JsonLD
      .entity(
        resourceId.asEntityId,
        EntityTypes.of(prov / "Location", schema / "Project"),
        Map.empty[Property, JsonLD]
      )
      .flatten
      .fold(throw _, identity)

  private implicit class ProjectMemberOps(gitLabPerson: ProjectMember) {

    def toCLIPayloadPerson(name: Name): entities.Person = gitLabPerson match {
      case _: ProjectMemberNoEmail =>
        personEntities.generateOne
          .copy(
            name = name,
            maybeEmail = None,
            maybeGitLabId = None
          )
          .to[entities.Person]
      case member: ProjectMemberWithEmail =>
        personEntities.generateOne
          .copy(
            name = name,
            maybeEmail = member.email.some,
            maybeGitLabId = None
          )
          .to[entities.Person]
    }

    lazy val toPerson: entities.Person = gitLabPerson match {
      case ProjectMemberNoEmail(name, _, gitLabId) =>
        entities.Person.WithGitLabId(persons.ResourceId(gitLabId),
                                     gitLabId,
                                     name,
                                     maybeEmail = None,
                                     maybeOrcidId = None,
                                     maybeAffiliation = None
        )
      case ProjectMemberWithEmail(name, _, gitLabId, email) =>
        entities.Person.WithGitLabId(persons.ResourceId(gitLabId),
                                     gitLabId,
                                     name,
                                     email.some,
                                     maybeOrcidId = None,
                                     maybeAffiliation = None
        )
    }

    def chooseSomeName =
      if (Random.nextBoolean()) gitLabPerson.name
      else persons.Name(gitLabPerson.username.value)
  }

  private def activityWith(author: entities.Person): projects.DateCreated => (entities.Activity, entities.StepPlan) =
    dateCreated => {
      val activity = activityEntities(stepPlanEntities().modify(_.removeCreators()))(dateCreated).generateOne
      activity.to[entities.Activity].copy(author = author) -> activity.plan.to[entities.StepPlan]
    }

  private def activityWithAssociationAgent(
      agent: entities.Person
  ): projects.DateCreated => (entities.Activity, entities.StepPlan) =
    dateCreated => {
      val activity         = activityEntities(stepPlanEntities().modify(_.removeCreators()))(dateCreated).generateOne
      val entitiesActivity = activity.to[entities.Activity]
      val entitiesPlan     = activity.plan.to[entities.StepPlan]
      entitiesActivity.copy(association =
        entities.Association.WithPersonAgent(entitiesActivity.association.resourceId, agent, entitiesPlan.resourceId)
      ) -> entitiesPlan
    }

  private def datasetWith(
      creators: NonEmptyList[entities.Person]
  ): projects.DateCreated => entities.Dataset[entities.Dataset.Provenance] = dateCreated => {
    val ds = datasetEntities(provenanceNonModified)(renkuUrl)(dateCreated).generateOne
      .to[entities.Dataset[entities.Dataset.Provenance]]
    addTo(ds, creators)
  }

  private def addTo(
      dataset:  entities.Dataset[entities.Dataset.Provenance],
      creators: NonEmptyList[entities.Person]
  ): entities.Dataset[entities.Dataset.Provenance] =
    dataset.copy(provenance = dataset.provenance match {
      case p: entities.Dataset.Provenance.Internal                         => p.copy(creators = creators.sortBy(_.name))
      case p: entities.Dataset.Provenance.ImportedExternal                 => p.copy(creators = creators.sortBy(_.name))
      case p: entities.Dataset.Provenance.ImportedInternalAncestorInternal => p.copy(creators = creators.sortBy(_.name))
      case p: entities.Dataset.Provenance.ImportedInternalAncestorExternal => p.copy(creators = creators.sortBy(_.name))
      case p: entities.Dataset.Provenance.Modified                         => p.copy(creators = creators.sortBy(_.name))
    })

  private def replaceAgent(activity: entities.Activity, newAgent: entities.Person): entities.Activity =
    ActivityLens.activityAssociationAgent.modify(_.map(_ => newAgent))(activity)

  private def byEmail(member: ProjectMemberWithEmail): entities.Person => Boolean =
    _.maybeEmail.contains(member.email)

  private def merge(person: entities.Person, member: ProjectMemberWithEmail): entities.Person =
    person
      .add(member.gitLabId)
      .copy(name = member.name, maybeEmail = member.email.some)
}
