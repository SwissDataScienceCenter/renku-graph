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

package io.renku.graph.model.entities

import PlanLens._
import cats.data.{NonEmptyList, ValidatedNel}
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.Schemas.{prov, renku, schema}
import io.renku.graph.model._
import io.renku.graph.model.entities.Generators.{compositePlanNonEmptyMappings, stepPlanGenFactory}
import io.renku.graph.model.entities.Project.ProjectMember.{ProjectMemberNoEmail, ProjectMemberWithEmail}
import io.renku.graph.model.entities.Project.{GitLabProjectInfo, ProjectMember}
import io.renku.graph.model.persons.Name
import io.renku.graph.model.projects.{DateCreated, Description, Keyword}
import io.renku.graph.model.testentities.RenkuProject.CreateCompositePlan
import io.renku.graph.model.testentities._
import io.renku.graph.model.testentities.generators.EntitiesGenerators.ProjectBasedGenFactoryOps
import io.renku.graph.model.testentities.generators.genMonad
import io.renku.jsonld.JsonLDDecoder._
import io.renku.jsonld.JsonLDEncoder.encodeOption
import io.renku.jsonld._
import io.renku.jsonld.syntax._
import monocle.Lens
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

    "turn JsonLD Project entity without parent into the Project object" in new TestCase {
      forAll(gitLabProjectInfos.map(projectInfoMaybeParent.set(None))) { projectInfo =>
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
            plan1 :: plan2 :: plan3 :: Nil,
            convertImageUris(resourceId.asEntityId)(info.avatarUrl.toList)
          )
        ).asRight
      }
    }

    "turn JsonLD Project entity with parent into the Project object" in new TestCase {
      forAll(gitLabProjectInfos.map(projectInfoMaybeParent.set(projectPaths.generateSome))) { projectInfo =>
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
          maybeCreator = None,
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
            projects.ResourceId(info.maybeParentPath.getOrElse(fail("No parent project"))),
            convertImageUris(resourceId.asEntityId)(info.avatarUrl.toList)
          )
        ).asRight
      }
    }

    "turn non-renku JsonLD Project entity without parent into the NonRenkuProject object" in {
      forAll(gitLabProjectInfos.map(projectInfoMaybeParent.set(None))) { projectInfo =>
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
            members.map(_.toPerson),
            convertImageUris(resourceId.asEntityId)(info.avatarUrl.toList)
          )
        ).asRight
      }
    }

    "turn non-renku JsonLD Project entity with parent into the NonRenkuProject object" in {
      forAll(gitLabProjectInfos.map(projectInfoMaybeParent.set(projectPaths.generateSome))) { projectInfo =>
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
            projects.ResourceId(info.maybeParentPath.getOrElse(fail("No parent project"))),
            convertImageUris(resourceId.asEntityId)(info.avatarUrl.toList)
          )
        ).asRight
      }
    }

    forAll {
      Table(
        "Project type"   -> "Project Info",
        "without parent" -> gitLabProjectInfos.map(projectInfoMaybeParent.set(None)).generateOne,
        "with parent"    -> gitLabProjectInfos.map(projectInfoMaybeParent.set(projectPaths.generateSome)).generateOne
      )
    } { (projectType, info) =>
      s"match persons in plan.creators for project $projectType" in new TestCase {

        val creator = projectMembersWithEmail.generateOne
        val member2 = projectMembersWithEmail.generateOne

        val projectInfo        = info.copy(maybeCreator = creator.some, members = Set(member2))
        val resourceId         = projects.ResourceId(projectInfo.path)
        val creatorAsCliPerson = creator.toCLIPayloadPerson(creator.chooseSomeName)
        val (activity, plan) = activityWith(member2.toCLIPayloadPerson(member2.chooseSomeName))(projectInfo.dateCreated)
          .bimap(identity, PlanLens.planCreators.set(List(creatorAsCliPerson)))

        val jsonLD = cliLikeJsonLD(
          resourceId,
          cliVersion,
          schemaVersion,
          projectInfo.maybeDescription,
          projectInfo.keywords,
          creatorAsCliPerson.some,
          projectInfo.dateCreated,
          activities = activity :: Nil,
          plans = plan :: Nil
        )

        val mergedCreator = merge(creatorAsCliPerson, creator)
        val mergedMember2 = merge(activity.author, member2)

        val Right(actual :: Nil) = jsonLD.cursor.as(decodeList(entities.Project.decoder(projectInfo)))

        actual.maybeCreator shouldBe mergedCreator.some
        actual.members      shouldBe Set(mergedMember2)
        actual.activities   shouldBe ActivityLens.activityAuthor.set(mergedMember2)(activity) :: Nil
        actual.plans        shouldBe PlanLens.planCreators.set(List(mergedCreator))(plan) :: Nil
      }

      s"update Plans' originalResourceId for project $projectType" in new TestCase {

        val resourceId                = projects.ResourceId(info.path)
        val activity                  = activityEntities(stepPlanEntities())(info.dateCreated).generateOne
        val plan                      = activity.plan
        val entitiesPlan              = plan.to[entities.Plan]
        val planModification1         = plan.createModification()
        val entitiesPlanModification1 = planModification1.to[entities.StepPlan.Modified]
        val planModification2         = planModification1.createModification()
        val entitiesPlanModification2 = planModification2.to[entities.StepPlan.Modified]

        val jsonLD = cliLikeJsonLD(
          resourceId,
          cliVersion,
          schemaVersion,
          info.maybeDescription,
          info.keywords,
          maybeCreator = None,
          info.dateCreated,
          activities = activity.to[entities.Activity] :: Nil,
          plans = entitiesPlan :: entitiesPlanModification1 :: entitiesPlanModification2 :: Nil
        )

        val Right(actual :: Nil) = jsonLD.cursor.as(decodeList(entities.Project.decoder(info)))

        val actualPlan1 :: actualPlan2 :: actualPlan3 :: Nil = actual.plans
        actualPlan1 shouldBe entitiesPlan
        actualPlan2 shouldBe entitiesPlanModification1
        actualPlan3 shouldBe (modifiedPlanDerivation >>> planDerivationOriginalId)
          .set(entitiesPlan.resourceId)(entitiesPlanModification2)
      }

      s"update Plans' dateCreated if there are Activities created before the Plan for project $projectType" in new TestCase {

        val resourceId = projects.ResourceId(info.path)
        val activity = {
          val a = activityEntities(stepPlanEntities())(info.dateCreated).generateOne
          a.replaceStartTime(
            timestamps(min = info.dateCreated.value, max = a.plan.dateCreated.value.minusSeconds(1))
              .generateAs(activities.StartTime)
          )
        }
        val entitiesActivity = activity.to[entities.Activity]
        val plan             = activity.plan
        val entitiesPlan     = plan.to[entities.Plan]

        val jsonLD = cliLikeJsonLD(
          resourceId,
          cliVersion,
          schemaVersion,
          info.maybeDescription,
          info.keywords,
          maybeCreator = None,
          info.dateCreated,
          activities = entitiesActivity :: Nil,
          plans = entitiesPlan :: Nil
        )

        val Right(actual :: Nil) = jsonLD.cursor.as(decodeList(entities.Project.decoder(info)))

        actual.plans shouldBe List(
          planDateCreated.set(plans.DateCreated(entitiesActivity.startTime.value))(entitiesPlan)
        )
        actual.activities shouldBe List(entitiesActivity)
      }
    }

    "update plans original id across multiple links" in {
      val info    = gitLabProjectInfos.generateOne
      val topPlan = stepPlanEntities().apply(info.dateCreated).generateOne
      val plan1   = topPlan.createModification()
      val plan2   = plan1.createModification()
      val plan3   = plan2.createModification()
      val plan4   = plan3.createModification()

      val realPlans = List(topPlan, plan1, plan2, plan3, plan4).map(_.to[entities.Plan])

      val update = new (List[entities.Plan] => ValidatedNel[String, List[entities.Plan]])
        with entities.RenkuProject.ProjectFactory {
        def apply(plans: List[entities.Plan]): ValidatedNel[String, List[entities.Plan]] =
          this.updatePlansOriginalId(plans)
      }

      val updatedPlans = update(realPlans)
        .fold(msgs => fail(s"updateOriginalIds failed: $msgs"), identity)
        .groupBy(_.resourceId)
        .view
        .mapValues(_.head)
        .toMap

      realPlans.tail.foreach { plan =>
        val updatedPlan = updatedPlans(plan.resourceId)
        val derivation  = PlanLens.getPlanDerivation.get(updatedPlan).get
        derivation.originalResourceId.value shouldBe realPlans.head.resourceId.value
      }
    }

    "validate composite plans in a project failing to find referenced entities" in {
      val validate = new (List[entities.Plan] => ValidatedNel[String, Unit]) with entities.RenkuProject.ProjectFactory {
        def apply(plans: List[entities.Plan]): ValidatedNel[String, Unit] =
          this.validateCompositePlanData(plans)
      }

      val invalidPlanGen =
        for {
          cp <- compositePlanNonEmptyMappings
                  .mapF(
                    _.map(_.asInstanceOf[CompositePlan.NonModified])
                  )
          step <- stepPlanGenFactory
        } yield cp.copy(plans = NonEmptyList.one(step))

      val cp = invalidPlanGen.generateOne.to[entities.CompositePlan]

      validate(List(cp)).fold(_.toList.toSet, _ => Set.empty) shouldBe (
        cp.plans.map(_.value).map(id => show"The subprocess plan $id is missing in the project.").toList.toSet ++
          cp.links.map(_.source).map(id => show"The source $id is not available in the set of plans.") ++
          cp.links.flatMap(_.sinks.toList).map(id => show"The sink $id is not available in the set of plans") ++
          cp.mappings
            .flatMap(_.mappedParameter.toList)
            .map(id => show"ParameterMapping '$id' does not exist in the set of plans.")
      )
    }

    "validate a correct composite plan in a project" in {
      val validate = new (List[entities.Plan] => ValidatedNel[String, Unit]) with entities.RenkuProject.ProjectFactory {
        def apply(plans: List[entities.Plan]): ValidatedNel[String, Unit] =
          this.validateCompositePlanData(plans)
      }
      val projectGen = renkuProjectEntitiesWithDatasetsAndActivities
        .map(_.addCompositePlan(CreateCompositePlan(compositePlanEntities)))

      forAll(projectGen) { project =>
        validate(project.to[entities.Project].plans)
          .leftMap(_.toList.intercalate("; "))
          .fold(fail(_), identity)

        val decoded = project.asJsonLD.flatten
          .fold(fail(_), identity)
          .cursor
          .as[List[entities.CompositePlan]]

        decoded shouldBe Right(project.plans.filter(_.isInstanceOf[CompositePlan]).map(_.to[entities.Plan]))
      }
    }

    "validate a composite plan that has references outside its children" in {
      val testPlan = compositePlanNonEmptyMappings.generateOne
        .asInstanceOf[CompositePlan.NonModified]

      // find a plan belonging to some mapped parameter
      val mappedPlanId = testPlan.mappings.head.mappedParam.head.planId

      // remove this plan from the children plan list
      val invalidPlan =
        testPlan.copy(plans = NonEmptyList.fromListUnsafe(testPlan.plans.filterNot(_.id == mappedPlanId)))

      // for convenience decode it all into a list
      val decodeAll = invalidPlan.asJsonLD.flatten.fold(fail(_), identity).cursor.as[List[entities.Plan]]

      val validate = new (List[entities.Plan] => ValidatedNel[String, Unit]) with entities.RenkuProject.ProjectFactory {
        def apply(plans: List[entities.Plan]): ValidatedNel[String, Unit] =
          this.validateCompositePlanData(plans)
      }

      validate(decodeAll.toOption.get).isInvalid shouldBe true
    }

    "return a DecodingFailure when there's a Person entity that cannot be decoded" in new TestCase {

      val projectInfo = gitLabProjectInfos.map(projectInfoMaybeParent.set(None)).generateOne
      val resourceId  = projects.ResourceId(projectInfo.path)
      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersion,
        schemaVersion,
        projectInfo.maybeDescription,
        projectInfo.keywords,
        maybeCreator = None,
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

    "return a DecodingFailure if there's a modified Plan pointing to a non-existing parent" in new TestCase {

      val info       = gitLabProjectInfos.generateOne
      val resourceId = projects.ResourceId(info.path)
      val activity   = activityEntities(stepPlanEntities())(info.dateCreated).generateOne
      val (plan, planModification) = {
        val p = activity.plan
        val modification = p
          .createModification()
          .to[entities.StepPlan.Modified]
          .copy(derivation =
            entities.Plan.Derivation(plans.DerivedFrom(planResourceIds.generateOne.value), planResourceIds.generateOne)
          )
        p.to[entities.Plan] -> modification
      }

      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersion,
        schemaVersion,
        info.maybeDescription,
        info.keywords,
        maybeCreator = None,
        info.dateCreated,
        activities = activity.to[entities.Activity] :: Nil,
        plans = plan :: planModification :: Nil
      )

      val Left(error) = jsonLD.cursor.as(decodeList(entities.Project.decoder(info)))

      error            shouldBe a[DecodingFailure]
      error.getMessage() should include(s"Cannot find parent plan ${planModification.derivation.derivedFrom}")
    }

    "return a DecodingFailure if there's a modified Plan with the date from before the parent date" in new TestCase {

      val info       = gitLabProjectInfos.generateOne
      val resourceId = projects.ResourceId(info.path)
      val activity   = activityEntities(stepPlanEntities())(info.dateCreated).generateOne
      val (plan, planModification) = {
        val p = activity.plan
        val modification = p
          .createModification()
          .to[entities.StepPlan.Modified]
          .copy(dateCreated =
            timestamps(info.dateCreated.value, p.dateCreated.value.minusSeconds(1)).generateAs(plans.DateCreated)
          )
        p.to[entities.Plan] -> modification
      }

      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersion,
        schemaVersion,
        info.maybeDescription,
        info.keywords,
        maybeCreator = None,
        info.dateCreated,
        activities = activity.to[entities.Activity] :: Nil,
        plans = plan :: planModification :: Nil
      )

      val Left(error) = jsonLD.cursor.as(decodeList(entities.Project.decoder(info)))

      error shouldBe a[DecodingFailure]
      error.getMessage() should include(
        show"Plan ${planModification.resourceId} is older than it's parent ${planModification.derivation.derivedFrom}"
      )
    }

    "return a DecodingFailure when there's a Dataset entity that cannot be decoded" in new TestCase {
      val projectInfo = gitLabProjectInfos.map(projectInfoMaybeParent.set(None)).generateOne
      val resourceId  = projects.ResourceId(projectInfo.path)
      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersion,
        schemaVersion,
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

    "return a DecodingFailure when there's an Activity entity created before project creation" in new TestCase {
      val projectInfo       = gitLabProjectInfos.map(projectInfoMaybeParent.set(None)).generateOne
      val resourceId        = projects.ResourceId(projectInfo.path)
      val dateBeforeProject = timestamps(max = projectInfo.dateCreated.value.minusSeconds(1)).generateOne
      val activity = activityEntities(
        stepPlanEntities().map(_.replacePlanDateCreated(plans.DateCreated(dateBeforeProject)))
      ).map(
        _.replaceStartTime(
          timestamps(min = dateBeforeProject, max = projectInfo.dateCreated.value).generateAs[activities.StartTime]
        )
      ).run(projects.DateCreated(dateBeforeProject))
        .generateOne
      val entitiesActivity = activity.to[entities.Activity]
      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersion,
        schemaVersion,
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

    "return a DecodingFailure when there's an internal Dataset entity created before project without parent" in new TestCase {
      val projectInfo     = gitLabProjectInfos.map(projectInfoMaybeParent.set(None)).generateOne
      val resourceId      = projects.ResourceId(projectInfo.path)
      val dataset         = datasetEntities(provenanceInternal).withDateBefore(projectInfo.dateCreated).generateOne
      val entitiesDataset = dataset.to[entities.Dataset[entities.Dataset.Provenance.Internal]]
      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersion,
        schemaVersion,
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

    "return a DecodingFailure when there's a Plan entity created before project without parent" in new TestCase {
      val projectInfo = gitLabProjectInfos.map(projectInfoMaybeParent.set(None)).generateOne
      val resourceId  = projects.ResourceId(projectInfo.path)
      val plan = stepPlanEntities()(planCommands)(projectInfo.dateCreated).generateOne
        .replacePlanDateCreated(timestamps(max = projectInfo.dateCreated.value).generateAs[plans.DateCreated])
      val entitiesPlan = plan.to[entities.Plan]
      val jsonLD = cliLikeJsonLD(
        resourceId,
        cliVersion,
        schemaVersion,
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

    "decode project when there's an internal or modified Dataset entity created before project with parent" in new TestCase {
      val parentPath  = projectPaths.generateOne
      val projectInfo = gitLabProjectInfos.map(projectInfoMaybeParent.set(parentPath.some)).generateOne
      val resourceId  = projects.ResourceId(projectInfo.path)
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
          projects.ResourceId(parentPath),
          convertImageUris(resourceId.asEntityId)(projectInfo.avatarUrl.toList)
        )
      ).asRight
    }

    "return a DecodingFailure when there's a modified Dataset entity created before project without parent" in new TestCase {
      val projectInfo = gitLabProjectInfos.map(projectInfoMaybeParent.set(None)).generateOne
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
        cliVersion,
        schemaVersion,
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

    "decode project when there's a Dataset (neither internal nor modified) created before project creation" in new TestCase {
      val projectInfo = gitLabProjectInfos.map(projectInfoMaybeParent.set(None)).generateOne
      val resourceId  = projects.ResourceId(projectInfo.path)
      val dataset1    = datasetEntities(provenanceImportedExternal).withDateBefore(projectInfo.dateCreated).generateOne
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
          plans = Nil,
          convertImageUris(resourceId.asEntityId)(projectInfo.avatarUrl.toList)
        )
      ).asRight
    }

    "return a DecodingFailure when there's a modified Dataset that is derived from a non-existing dataset" in new TestCase {
      Set(
        gitLabProjectInfos.map(projectInfoMaybeParent.set(None)).generateOne,
        gitLabProjectInfos.map(projectInfoMaybeParent.set(projectPaths.generateSome)).generateOne
      ) foreach { projectInfo =>
        val resourceId = projects.ResourceId(projectInfo.path)
        val (original, modified) =
          datasetAndModificationEntities(provenanceInternal, projectInfo.dateCreated).generateOne
        val (_, broken) = datasetAndModificationEntities(provenanceInternal, projectInfo.dateCreated).generateOne

        val jsonLD = cliLikeJsonLD(
          resourceId,
          cliVersion,
          schemaVersion,
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

    "pick the earliest from dateCreated found in gitlabProjectInfo and the CLI" in new TestCase {
      val gitlabDate   = projectCreatedDates().generateOne
      val cliDate      = projectCreatedDates().generateOne
      val earliestDate = List(gitlabDate, cliDate).min
      val projectInfo  = gitLabProjectInfos.map(_.copy(maybeParentPath = None, dateCreated = gitlabDate)).generateOne
      val resourceId   = projects.ResourceId(projectInfo.path)

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
          plans = Nil,
          convertImageUris(resourceId.asEntityId)(projectInfo.avatarUrl.toList)
        )
      ).asRight
    }

    "favor the CLI description and keywords over the gitlab values" in new TestCase {
      val gitlabDate   = projectCreatedDates().generateOne
      val cliDate      = projectCreatedDates().generateOne
      val earliestDate = List(gitlabDate, cliDate).min
      val projectInfo = gitLabProjectInfos.generateOne.copy(maybeParentPath = None,
                                                            dateCreated = gitlabDate,
                                                            maybeDescription = projectDescriptions.generateSome,
                                                            keywords = projectKeywords.generateSet(min = 1)
      )
      val description = projectDescriptions.generateSome
      val keywords    = projectKeywords.generateSet(min = 1)
      val resourceId  = projects.ResourceId(projectInfo.path)

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
          plans = Nil,
          convertImageUris(resourceId.asEntityId)(projectInfo.avatarUrl.toList)
        )
      ).asRight
    }

    "fallback to GitLab's description and/or keywords if they are absent in the CLI payload" in new TestCase {
      val gitlabDate   = projectCreatedDates().generateOne
      val cliDate      = projectCreatedDates().generateOne
      val earliestDate = List(gitlabDate, cliDate).min
      val projectInfo = gitLabProjectInfos.generateOne.copy(
        maybeParentPath = None,
        dateCreated = gitlabDate,
        maybeDescription = projectDescriptions.generateSome,
        keywords = projectKeywords.generateSet(min = 1)
      )
      val resourceId = projects.ResourceId(projectInfo.path)

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
          plans = Nil,
          convertImageUris(resourceId.asEntityId)(projectInfo.avatarUrl.toList)
        )
      ).asRight
    }

    "return no description and/or keywords if they are absent in both the CLI payload and gitlab" in new TestCase {
      val gitlabDate   = projectCreatedDates().generateOne
      val cliDate      = projectCreatedDates().generateOne
      val earliestDate = List(gitlabDate, cliDate).min
      val projectInfo = gitLabProjectInfos.generateOne.copy(maybeParentPath = None,
                                                            dateCreated = gitlabDate,
                                                            maybeDescription = projectDescriptions.generateNone,
                                                            keywords = Set.empty
      )
      val resourceId = projects.ResourceId(projectInfo.path)

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
          plans = Nil,
          convertImageUris(resourceId.asEntityId)(projectInfo.avatarUrl.toList)
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

  private trait TestCase {
    val cliVersion    = cliVersions.generateOne
    val schemaVersion = projectSchemaVersions.generateOne
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
      val activity = activityEntities(stepPlanEntities().map(_.removeCreators()))(dateCreated).generateOne
      activity.to[entities.Activity].copy(author = author) -> activity.plan.to[entities.StepPlan]
    }

  private def activityWithAssociationAgent(
      agent: entities.Person
  ): projects.DateCreated => (entities.Activity, entities.StepPlan) =
    dateCreated => {
      val activity         = activityEntities(stepPlanEntities().map(_.removeCreators()))(dateCreated).generateOne
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

  private lazy val projectInfoMaybeParent: Lens[GitLabProjectInfo, Option[projects.Path]] =
    Lens[GitLabProjectInfo, Option[projects.Path]](_.maybeParentPath)(mpp => _.copy(maybeParentPath = mpp))

  private lazy val modifiedPlanDerivation: Lens[entities.StepPlan.Modified, entities.Plan.Derivation] =
    Lens[entities.StepPlan.Modified, entities.Plan.Derivation](_.derivation)(d => _.copy(derivation = d))

  private lazy val planDerivationOriginalId: Lens[entities.Plan.Derivation, plans.ResourceId] =
    Lens[entities.Plan.Derivation, plans.ResourceId](_.originalResourceId)(id => _.copy(originalResourceId = id))
}
