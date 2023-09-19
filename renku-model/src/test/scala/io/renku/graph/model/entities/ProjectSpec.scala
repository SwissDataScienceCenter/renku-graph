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

import cats.data.{NonEmptyList, ValidatedNel}
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.syntax._
import io.renku.cli.model.CliModel._
import io.renku.cli.model.CliPlan.{allMappingParameterIds, allStepParameterIds}
import io.renku.cli.model._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.Schemas.{prov, renku, schema}
import io.renku.graph.model._
import io.renku.graph.model.entities.Generators.{compositePlanNonEmptyMappings, stepPlanGenFactory}
import io.renku.graph.model.gitlab.{GitLabMember, GitLabProjectInfo, GitLabUser}
import io.renku.graph.model.images.Image
import io.renku.graph.model.projects.ForksCount
import io.renku.graph.model.testentities.RenkuProject.CreateCompositePlan
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.testentities.generators.EntitiesGenerators.ProjectBasedGenFactoryOps
import io.renku.graph.model.testentities.{CompositePlan, ModelOps}
import io.renku.graph.model.tools.AdditionalMatchers
import io.renku.graph.model.versions.{CliVersion, SchemaVersion}
import io.renku.jsonld.JsonLDDecoder._
import io.renku.jsonld.JsonLDEncoder.encodeOption
import io.renku.jsonld._
import io.renku.jsonld.syntax._
import monocle.Lens
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.{LocalDate, ZoneOffset}

class ProjectSpec
    extends AnyWordSpec
    with should.Matchers
    with EntitiesGenerators
    with ModelOps
    with ScalaCheckPropertyChecks
    with AdditionalMatchers
    with EitherValues
    with DiffInstances {

  "ProjectMember.add" should {

    "add the given email to the Project without an email" in {
      val member = projectMembersNoEmail.generateOne
      val email  = personEmails.generateOne

      (member withEmail email) shouldBe GitLabMember(
        member.user.name,
        member.user.username,
        member.user.gitLabId,
        email.some,
        member.accessLevel
      )
    }
  }

  "fromCli" should {

    "add images from gitlab project avatar" in new TestCase {
      val projectInfo =
        gitLabProjectInfos.map(projectInfoMaybeParent.replace(None)).suchThat(_.avatarUrl.isDefined).generateOne
      val testProject: testentities.Project =
        createRenkuProject(projectInfo, cliVersion, schemaVersion)
          .asInstanceOf[testentities.RenkuProject.WithoutParent]
          .copy(images = Nil)

      val cliProject = testProject.to[CliProject]
      cliProject.images shouldBe Nil

      val modelProject = entities.Project
        .fromCli(cliProject, Set.empty, projectInfo)
        .toEither
        .fold(errs => sys.error(errs.toString()), identity)

      modelProject.images shouldBe List(
        Image.projectImage(modelProject.resourceId, projectInfo.avatarUrl.get)
      )
    }

    "turn CliProject entity without parent into the Project object" in new TestCase {
      forAll(gitLabProjectInfos.map(projectInfoMaybeParent.replace(None))) { projectInfo =>
        val creator = projectMembersWithEmail.generateOne
        val member1 = projectMembersNoEmail.generateOne
        val member2 = projectMembersWithEmail.generateOne
        val member3 = projectMembersWithEmail.generateOne
        val info    = projectInfo.copy(maybeCreator = creator.user.some, members = Set(member1, member2, member3))

        val creatorAsCliPerson = creator.toTestPerson.copy(maybeGitLabId = None)
        val activity1          = activityWith(member2.toTestPerson.copy(maybeGitLabId = None))(info.dateCreated)
        val activity2          = activityWith(cliShapedPersons.generateOne)(info.dateCreated)
        val activity3 = activityWithAssociationAgent(creator.toTestPerson.copy(maybeGitLabId = None))(info.dateCreated)
        val dataset1  = datasetWith(member3.toTestPerson.copy(maybeGitLabId = None))(info.dateCreated)
        val dataset2: testentities.Dataset[testentities.Dataset.Provenance] =
          datasetWith(cliShapedPersons.generateOne)(info.dateCreated)

        val testProject: testentities.Project =
          createRenkuProject(info, cliVersion, schemaVersion)
            .asInstanceOf[testentities.RenkuProject.WithoutParent]
            .copy(
              activities = activity1 :: activity2 :: activity3 :: Nil,
              datasets = dataset1 :: dataset2 :: Nil
            )

        val cliProject = testProject.to[CliProject]
        val allPersons = cliProject.collectAllPersons

        val mergedCreator = merge(creatorAsCliPerson, creator)
        val mergedMember2 = merge(activity1.author, member2)
        val mergedMember3 = dataset1.provenance.creators
          .find(byEmail(member3))
          .map(merge(_, member3))
          .getOrElse(fail(show"No dataset1 creator with ${member3.user.email}"))

        val expectedActivities: List[testentities.Activity] =
          (activity1.copy(author = mergedMember2.person) :: activity2 :: replaceAgent(activity3,
                                                                                      mergedCreator.person
          ) :: Nil)
            .sortBy(_.startTime)

        val decoded = Project.fromCli(cliProject, allPersons, info)
        decoded shouldMatchToValid testProject
          .to[entities.Project]
          .asInstanceOf[entities.RenkuProject.WithoutParent]
          .copy(
            dateModified = List(cliProject.dateModified, info.dateModified).max,
            activities = expectedActivities.map(_.to[entities.Activity]),
            maybeCreator = mergedCreator.to[entities.Project.Member].person.some,
            datasets = List(
              addTo(dataset1, NonEmptyList.one(mergedMember3.person))
                .to[entities.Dataset[entities.Dataset.Provenance]],
              dataset2
                .to[entities.Dataset[entities.Dataset.Provenance]]
            ),
            members = Set(member1.toMember,
                          mergedMember2.to[entities.Project.Member],
                          mergedMember3.to[entities.Project.Member]
            )
          )
      }
    }

    "turn CliProject entity with parent into the Project object" in new TestCase {
      forAll(gitLabProjectInfos.map(projectInfoMaybeParent.replace(projectSlugs.generateSome))) { projectInfo =>
        val creator = projectMembersWithEmail.generateOne
        val member1 = projectMembersNoEmail.generateOne
        val member2 = projectMembersWithEmail.generateOne
        val member3 = projectMembersWithEmail.generateOne
        val info    = projectInfo.copy(maybeCreator = creator.user.some, members = Set(member1, member2, member3))

        val creatorAsCliPerson = creator.toTestPerson.copy(maybeGitLabId = None)
        val activity1          = activityWith(member2.toTestPerson.copy(maybeGitLabId = None))(info.dateCreated)
        val activity2          = activityWith(cliShapedPersons.generateOne)(info.dateCreated)
        val activity3          = activityWithAssociationAgent(creatorAsCliPerson)(info.dateCreated)
        val dataset1           = datasetWith(member3.toTestPerson.copy(maybeGitLabId = None))(info.dateCreated)
        val dataset2           = datasetWith(cliShapedPersons.generateOne)(info.dateCreated)

        val testProject: testentities.Project =
          createRenkuProject(info, cliVersion, schemaVersion)
            .asInstanceOf[testentities.RenkuProject.WithParent]
            .copy(
              activities = activity1 :: activity2 :: activity3 :: Nil,
              datasets = dataset1 :: dataset2 :: Nil
            )

        val cliProject = testProject.to[CliProject]
        val allPersons = cliProject.collectAllPersons

        val mergedCreator = merge(creatorAsCliPerson, creator)
        val mergedMember2 = merge(activity1.author, member2)
        val mergedMember3 = dataset1.provenance.creators
          .find(byEmail(member3))
          .map(merge(_, member3))
          .getOrElse(fail(show"No dataset1 creator with ${member3.user.email}"))

        val expectedActivities: List[testentities.Activity] =
          (activity1.copy(author = mergedMember2.person) :: activity2 :: replaceAgent(activity3,
                                                                                      mergedCreator.person
          ) :: Nil)
            .sortBy(_.startTime)

        val decoded = Project.fromCli(cliProject, allPersons, info)

        decoded shouldMatchToValid testProject
          .to[entities.Project]
          .asInstanceOf[entities.RenkuProject.WithParent]
          .copy(
            dateModified = List(cliProject.dateModified, info.dateModified).max,
            members = Set(member1.toMember,
                          mergedMember2.to[entities.Project.Member],
                          mergedMember3.to[entities.Project.Member]
            ),
            maybeCreator = mergedCreator.to[entities.Project.Member].person.some,
            activities = expectedActivities.map(_.to[entities.Activity]),
            datasets = List(
              addTo(dataset1, NonEmptyList.one(mergedMember3.person)).to[entities.Dataset[entities.Dataset.Provenance]],
              dataset2.to[entities.Dataset[entities.Dataset.Provenance]]
            )
          )
      }
    }

    "turn non-renku CliProject entity without parent into the NonRenkuProject object" in {
      forAll(gitLabProjectInfos.map(projectInfoMaybeParent.replace(None))) { projectInfo =>
        val creator = projectMembersWithEmail.generateOne
        val members = gitLabProjectMembers.generateSet()
        val info    = projectInfo.copy(maybeCreator = creator.user.some, members = members)
        val testProject: testentities.Project = createNonRenkuProject(info)

        val cliProject = testProject.to[CliProject]
        val allPersons = cliProject.collectAllPersons

        Project.fromCli(cliProject, allPersons, info) shouldMatchToValid
          testProject
            .to[entities.Project]
            .asInstanceOf[entities.NonRenkuProject.WithoutParent]
            .copy(
              members = members.map(_.toMember),
              maybeCreator = creator.toPerson.some
            )
      }
    }

    "turn non-renku CliProject entity with parent into the NonRenkuProject object" in {
      forAll(gitLabProjectInfos.map(projectInfoMaybeParent.replace(projectSlugs.generateSome))) { projectInfo =>
        val creator = projectMembersWithEmail.generateOne
        val members = gitLabProjectMembers.generateSet()
        val info    = projectInfo.copy(maybeCreator = creator.user.some, members = members)
        val testProject: testentities.Project = createNonRenkuProject(info)

        val cliProject = testProject.to[CliProject]
        val allPersons = cliProject.collectAllPersons

        Project.fromCli(cliProject, allPersons, info) shouldMatchToValid
          testProject
            .to[entities.Project]
            .asInstanceOf[entities.NonRenkuProject.WithParent]
            .copy(members = members.map(_.toMember), maybeCreator = creator.toPerson.some)
      }
    }

    forAll {
      Table(
        "Project type"   -> "Project Info",
        "without parent" -> gitLabProjectInfos.map(projectInfoMaybeParent.replace(None)).generateOne,
        "with parent" -> gitLabProjectInfos.map(projectInfoMaybeParent.replace(projectSlugs.generateSome)).generateOne
      )
    } { (projectType, info) =>
      s"match persons in plan.creators for project $projectType" in new TestCase {

        val creator = projectMembersWithEmail.generateOne
        val member2 = projectMembersWithEmail.generateOne

        val projectInfo        = info.copy(maybeCreator = creator.user.some, members = Set(member2))
        val creatorAsCliPerson = creator.toTestPerson.copy(maybeGitLabId = None)
        val activity =
          testentities.Activity.Lenses.planCreators.replace(List(creatorAsCliPerson))(
            activityWith(member2.toTestPerson.copy(maybeGitLabId = None))(projectInfo.dateCreated)
          )

        val testProject = createRenkuProject(projectInfo, cliVersion, schemaVersion)
          .asInstanceOf[testentities.RenkuProject]
          .fold(_.copy(activities = activity :: Nil), _.copy(activities = activity :: Nil))
          .asInstanceOf[testentities.Project]

        val cliProject = testProject.to[CliProject]
        val allPersons = cliProject.collectAllPersons

        val mergedCreator = merge(creatorAsCliPerson, creator)
        val mergedMember2 = merge(activity.author, member2)

        val actual =
          Project.fromCli(cliProject, allPersons, projectInfo).fold(errs => fail(errs.intercalate("; ")), identity)

        actual.maybeCreator shouldBe mergedCreator.to[entities.Project.Member].person.some
        actual.members      shouldBe Set(mergedMember2.to[entities.Project.Member])
        actual.activities shouldBe ActivityLens.activityAuthor.replace(mergedMember2.to[entities.Project.Member].person)(
          activity.to[entities.Activity]
        ) :: Nil
        actual.plans shouldBe PlanLens.planCreators.replace(List(mergedCreator.to[entities.Project.Member].person))(
          activity.plan.to[entities.Plan]
        ) :: Nil
      }

      s"update Plans' originalResourceId for project $projectType" in new TestCase {

        val activity = activityEntities(stepPlanEntities(planCommands, cliShapedPersons), cliShapedPersons)(
          info.dateCreated
        ).generateOne
        val plan                      = activity.plan
        val entitiesPlan              = plan.to[entities.Plan]
        val planModification1         = plan.createModification()
        val entitiesPlanModification1 = planModification1.to[entities.StepPlan.Modified]
        val planModification2         = planModification1.createModification()
        val entitiesPlanModification2 = planModification2.to[entities.StepPlan.Modified]

        val testProject = createRenkuProject(info, cliVersion, schemaVersion)
          .asInstanceOf[testentities.RenkuProject]
          .fold(
            _.copy(activities = activity :: Nil, unlinkedPlans = planModification1 :: planModification2 :: Nil),
            _.copy(activities = activity :: Nil, unlinkedPlans = planModification1 :: planModification2 :: Nil)
          )
          .asInstanceOf[testentities.Project]

        val cliProject = testProject.to[CliProject]
        val allPersons = cliProject.collectAllPersons

        val results = Project.fromCli(cliProject, allPersons, info).toEither

        val actualPlan1 :: actualPlan2 :: actualPlan3 :: Nil = results.value.plans
        actualPlan1 shouldBe entitiesPlan
        actualPlan2 shouldBe entitiesPlanModification1
        actualPlan3 shouldBe (modifiedPlanDerivation >>> planDerivationOriginalId)
          .replace(entitiesPlan.resourceId)(entitiesPlanModification2)
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

    "fail composite plans validation in case of missing referenced entities" in {

      val validate = new (List[entities.Plan] => ValidatedNel[String, Unit]) with entities.RenkuProject.ProjectFactory {
        def apply(plans: List[entities.Plan]): ValidatedNel[String, Unit] =
          this.validateCompositePlanData(plans)
      }

      val invalidPlanGen = for {
        cp   <- compositePlanNonEmptyMappings(personEntities).mapF(_.map(_.asInstanceOf[CompositePlan.NonModified]))
        step <- stepPlanGenFactory(personEntities)
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

      val projectGen = renkuProjectEntitiesWithDatasetsAndActivities(personGen = cliShapedPersons)
        .map(_.addCompositePlan(CreateCompositePlan(compositePlanEntities(cliShapedPersons, _))))

      forAll(projectGen) { (project: testentities.RenkuProject) =>
        validate(project.to[entities.Project].plans)
          .leftMap(_.toList.intercalate("; "))
          .fold(fail(_), identity)

        val cliCompositePlans = project
          .to[CliProject]
          .plans
          .collect {
            case CliProject.ProjectPlan.Composite(plan)             => CliPlan(plan)
            case CliProject.ProjectPlan.WorkflowFileComposite(plan) => CliPlan(plan.asCliCompositePlan)
          }
        val plans = cliCompositePlans.traverse(entities.Plan.fromCli)

        plans shouldMatchToValid project.plans.filter(_.isInstanceOf[CompositePlan]).map(_.to[entities.Plan])
      }
    }

    "fail composite plan validation in case there are references pointing outside of this composite plan" in {

      val testPlan: testentities.CompositePlan = compositePlanNonEmptyMappings(cliShapedPersons).generateOne
        .asInstanceOf[CompositePlan.NonModified]

      val testCliPlan = testPlan.to[CliCompositePlan]

      // find a parameter id that is mapped
      val mappedParameterId =
        testCliPlan.mappings.headOption
          .map(_.mapsTo.head.fold(_.resourceId, _.resourceId, _.resourceId, _.resourceId))
          .getOrElse(fail("Cannot find any mapping"))

      // remove this plan from the children plan list
      val invalidCliPlan =
        testCliPlan.copy(plans =
          NonEmptyList.fromListUnsafe(
            testCliPlan.plans.filterNot(_.fold(allStepParameterIds, allMappingParameterIds).contains(mappedParameterId))
          )
        )

      val decoded = Plan
        .fromCli(CliPlan(invalidCliPlan))
        .map(_ :: Nil)
        .toEither
        .fold(errs => fail(errs.intercalate("; ")), identity)

      val validate = new (List[entities.Plan] => ValidatedNel[String, Unit]) with entities.RenkuProject.ProjectFactory {
        def apply(plans: List[entities.Plan]): ValidatedNel[String, Unit] =
          this.validateCompositePlanData(plans)
      }

      validate(decoded).isInvalid shouldBe true
    }

    "return Invalid if there's a modified Plan pointing to a non-existing parent" in new TestCase {

      val info = gitLabProjectInfos.generateOne
      val activity = activityEntities(stepPlanEntities(planCommands, cliShapedPersons), cliShapedPersons)(
        info.dateCreated
      ).generateOne

      val planModification = {
        val p = activity.plan
        val modification = p
          .createModification()
          .copy(
            parent = stepPlanEntities().generateOne
          )
        modification
      }

      val testProject = createRenkuProject(info, cliVersion, schemaVersion)
        .fold(_.copy(
                activities = activity :: Nil,
                unlinkedPlans = planModification :: Nil
              ),
              _.copy(
                activities = activity :: Nil,
                unlinkedPlans = planModification :: Nil
              )
        )
      val cliProject = testProject.to[CliProject]
      val allPersons = cliProject.collectAllPersons

      val result = Project.fromCli(cliProject, allPersons, info)
      result should beInvalidWithMessageIncluding(s"Cannot find parent plan ${planModification.parent.id.asEntityId}")
    }

    "return Invalid if there's a modified Plan with the date from before the parent date" in new TestCase {

      val info = gitLabProjectInfos.generateOne
      val activity = activityEntities(stepPlanEntities(planCommands, cliShapedPersons), cliShapedPersons)(
        info.dateCreated
      ).generateOne
      val planModification = {
        val p = activity.plan
        val modification = p
          .createModification()
          .copy(dateCreated =
            timestamps(info.dateCreated.value, p.dateCreated.value.minusSeconds(1)).generateAs(plans.DateCreated)
          )
        modification
      }

      val testProject = createRenkuProject(info, cliVersion, schemaVersion)
        .fold(_.copy(
                activities = activity :: Nil,
                unlinkedPlans = planModification :: Nil
              ),
              _.copy(
                activities = activity :: Nil,
                unlinkedPlans = planModification :: Nil
              )
        )
      val cliProject = testProject.to[CliProject]
      val allPersons = cliProject.collectAllPersons

      val result = Project.fromCli(cliProject, allPersons, info)
      result should beInvalidWithMessageIncluding(
        show"Plan ${planModification.id.asEntityId} is older than it's parent ${planModification.parent.id.asEntityId}"
      )
    }

    "return Invalid when there's a Dataset entity that cannot be decoded" in new TestCase {

      val projectInfo = gitLabProjectInfos.map(projectInfoMaybeParent.replace(None)).generateOne

      val testProject = createRenkuProject(projectInfo, cliVersion, schemaVersion)

      val cliProject = testProject
        .to[CliProject]
        .copy(datasets =
          datasetEntities(provenanceInternal(cliShapedPersons))
            .withDateBefore(projectInfo.dateCreated)
            .generateFixedSizeList(1)
            .map(_.to[CliDataset])
        )
      val allPersons = cliProject.collectAllPersons

      val result = Project.fromCli(cliProject, allPersons, projectInfo)
      result should beInvalidWithMessageIncluding("Dataset", "is older than project")
    }

    "return Invalid when there's an Activity entity created before project creation" in new TestCase {

      val projectInfo       = gitLabProjectInfos.map(projectInfoMaybeParent.replace(None)).generateOne
      val dateBeforeProject = timestamps(max = projectInfo.dateCreated.value.minusSeconds(1)).generateOne
      val activity = activityEntities(
        stepPlanEntities(planCommands, cliShapedPersons).map(
          _.replacePlanDateCreated(plans.DateCreated(dateBeforeProject))
        ),
        cliShapedPersons
      ).map(
        _.replaceStartTime(
          timestamps(min = dateBeforeProject, max = projectInfo.dateCreated.value).generateAs[activities.StartTime]
        )
      ).run(projects.DateCreated(dateBeforeProject))
        .generateOne
      val entitiesActivity = activity.to[entities.Activity]

      val testProject = createRenkuProject(projectInfo, cliVersion, schemaVersion)

      val cliProject = testProject
        .to[CliProject]
        .copy(activities = activity.to[CliActivity] :: Nil,
              plans = CliProject.ProjectPlan(activity.plan.to[CliPlan]) :: Nil
        )
      val allPersons = cliProject.collectAllPersons

      val result = Project.fromCli(cliProject, allPersons, projectInfo)
      result should beInvalidWithMessageIncluding(
        s"Activity ${entitiesActivity.resourceId} " +
          s"date ${activity.startTime} is older than project ${projectInfo.dateCreated}"
      )
    }

    "return Invalid when there's an internal Dataset entity created before project without parent" in new TestCase {

      val projectInfo = gitLabProjectInfos.map(projectInfoMaybeParent.replace(None)).generateOne
      val dataset =
        datasetEntities(provenanceInternal(cliShapedPersons)).withDateBefore(projectInfo.dateCreated).generateOne
      val entitiesDataset = dataset.to[entities.Dataset[entities.Dataset.Provenance.Internal]]

      val testProject = createRenkuProject(projectInfo, cliVersion, schemaVersion)
      val cliProject = testProject
        .to[CliProject]
        .copy(
          datasets = dataset.to[CliDataset] :: Nil
        )
      val allPersons = cliProject.collectAllPersons

      val result = Project.fromCli(cliProject, allPersons, projectInfo)
      result should beInvalidWithMessageIncluding(
        s"Dataset ${entitiesDataset.resourceId} " +
          s"date ${dataset.provenance.date} is older than project ${projectInfo.dateCreated}"
      )
    }

    "return Invalid when there's a Plan entity created before project without parent" in new TestCase {

      val projectInfo = gitLabProjectInfos.map(projectInfoMaybeParent.replace(None)).generateOne
      val plan = stepPlanEntities(planCommands, cliShapedPersons)(projectInfo.dateCreated).generateOne
        .replacePlanDateCreated(timestamps(max = projectInfo.dateCreated.value).generateAs[plans.DateCreated])
      val entitiesPlan = plan.to[entities.Plan]

      val cliProject = createRenkuProject(projectInfo, cliVersion, schemaVersion)
        .to[CliProject]
        .copy(plans = CliProject.ProjectPlan(plan.to[CliPlan]) :: Nil)
      val allPersons = cliProject.collectAllPersons

      val result = Project.fromCli(cliProject, allPersons, projectInfo)

      result should beInvalidWithMessageIncluding(
        s"Plan ${entitiesPlan.resourceId} " +
          s"date ${entitiesPlan.dateCreated} is older than project ${projectInfo.dateCreated}"
      )
    }

    "convert project when there's an internal or modified Dataset entity created before project with parent" in new TestCase {

      val parentSlug  = projectSlugs.generateOne
      val projectInfo = gitLabProjectInfos.map(projectInfoMaybeParent.replace(parentSlug.some)).generateOne
      val dataset1 = datasetEntities(provenanceInternal(cliShapedPersons))
        .withDateBefore(projectInfo.dateCreated)
        .generateOne
        .copy(parts = Nil)
      val (dataset2, dataset2Modified) = datasetAndModificationEntities(provenanceInternal(cliShapedPersons),
                                                                        modificationCreatorGen = cliShapedPersons
      ).map { case (orig, modified) =>
        val newOrigDate = timestamps(max = projectInfo.dateCreated.value).generateAs[datasets.DateCreated]
        val newModificationDate =
          timestamps(min = newOrigDate.instant, max = projectInfo.dateCreated.value).generateAs[datasets.DateCreated]
        (
          orig.copy(provenance = orig.provenance.copy(date = newOrigDate), parts = Nil),
          modified.copy(provenance = modified.provenance.copy(date = newModificationDate), parts = Nil)
        )
      }.generateOne
      val testProject = createRenkuProject(projectInfo, cliVersion, schemaVersion)
        .asInstanceOf[testentities.RenkuProject.WithParent]
        .copy(
          datasets = List(dataset1, dataset2, dataset2Modified)
        )
      val cliProject = testProject.to[CliProject]
      val allPersons = cliProject.collectAllPersons

      Project.fromCli(cliProject, allPersons, projectInfo) shouldMatchToValid testProject
        .to[entities.RenkuProject.WithParent]
        .copy(
          members = projectInfo.members.map(_.toMember),
          maybeCreator = projectInfo.maybeCreator.map(_.toPerson)
        )
    }

    "return Invalid when there's a modified Dataset entity created before project without parent" in new TestCase {

      val projectInfo = gitLabProjectInfos.map(projectInfoMaybeParent.replace(None)).generateOne
      val (dataset, modifiedDataset) =
        datasetAndModificationEntities(provenanceImportedExternal(creatorsGen = cliShapedPersons),
                                       modificationCreatorGen = cliShapedPersons
        ).map { case (orig, modified) =>
          val newOrigDate = timestamps(max = projectInfo.dateCreated.value)
            .map(LocalDate.ofInstant(_, ZoneOffset.UTC))
            .generateAs[datasets.DatePublished]
          val newModificationDate =
            timestamps(min = newOrigDate.instant, max = projectInfo.dateCreated.value)
              .generateAs[datasets.DateCreated]
          (
            orig.copy(provenance = orig.provenance.copy(date = newOrigDate), parts = Nil),
            modified.copy(provenance = modified.provenance.copy(date = newModificationDate), parts = Nil)
          )
        }.generateOne
      val entitiesModifiedDataset = modifiedDataset.to[entities.Dataset[entities.Dataset.Provenance.Modified]]

      val cliProject = createRenkuProject(projectInfo, cliVersion, schemaVersion)
        .to[CliProject]
        .copy(datasets = List(dataset, modifiedDataset).map(_.to[CliDataset]))
      val allPersons = cliProject.collectAllPersons

      val result = Project.fromCli(cliProject, allPersons, projectInfo)
      result should beInvalidWithMessageIncluding(
        s"Dataset ${entitiesModifiedDataset.resourceId} " +
          s"date ${entitiesModifiedDataset.provenance.date} is older than project ${projectInfo.dateCreated}"
      )
    }

    "convert project when there's a Dataset (neither internal nor modified) created before project creation" in new TestCase {

      val projectInfo = gitLabProjectInfos.map(projectInfoMaybeParent.replace(None)).generateOne
      val dataset1 = datasetEntities(provenanceImportedExternal(creatorsGen = cliShapedPersons))
        .withDateBefore(projectInfo.dateCreated)
        .generateOne
      val dataset2 = datasetEntities(provenanceImportedInternalAncestorExternal(creatorsGen = cliShapedPersons))
        .withDateBefore(projectInfo.dateCreated)
        .generateOne
      val dataset3 = datasetEntities(provenanceImportedInternalAncestorInternal(creatorsGen = cliShapedPersons))
        .withDateBefore(projectInfo.dateCreated)
        .generateOne

      val testProject = createRenkuProject(projectInfo, cliVersion, schemaVersion)
        .asInstanceOf[testentities.RenkuProject.WithoutParent]
        .copy(datasets = List(dataset1, dataset2, dataset3))

      val cliProject = testProject.to[CliProject]
      val allPersons = cliProject.collectAllPersons

      Project.fromCli(cliProject, allPersons, projectInfo) shouldMatchToValid testProject
        .to[entities.RenkuProject.WithoutParent]
        .copy(
          members = projectInfo.members.map(_.toMember),
          maybeCreator = projectInfo.maybeCreator.map(_.toPerson)
        )
    }

    "return Invalid when there's a modified Dataset that is derived from a non-existing dataset" in new TestCase {
      Set(
        gitLabProjectInfos.map(projectInfoMaybeParent.replace(None)).generateOne,
        gitLabProjectInfos.map(projectInfoMaybeParent.replace(projectSlugs.generateSome)).generateOne
      ) foreach { projectInfo =>
        val (original, modified) = datasetAndModificationEntities(provenanceInternal(cliShapedPersons),
                                                                  projectInfo.dateCreated,
                                                                  modificationCreatorGen = cliShapedPersons
        ).generateOne
        val (_, broken) = datasetAndModificationEntities(provenanceInternal(cliShapedPersons),
                                                         projectInfo.dateCreated,
                                                         modificationCreatorGen = cliShapedPersons
        ).generateOne

        val cliProject = createRenkuProject(projectInfo, cliVersion, schemaVersion)
          .to[CliProject]
          .copy(
            datasets = List(original, modified, broken).map(_.to[CliDataset])
          )
        val allPersons = cliProject.collectAllPersons

        val result = Project.fromCli(cliProject, allPersons, projectInfo)
        result should beInvalidWithMessageIncluding(
          show"Dataset ${broken.identification.identifier} is derived from non-existing dataset ${broken.provenance.derivedFrom}"
        )
      }
    }

    "pick the earliest from dateCreated found in gitlabProjectInfo and the CLI" in new TestCase {
      val gitlabDate   = projectCreatedDates().generateOne
      val cliDate      = projectCreatedDates().generateOne
      val earliestDate = List(gitlabDate, cliDate).min
      val projectInfo = gitLabProjectInfos
        .map(
          _.copy(maybeParentSlug = None,
                 dateCreated = gitlabDate,
                 dateModified = projectModifiedDates(gitlabDate.value).generateOne
          )
        )
        .generateOne

      val testProject = createRenkuProject(projectInfo, cliVersion, schemaVersion)
        .asInstanceOf[testentities.RenkuProject.WithoutParent]
        .copy(dateCreated = cliDate, dateModified = projectModifiedDates(cliDate.value).generateOne)

      val cliProject = testProject.to[CliProject]
      val allPersons = cliProject.collectAllPersons

      Project.fromCli(cliProject, allPersons, projectInfo) shouldMatchToValid testProject
        .to[entities.RenkuProject.WithoutParent]
        .copy(
          members = projectInfo.members.map(_.toMember),
          maybeCreator = projectInfo.maybeCreator.map(_.toPerson),
          dateCreated = earliestDate,
          dateModified = List(cliProject.dateModified, projectInfo.dateModified).max
        )
    }

    "favour the CLI description and keywords over the gitlab values" in new TestCase {
      val gitlabDate   = projectCreatedDates().generateOne
      val cliDate      = projectCreatedDates().generateOne
      val earliestDate = List(gitlabDate, cliDate).min
      val projectInfo = gitLabProjectInfos.generateOne.copy(
        maybeParentSlug = None,
        dateCreated = gitlabDate,
        dateModified = projectModifiedDates(gitlabDate.value).generateOne,
        maybeDescription = projectDescriptions.generateSome,
        keywords = projectKeywords.generateSet(min = 1)
      )
      val description = projectDescriptions.generateSome
      val keywords    = projectKeywords.generateSet(min = 1)

      val testProject = createRenkuProject(projectInfo, cliVersion, schemaVersion)
        .asInstanceOf[testentities.RenkuProject.WithoutParent]
        .copy(
          maybeDescription = description,
          keywords = keywords,
          dateCreated = cliDate,
          dateModified = projectModifiedDates(cliDate.value).generateOne,
          maybeCreator = None
        )
      val cliProject = testProject.to[CliProject]
      val allPersons = cliProject.collectAllPersons

      Project.fromCli(cliProject, allPersons, projectInfo) shouldMatchToValid testProject
        .to[entities.RenkuProject.WithoutParent]
        .copy(
          members = projectInfo.members.map(_.toMember),
          maybeCreator = projectInfo.maybeCreator.map(_.toPerson),
          dateCreated = earliestDate,
          dateModified = List(projectInfo.dateModified, cliProject.dateModified).max
        )
    }

    "fallback to GitLab's description and/or keywords if they are absent in the CLI payload" in new TestCase {
      val gitlabDate   = projectCreatedDates().generateOne
      val cliDate      = projectCreatedDates().generateOne
      val earliestDate = List(gitlabDate, cliDate).min
      val projectInfo = gitLabProjectInfos.generateOne.copy(
        maybeParentSlug = None,
        dateCreated = gitlabDate,
        dateModified = projectModifiedDates(gitlabDate.value).generateOne,
        maybeDescription = projectDescriptions.generateSome,
        keywords = projectKeywords.generateSet(min = 1)
      )
      val testProject = createRenkuProject(projectInfo, cliVersion, schemaVersion)
        .asInstanceOf[testentities.RenkuProject.WithoutParent]
        .copy(maybeDescription = None,
              keywords = Set.empty,
              maybeCreator = None,
              dateCreated = cliDate,
              dateModified = projectModifiedDates(cliDate.value).generateOne
        )

      val cliProject = testProject.to[CliProject]
      val allPersons = cliProject.collectAllPersons

      Project.fromCli(cliProject, allPersons, projectInfo) shouldMatchToValid testProject
        .to[entities.RenkuProject.WithoutParent]
        .copy(
          members = projectInfo.members.map(_.toMember),
          maybeCreator = projectInfo.maybeCreator.map(_.toPerson),
          dateCreated = earliestDate,
          dateModified = List(projectInfo.dateModified, cliProject.dateModified).max,
          maybeDescription = projectInfo.maybeDescription,
          keywords = projectInfo.keywords
        )
    }

    "return no description and/or keywords if they are absent in both the CLI payload and gitlab" in new TestCase {
      val gitlabDate   = projectCreatedDates().generateOne
      val cliDate      = projectCreatedDates().generateOne
      val earliestDate = List(gitlabDate, cliDate).min
      val projectInfo = gitLabProjectInfos.generateOne.copy(
        maybeParentSlug = None,
        dateCreated = gitlabDate,
        dateModified = projectModifiedDates(gitlabDate.value).generateOne,
        maybeDescription = projectDescriptions.generateNone,
        keywords = Set.empty
      )

      val testProject = createRenkuProject(projectInfo, cliVersion, schemaVersion)
        .asInstanceOf[testentities.RenkuProject.WithoutParent]
        .copy(
          maybeDescription = None,
          keywords = Set.empty,
          maybeCreator = None,
          dateCreated = cliDate,
          dateModified = projectModifiedDates(cliDate.value).generateOne
        )

      val cliProject = testProject.to[CliProject]
      val allPersons = cliProject.collectAllPersons

      Project.fromCli(cliProject, allPersons, projectInfo) shouldMatchToValid testProject
        .to[entities.RenkuProject.WithoutParent]
        .copy(
          members = projectInfo.members.map(_.toMember),
          maybeCreator = projectInfo.maybeCreator.map(_.toPerson),
          dateCreated = earliestDate,
          dateModified = List(projectInfo.dateModified, cliProject.dateModified).max,
          maybeDescription = None,
          keywords = Set.empty
        )
    }
  }

  "RenkuProject.WithParent.from" should {

    "fail if dateCreated > dateModified" in new TestCase {

      val dateCreated  = projectCreatedDates().generateOne
      val dateModified = timestamps(max = dateCreated.value.minusSeconds(1)).generateAs(projects.DateModified)
      RenkuProject.WithParent
        .from(
          projectResourceIds.generateOne,
          projectSlugs.generateOne,
          projectNames.generateOne,
          maybeDescription = None,
          cliVersions.generateOne,
          dateCreated,
          dateModified,
          maybeCreator = None,
          projectVisibilities.generateOne,
          keywords = Set.empty,
          members = Set.empty,
          projectSchemaVersions.generateOne,
          activities = List.empty,
          datasets = List.empty,
          plans = List.empty,
          parentResourceId = projectResourceIds.generateOne,
          images = List.empty
        )
        .toEither
        .left
        .value shouldBe NonEmptyList.one(
        show"Project dateModified $dateModified is older than dateCreated $dateCreated"
      )
    }
  }

  "RenkuProject.WithoutParent.from" should {

    "fail if dateCreated > dateModified" in new TestCase {

      val dateCreated  = projectCreatedDates().generateOne
      val dateModified = timestamps(max = dateCreated.value.minusSeconds(1)).generateAs(projects.DateModified)
      RenkuProject.WithoutParent
        .from(
          projectResourceIds.generateOne,
          projectSlugs.generateOne,
          projectNames.generateOne,
          maybeDescription = None,
          cliVersions.generateOne,
          dateCreated,
          dateModified,
          maybeCreator = None,
          projectVisibilities.generateOne,
          keywords = Set.empty,
          members = Set.empty,
          projectSchemaVersions.generateOne,
          activities = List.empty,
          datasets = List.empty,
          plans = List.empty,
          images = List.empty
        )
        .toEither
        .left
        .value shouldBe NonEmptyList.one(
        show"Project dateModified $dateModified is older than dateCreated $dateCreated"
      )
    }
  }

  "NonRenkuProject.WithParent.from" should {

    "fail if dateCreated > dateModified" in new TestCase {

      val dateCreated  = projectCreatedDates().generateOne
      val dateModified = timestamps(max = dateCreated.value.minusSeconds(1)).generateAs(projects.DateModified)
      NonRenkuProject.WithParent
        .from(
          projectResourceIds.generateOne,
          projectSlugs.generateOne,
          projectNames.generateOne,
          maybeDescription = None,
          dateCreated,
          dateModified,
          maybeCreator = None,
          projectVisibilities.generateOne,
          keywords = Set.empty,
          members = Set.empty,
          parentResourceId = projectResourceIds.generateOne,
          images = List.empty
        )
        .toEither
        .left
        .value shouldBe NonEmptyList.one(
        show"Project dateModified $dateModified is older than dateCreated $dateCreated"
      )
    }
  }

  "NonRenkuProject.WithoutParent.from" should {

    "fail if dateCreated > dateModified" in new TestCase {

      val dateCreated  = projectCreatedDates().generateOne
      val dateModified = timestamps(max = dateCreated.value.minusSeconds(1)).generateAs(projects.DateModified)
      NonRenkuProject.WithoutParent
        .from(
          projectResourceIds.generateOne,
          projectSlugs.generateOne,
          projectNames.generateOne,
          maybeDescription = None,
          dateCreated,
          dateModified,
          maybeCreator = None,
          projectVisibilities.generateOne,
          keywords = Set.empty,
          members = Set.empty,
          images = List.empty
        )
        .toEither
        .left
        .value shouldBe NonEmptyList.one(
        show"Project dateModified $dateModified is older than dateCreated $dateCreated"
      )
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
              renku / "slug"              -> project.slug.asJsonLD,
              renku / "projectPath"       -> project.slug.asJsonLD,
              renku / "projectNamespace"  -> project.slug.toNamespace.asJsonLD,
              renku / "projectNamespaces" -> project.namespaces.asJsonLD,
              schema / "description"      -> project.maybeDescription.asJsonLD,
              schema / "agent"            -> project.agent.asJsonLD,
              schema / "dateCreated"      -> project.dateCreated.asJsonLD,
              schema / "dateModified"     -> project.dateModified.asJsonLD,
              schema / "creator"          -> project.maybeCreator.asJsonLD,
              renku / "projectVisibility" -> project.visibility.asJsonLD,
              schema / "keywords"         -> project.keywords.asJsonLD,
              schema / "member"           -> project.members.map(_.person).toList.asJsonLD,
              schema / "schemaVersion"    -> project.version.asJsonLD,
              renku / "hasActivity"       -> project.activities.asJsonLD,
              renku / "hasPlan"           -> project.plans.asJsonLD,
              renku / "hasDataset"        -> project.datasets.asJsonLD,
              prov / "wasDerivedFrom"     -> maybeParentId.map(_.asEntityId).asJsonLD,
              schema / "image"            -> project.images.asJsonLD
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
              renku / "slug"              -> project.slug.asJsonLD,
              renku / "projectPath"       -> project.slug.asJsonLD,
              renku / "projectNamespace"  -> project.slug.toNamespace.asJsonLD,
              renku / "projectNamespaces" -> project.namespaces.asJsonLD,
              schema / "description"      -> project.maybeDescription.asJsonLD,
              schema / "dateCreated"      -> project.dateCreated.asJsonLD,
              schema / "dateModified"     -> project.dateModified.asJsonLD,
              schema / "creator"          -> project.maybeCreator.asJsonLD,
              renku / "projectVisibility" -> project.visibility.asJsonLD,
              schema / "keywords"         -> project.keywords.asJsonLD,
              schema / "member"           -> project.members.map(_.person).toList.asJsonLD,
              prov / "wasDerivedFrom"     -> maybeParentId.map(_.asEntityId).asJsonLD,
              schema / "image"            -> project.images.asJsonLD
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
          .modify(replaceMembers(projectMemberEntities(withoutGitLabId).generateFixedSizeSet(ofSize = 1)))
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
              renku / "slug"              -> project.slug.asJsonLD,
              renku / "projectPath"       -> project.slug.asJsonLD,
              renku / "projectNamespace"  -> project.slug.toNamespace.asJsonLD,
              renku / "projectNamespaces" -> project.namespaces.asJsonLD,
              schema / "description"      -> project.maybeDescription.asJsonLD,
              schema / "agent"            -> project.agent.asJsonLD,
              schema / "dateCreated"      -> project.dateCreated.asJsonLD,
              schema / "dateModified"     -> project.dateModified.asJsonLD,
              schema / "creator"          -> project.maybeCreator.map(_.resourceId.asEntityId).asJsonLD,
              renku / "projectVisibility" -> project.visibility.asJsonLD,
              schema / "keywords"         -> project.keywords.asJsonLD,
              schema / "member"           -> project.members.map(_.person.resourceId.asEntityId).toList.asJsonLD,
              schema / "schemaVersion"    -> project.version.asJsonLD,
              renku / "hasActivity"       -> project.activities.asJsonLD,
              renku / "hasPlan"           -> project.plans.asJsonLD,
              renku / "hasDataset"        -> project.datasets.asJsonLD,
              prov / "wasDerivedFrom"     -> maybeParentId.map(_.asEntityId).asJsonLD,
              schema / "image"            -> project.images.asJsonLD
            ) :: project.datasets.flatMap(_.publicationEvents.map(_.asJsonLD)): _*
          )
          .toJson
      }
    }

    "produce JsonLD with all the relevant properties or a non-Renku Project" in {
      forAll(
        anyNonRenkuProjectEntities
          .modify(replaceMembers(projectMemberEntities(withoutGitLabId).generateFixedSizeSet(ofSize = 1)))
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
              renku / "slug"              -> project.slug.asJsonLD,
              renku / "projectPath"       -> project.slug.asJsonLD,
              renku / "projectNamespace"  -> project.slug.toNamespace.asJsonLD,
              renku / "projectNamespaces" -> project.namespaces.asJsonLD,
              schema / "description"      -> project.maybeDescription.asJsonLD,
              schema / "dateCreated"      -> project.dateCreated.asJsonLD,
              schema / "dateModified"     -> project.dateModified.asJsonLD,
              schema / "creator"          -> project.maybeCreator.map(_.resourceId.asEntityId).asJsonLD,
              renku / "projectVisibility" -> project.visibility.asJsonLD,
              schema / "keywords"         -> project.keywords.asJsonLD,
              schema / "member"           -> project.members.map(_.person.resourceId.asEntityId).toList.asJsonLD,
              prov / "wasDerivedFrom"     -> maybeParentId.map(_.asEntityId).asJsonLD,
              schema / "image"            -> project.images.asJsonLD
            )
          )
          .toJson
      }
    }

    "produce JsonLD with all the relevant properties without images" in {
      forAll(
        anyNonRenkuProjectEntities
          .modify(replaceMembers(projectMemberEntities(withoutGitLabId).generateFixedSizeSet(ofSize = 1)))
          .modify(replaceImages(Nil))
          .map(_.to[entities.NonRenkuProject])
      ) { project =>
        val maybeParentId = project match {
          case p: entities.NonRenkuProject.WithParent => p.parentResourceId.some
          case _ => Option.empty[projects.ResourceId]
        }

        val modifiedJson =
          project.asJsonLD.toJson.asArray
            .getOrElse(Vector.empty)
            .flatMap(_.asObject)
            .map(obj => obj.remove((schema / "image").show))
            .asJson

        modifiedJson shouldBe JsonLD
          .arr(
            JsonLD.entity(
              EntityId.of(project.resourceId.show),
              entities.Project.entityTypes,
              schema / "name"             -> project.name.asJsonLD,
              renku / "slug"              -> project.slug.asJsonLD,
              renku / "projectPath"       -> project.slug.asJsonLD,
              renku / "projectNamespace"  -> project.slug.toNamespace.asJsonLD,
              renku / "projectNamespaces" -> project.namespaces.asJsonLD,
              schema / "description"      -> project.maybeDescription.asJsonLD,
              schema / "dateCreated"      -> project.dateCreated.asJsonLD,
              schema / "dateModified"     -> project.dateModified.asJsonLD,
              schema / "creator"          -> project.maybeCreator.map(_.resourceId.asEntityId).asJsonLD,
              renku / "projectVisibility" -> project.visibility.asJsonLD,
              schema / "keywords"         -> project.keywords.asJsonLD,
              schema / "member"           -> project.members.map(_.person.resourceId.asEntityId).toList.asJsonLD,
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

  private def createRenkuProjectFromSlug(
      parentSlug:    projects.Slug,
      cliVersion:    CliVersion,
      schemaVersion: SchemaVersion
  ) = {
    val dateCreated = projectCreatedDates().generateOne
    testentities.RenkuProject.WithoutParent(
      slug = parentSlug,
      name = projectNames.generateOne,
      maybeDescription = None,
      agent = cliVersion,
      dateCreated = dateCreated,
      dateModified = projectModifiedDates(dateCreated.value).generateOne,
      maybeCreator = None,
      visibility = projects.Visibility.Public,
      keywords = Set.empty,
      members = Set.empty,
      version = schemaVersion,
      activities = Nil,
      datasets = Nil,
      unlinkedPlans = Nil,
      images = Nil,
      forksCount = ForksCount(1)
    )
  }

  private def createRenkuProject(info: GitLabProjectInfo, cliVersion: CliVersion, schemaVersion: SchemaVersion) =
    info.maybeParentSlug match {
      case Some(parentSlug) =>
        testentities.RenkuProject.WithParent(
          slug = info.slug,
          name = info.name,
          maybeDescription = info.maybeDescription,
          agent = cliVersion,
          dateCreated = info.dateCreated,
          dateModified = info.dateModified,
          maybeCreator = None,
          visibility = info.visibility,
          forksCount = ForksCount(1),
          keywords = info.keywords,
          members = Set.empty,
          version = schemaVersion,
          activities = Nil,
          datasets = Nil,
          unlinkedPlans = Nil,
          images = info.avatarUrl.toList,
          createCompositePlans = Nil,
          parent = createRenkuProjectFromSlug(parentSlug, cliVersion, schemaVersion)
        )
      case None =>
        testentities.RenkuProject.WithoutParent(
          slug = info.slug,
          name = info.name,
          maybeDescription = info.maybeDescription,
          agent = cliVersion,
          dateCreated = info.dateCreated,
          dateModified = info.dateModified,
          maybeCreator = None,
          visibility = info.visibility,
          forksCount = ForksCount(1),
          keywords = info.keywords,
          members = Set.empty,
          version = schemaVersion,
          activities = Nil,
          datasets = Nil,
          unlinkedPlans = Nil,
          images = info.avatarUrl.toList,
          createCompositePlans = Nil
        )
    }

  private def createNonRenkuProjectFromSlug(parentSlug: projects.Slug) = {
    val createdDate = projectCreatedDates().generateOne
    testentities.NonRenkuProject.WithoutParent(
      slug = parentSlug,
      name = projectNames.generateOne,
      maybeDescription = None,
      dateCreated = createdDate,
      dateModified = projectModifiedDates(createdDate.value).generateOne,
      maybeCreator = None,
      visibility = projects.Visibility.Public,
      forksCount = ForksCount(1),
      keywords = Set.empty,
      members = Set.empty,
      images = Nil
    )
  }

  private def createNonRenkuProject(info: GitLabProjectInfo) =
    info.maybeParentSlug match {
      case Some(parentSlug) =>
        testentities.NonRenkuProject.WithParent(
          slug = info.slug,
          name = info.name,
          maybeDescription = info.maybeDescription,
          dateCreated = info.dateCreated,
          dateModified = info.dateModified,
          maybeCreator = None,
          visibility = info.visibility,
          forksCount = ForksCount(1),
          keywords = info.keywords,
          members = Set.empty,
          images = info.avatarUrl.toList,
          parent = createNonRenkuProjectFromSlug(parentSlug)
        )
      case None =>
        testentities.NonRenkuProject.WithoutParent(
          slug = info.slug,
          name = info.name,
          maybeDescription = info.maybeDescription,
          dateCreated = info.dateCreated,
          dateModified = info.dateModified,
          maybeCreator = None,
          visibility = info.visibility,
          forksCount = ForksCount(1),
          keywords = info.keywords,
          members = Set.empty,
          images = info.avatarUrl.toList
        )
    }

  private implicit class GitLabMemberOps(gitLabPerson: GitLabMember) {
    lazy val toPerson: entities.Person =
      gitLabPerson.user.toPerson

    lazy val toTestPerson: testentities.Person =
      gitLabPerson.user.toTestPerson

    lazy val toMember: entities.Project.Member =
      entities.Project.Member(toPerson, gitLabPerson.role)

    lazy val toTestMember: testentities.Project.Member =
      testentities.Project.Member(toTestPerson, gitLabPerson.role)
  }

  private implicit class GitLabUserOps(gitLabUser: GitLabUser) {
    lazy val toPerson: entities.Person = entities.Person.WithGitLabId(
      persons.ResourceId(gitLabUser.gitLabId),
      gitLabUser.gitLabId,
      gitLabUser.name,
      maybeEmail = gitLabUser.email,
      maybeOrcidId = None,
      maybeAffiliation = None
    )
    lazy val toTestPerson: testentities.Person = testentities.Person(
      persons.Name(gitLabUser.username.value),
      gitLabUser.email,
      gitLabUser.gitLabId.some,
      maybeOrcidId = None,
      maybeAffiliation = None
    )
  }

  private implicit class CliProjectOps(self: CliProject) {
    def collectAllPersons: Set[CliPerson] =
      self.asFlattenedJsonLD.cursor.as[List[CliPerson]].fold(throw _, _.toSet)
  }

  private def activityWith(
      author: testentities.Person
  ): projects.DateCreated => testentities.Activity =
    dateCreated => {
      val activity =
        activityEntities(stepPlanEntities(planCommands, cliShapedPersons).map(_.removeCreators()), cliShapedPersons)(
          dateCreated
        ).generateOne
      activity.copy(author = author)
    }

  private def activityWithAssociationAgent(
      agent: testentities.Person
  ): projects.DateCreated => testentities.Activity =
    dateCreated => {
      val activity = activityEntities(stepPlanEntities(planCommands, cliShapedPersons).map(_.removeCreators()),
                                      cliShapedPersons
      )(dateCreated).generateOne

      activity.copy(associationFactory = a => testentities.Association.WithPersonAgent(a, agent, activity.plan))
    }

  private def datasetWith(
      creator: testentities.Person,
      other:   testentities.Person*
  ): projects.DateCreated => testentities.Dataset[testentities.Dataset.Provenance] = dateCreated => {
    val ds = datasetEntities(provenanceNonModified(cliShapedPersons))(renkuUrl)(dateCreated).generateOne

    addTo(ds, NonEmptyList.of(creator, other: _*))
  }

  private def addTo(
      dataset:  testentities.Dataset[testentities.Dataset.Provenance],
      creators: NonEmptyList[testentities.Person]
  ): testentities.Dataset[testentities.Dataset.Provenance] =
    dataset.copy(provenance = dataset.provenance match {
      case p: testentities.Dataset.Provenance.Internal         => p.copy(creators = creators.sortBy(_.name))
      case p: testentities.Dataset.Provenance.ImportedExternal => p.copy(creators = creators.sortBy(_.name))
      case p: testentities.Dataset.Provenance.ImportedInternalAncestorInternal =>
        p.copy(creators = creators.sortBy(_.name))
      case p: testentities.Dataset.Provenance.ImportedInternalAncestorExternal =>
        p.copy(creators = creators.sortBy(_.name))
      case p: testentities.Dataset.Provenance.Modified => p.copy(creators = creators.sortBy(_.name))
    })

  private def replaceAgent(activity: testentities.Activity, newAgent: testentities.Person): testentities.Activity =
    testentities.Activity.Lenses.associationAgent.replace(Right(newAgent))(activity)

  private def byEmail(member: GitLabMember): testentities.Person => Boolean =
    p => (p.maybeEmail, member.user.email).mapN(_ == _).getOrElse(false)

  private def merge(person: testentities.Person, member: GitLabMember): testentities.Project.Member = {
    val p =
      person.copy(maybeGitLabId = member.user.gitLabId.some, name = member.user.name, maybeEmail = member.user.email)
    testentities.Project.Member(p, member.role)
  }

  private lazy val projectInfoMaybeParent: Lens[GitLabProjectInfo, Option[projects.Slug]] =
    Lens[GitLabProjectInfo, Option[projects.Slug]](_.maybeParentSlug)(mpp => _.copy(maybeParentSlug = mpp))

  private lazy val modifiedPlanDerivation: Lens[entities.StepPlan.Modified, entities.Plan.Derivation] =
    Lens[entities.StepPlan.Modified, entities.Plan.Derivation](_.derivation)(d => _.copy(derivation = d))

  private lazy val planDerivationOriginalId: Lens[entities.Plan.Derivation, plans.ResourceId] =
    Lens[entities.Plan.Derivation, plans.ResourceId](_.originalResourceId)(id => _.copy(originalResourceId = id))
}
