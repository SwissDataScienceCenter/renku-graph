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

package io.renku.graph.model.testentities
package generators

import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{noDashUuid, nonBlankStrings, nonEmptyStrings, positiveInts, relativePaths, sentences, timestamps, timestampsNotInTheFuture}
import io.renku.graph.model.GraphModelGenerators.{cliVersions, projectCreatedDates}
import io.renku.graph.model._
import io.renku.graph.model.commandParameters.ParameterDefaultValue
import io.renku.graph.model.entityModel.{Checksum, Location}
import io.renku.graph.model.parameterValues.ValueOverride
import io.renku.graph.model.plans.Command
import io.renku.graph.model.testentities.Entity.{InputEntity, OutputEntity}
import io.renku.graph.model.testentities.Plan.CommandParameters
import io.renku.graph.model.testentities.Plan.CommandParameters.CommandParameterFactory
import io.renku.graph.model.testentities.generators.EntitiesGenerators.{ActivityGenFactory, PlanGenFactory}
import io.renku.tinytypes.InstantTinyType
import org.scalacheck.Gen

import java.nio.charset.StandardCharsets._

object ActivityGenerators extends ActivityGenerators

trait ActivityGenerators {

  val activityIds:        Gen[Activity.Id]          = noDashUuid.toGeneratorOf(Activity.Id)
  val activityStartTimes: Gen[activities.StartTime] = timestampsNotInTheFuture.map(activities.StartTime.apply)
  def activityStartTimes(after: InstantTinyType): Gen[activities.StartTime] =
    timestampsNotInTheFuture(after.value).toGeneratorOf(activities.StartTime)

  val entityFileLocations:   Gen[Location.File]   = relativePaths() map Location.File.apply
  val entityFolderLocations: Gen[Location.Folder] = relativePaths() map Location.Folder.apply
  val entityLocations:       Gen[Location]        = Gen.oneOf(entityFileLocations, entityFolderLocations)
  val entityChecksums:       Gen[Checksum]        = nonBlankStrings(40, 40).map(_.value).map(Checksum.apply)

  implicit val planIdentifiers: Gen[plans.Identifier] = noDashUuid.toGeneratorOf(plans.Identifier)
  implicit val planNames:    Gen[plans.Name]    = nonBlankStrings(minLength = 5).map(_.value).toGeneratorOf[plans.Name]
  implicit val planKeywords: Gen[plans.Keyword] = nonBlankStrings(minLength = 5) map (_.value) map plans.Keyword.apply
  implicit val planDescriptions: Gen[plans.Description] = sentences().map(_.value).toGeneratorOf[plans.Description]
  implicit val planCommands:     Gen[plans.Command]     = nonBlankStrings().map(_.value).toGeneratorOf[plans.Command]
  implicit val planProgrammingLanguages: Gen[plans.ProgrammingLanguage] =
    nonBlankStrings().map(_.value).toGeneratorOf[plans.ProgrammingLanguage]
  implicit val planSuccessCodes: Gen[plans.SuccessCode] =
    positiveInts().map(_.value).toGeneratorOf[plans.SuccessCode]

  def planDatesCreated(after: InstantTinyType): Gen[plans.DateCreated] =
    timestampsNotInTheFuture(after.value).toGeneratorOf(plans.DateCreated)

  implicit val commandParameterNames: Gen[commandParameters.Name] =
    nonBlankStrings().map(_.value).toGeneratorOf[commandParameters.Name]

  implicit val commandParameterEncodingFormats: Gen[commandParameters.EncodingFormat] =
    Gen
      .oneOf(UTF_8.name(), US_ASCII.name(), ISO_8859_1.name, UTF_16.name(), nonEmptyStrings().generateOne)
      .map(commandParameters.EncodingFormat(_))
  implicit val commandParameterFolderCreation: Gen[commandParameters.FolderCreation] =
    Gen.oneOf(commandParameters.FolderCreation.yes, commandParameters.FolderCreation.no)

  lazy val generationIds: Gen[Generation.Id] = noDashUuid.toGeneratorOf(Generation.Id)

  lazy val inputEntities: Gen[InputEntity] = for {
    location <- entityLocations
    checksum <- entityChecksums
  } yield InputEntity(location, checksum)

  lazy val outputEntityFactories: Gen[Generation => OutputEntity] = for {
    location <- entityLocations
    checksum <- entityChecksums
  } yield (generation: Generation) => OutputEntity(location, checksum, generation)

  implicit val agentEntities: Gen[Agent] = cliVersions map Agent.apply

  lazy val parameterDefaultValues: Gen[ParameterDefaultValue] =
    nonBlankStrings().map(v => ParameterDefaultValue(v.value))

  lazy val parameterValueOverrides: Gen[ValueOverride] =
    nonBlankStrings().map(v => ValueOverride(v.value))

  def activityEntities(planGen: projects.DateCreated => Gen[Plan]): ActivityGenFactory =
    executionPlanners(planGen, _: projects.DateCreated).map(_.buildProvenanceUnsafe())

  def planEntities(
      parameterFactories:     CommandParameterFactory*
  )(implicit planCommandsGen: Gen[Command]): PlanGenFactory = projectDateCreated =>
    for {
      name         <- planNames
      maybeCommand <- planCommandsGen.toGeneratorOfOptions
      dateCreated  <- planDatesCreated(after = projectDateCreated)
    } yield Plan.of(name, maybeCommand, dateCreated, CommandParameters.of(parameterFactories: _*))

  def executionPlanners(planGen: projects.DateCreated => Gen[Plan], project: RenkuProject): Gen[ExecutionPlanner] =
    executionPlanners(planGen, project.topAncestorDateCreated)

  def executionPlanners(planGen:            projects.DateCreated => Gen[Plan],
                        projectDateCreated: projects.DateCreated
  ): Gen[ExecutionPlanner] = for {
    plan       <- planGen(projectDateCreated)
    author     <- personEntities
    cliVersion <- cliVersions
  } yield ExecutionPlanner.of(plan,
                              activityStartTimes(projectDateCreated).generateOne,
                              author,
                              cliVersion,
                              projectDateCreated
  )

  def executionPlannersDecoupledFromProject(planGen: projects.DateCreated => Gen[Plan]): Gen[ExecutionPlanner] =
    executionPlanners(planGen, projectCreatedDates().generateOne)

  implicit class ActivityGenFactoryOps(factory: ActivityGenFactory) {

    def generateList(projectDateCreated: projects.DateCreated): List[Activity] =
      factory(projectDateCreated).generateList()

    def multiple: List[ActivityGenFactory] = List.fill(positiveInts(5).generateOne.value)(factory)

    def withDateBefore(max: InstantTinyType): Gen[Activity] =
      factory(projects.DateCreated(max.value))
        .map(_.copy(startTime = timestamps(max = max.value).generateAs[activities.StartTime]))

    def modify(f: Activity => Activity): ActivityGenFactory =
      projectCreationDate => factory(projectCreationDate).map(f)
  }

  def toAssociationPersonAgent: Activity => Activity = toAssociationPersonAgent(personEntities.generateOne)

  def toAssociationPersonAgent(person: Person): Activity => Activity = activity =>
    activity.copy(associationFactory =
      act =>
        activity.associationFactory(act) match {
          case Association.WithRenkuAgent(_, _, plan) =>
            Association.WithPersonAgent(act, person, plan)
          case assoc: Association.WithPersonAgent => assoc
        }
    )

  def toAssociationRenkuAgent(agent: Agent): Activity => Activity =
    activity =>
      activity.copy(associationFactory = activity.associationFactory.andThen {
        case p: Association.WithPersonAgent =>
          Association.WithRenkuAgent(p.activity, agent, p.plan)
        case p: Association.WithRenkuAgent =>
          p.copy(agent = agent)
      })

  def setPlanCreator(person: Person): Activity => Activity =
    activity =>
      activity.copy(associationFactory = activity.associationFactory.andThen {
        case p: Association.WithPersonAgent =>
          Association.WithPersonAgent(p.activity, p.agent, p.plan.copy(creators = Set(person)))
        case p: Association.WithRenkuAgent =>
          Association.WithRenkuAgent(p.activity, p.agent, p.plan.copy(creators = Set(person)))
      })

  implicit class PlanGenFactoryOps(factory: PlanGenFactory) {

    def modify(f: Plan => Plan): PlanGenFactory =
      projectCreationDate => factory(projectCreationDate).map(f)
  }

  def replacePlanName(to: plans.Name): Plan => Plan = _.copy(name = to)

  def replacePlanKeywords(to: List[plans.Keyword]): Plan => Plan = _.copy(keywords = to)

  def replacePlanDesc(to: Option[plans.Description]): Plan => Plan = _.copy(maybeDescription = to)

  def replacePlanDateCreated(to: plans.DateCreated): Plan => Plan = _.copy(dateCreated = to)
}
