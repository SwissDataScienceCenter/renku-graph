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

import cats.syntax.all._
import StepPlan.CommandParameters
import StepPlan.CommandParameters.CommandParameterFactory
import cats.data.{Kleisli, NonEmptyList}
import generators.EntitiesGenerators.{ActivityGenFactory, CompositePlanGenFactory, PlanGenFactory, ProjectBasedGenFactory, StepPlanGenFactory}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{noDashUuid, nonBlankStrings, nonEmptyStrings, positiveInts, relativePaths, sentences, timestampsNotInTheFuture}
import io.renku.graph.model.GraphModelGenerators.{cliVersions, projectCreatedDates}
import io.renku.graph.model._
import io.renku.graph.model.commandParameters.ParameterDefaultValue
import io.renku.graph.model.entityModel.{Checksum, Location}
import io.renku.graph.model.parameterValues.ValueOverride
import io.renku.graph.model.plans.Command
import io.renku.graph.model.testentities.Entity.{InputEntity, OutputEntity}
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
  def planResourceIds(implicit renkuUrl: RenkuUrl): Gen[plans.ResourceId] = planIdentifiers.map(plans.ResourceId(_))
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

  def planDatesCreatedK: Kleisli[Gen, InstantTinyType, plans.DateCreated] =
    Kleisli(planDatesCreated)

  implicit val commandParameterNames: Gen[commandParameters.Name] =
    nonBlankStrings().map(_.value).toGeneratorOf[commandParameters.Name]

  implicit val commandParameterEncodingFormats: Gen[commandParameters.EncodingFormat] =
    Gen
      .oneOf(UTF_8.name(), US_ASCII.name(), ISO_8859_1.name, UTF_16.name(), nonEmptyStrings().generateOne)
      .map(commandParameters.EncodingFormat(_))
  implicit val commandParameterFolderCreation: Gen[commandParameters.FolderCreation] =
    Gen.oneOf(commandParameters.FolderCreation.yes, commandParameters.FolderCreation.no)

  val commandParameterDescription: Gen[commandParameters.Description] =
    sentences().map(s => commandParameters.Description(s.value))

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

  def activityEntities(planGen: StepPlanGenFactory): ActivityGenFactory =
    executionPlanners(planGen, _: projects.DateCreated).map(_.buildProvenanceUnsafe())

  def planEntities(
      parameterFactories:     CommandParameterFactory*
  )(implicit planCommandsGen: Gen[Command]): PlanGenFactory =
    Kleisli(dateCreated =>
      Gen.recursive[Plan] { recurse =>
        val spGen = stepPlanEntities(parameterFactories: _*)(planCommandsGen).run(dateCreated).map(_.widen)
        val cpGen =
          compositePlanEntities(Kleisli(_ => recurse.toGeneratorOfList(min = 2, max = 12)))
            .run(dateCreated)
            .map(_.widen)
        Gen.frequency(1 -> cpGen, 4 -> spGen)
      }
    )

  def planEntitiesList(min: Int = 2, max: Int = 12, parameterFactories: List[CommandParameterFactory])(implicit
      planCommandsGen:      Gen[Command]
  ): ProjectBasedGenFactory[List[Plan]] =
    planEntities(parameterFactories: _*)(planCommandsGen).mapF(_.toGeneratorOfList(min, max))

  def stepPlanEntities(
      parameterFactories:     CommandParameterFactory*
  )(implicit planCommandsGen: Gen[Command]): StepPlanGenFactory = Kleisli(projectDateCreated =>
    for {
      name         <- planNames
      maybeCommand <- planCommandsGen.toGeneratorOfOptions
      dateCreated  <- planDatesCreated(after = projectDateCreated)
      creators     <- personEntities.toGeneratorOfList(max = 2)
    } yield Plan.of(name, maybeCommand, dateCreated, creators, CommandParameters.of(parameterFactories: _*))
  )

  def compositePlanEntities(childGen: ProjectBasedGenFactory[List[Plan]]): CompositePlanGenFactory =
    (planDatesCreatedK, childGen).flatMapN { (dateCreated, childPlans) =>
      for {
        id       <- ProjectBasedGenFactory.liftF(planIdentifiers)
        name     <- ProjectBasedGenFactory.liftF(planNames)
        creators <- ProjectBasedGenFactory.liftF(personEntities.toGeneratorOfList(max = 2))
        descr    <- ProjectBasedGenFactory.liftF(Gen.some(planDescriptions))
        kw       <- ProjectBasedGenFactory.liftF(planKeywords.toGeneratorOfList())
        cp = CompositePlan.NonModified(
               id = id,
               name = name,
               maybeDescription = descr,
               creators = creators,
               dateCreated = dateCreated,
               keywords = kw,
               plans = NonEmptyList.fromListUnsafe(childPlans),
               mappings = Nil,
               links = Nil
             )
        ml <- ProjectBasedGenFactory.liftF(createMappingsAndLinks(cp, childPlans))
      } yield cp.copy(mappings = ml._1, links = ml._2): CompositePlan
    }

  private def createMappingsAndLinks(cp:    CompositePlan,
                                     plans: List[Plan]
  ): Gen[(List[ParameterMapping], List[ParameterLink])] = {
    val planLen  = plans.size
    val maxN     = Gen.choose(0, math.min(5, plans.size))
    val mapPlans = maxN.flatMap(n => Gen.pick(n, plans).map(_.toList))
    val linkPlans = for {
      from <- Gen.atLeastOne(plans.take(planLen / 2))
      to   <- Gen.someOf(plans.diff(from))
    } yield from.zip(to).toList

    for {
      mappingCandidates <- mapPlans
      mappings          <- mappingCandidates.flatTraverse(parameterMappingGen(cp))
      linkCandidates    <- if (planLen >= 2) linkPlans else Gen.const(List.empty[(Plan, Plan)])
      links             <- linkCandidates.traverse(parameterLinksGen)
    } yield (mappings, links.flatten)
  }

  def parameterMappingGen(parent: CompositePlan)(childPlan: Plan): Gen[List[ParameterMapping]] =
    childPlan match {
      case sp: StepPlan =>
        val candidates = sp.outputs ++ sp.inputs ++ sp.parameters
        Gen
          .choose(0, math.min(5, candidates.size))
          .flatMap(n => Gen.pick(n, candidates))
          .flatMap(_.toList.traverse { target =>
            for {
              id          <- noDashUuid.map(ParameterMapping.Identifier(_))
              mappingName <- commandParameterNames
              defval      <- parameterDefaultValues
              descr       <- Gen.some(commandParameterDescription)
            } yield ParameterMapping(
              id = id,
              name = mappingName,
              description = descr,
              defaultValue = defval.value,
              plan = parent,
              mappedParam = NonEmptyList.one(target)
            )
          })

      case cp: CompositePlan =>
        Gen
          .choose(0, math.min(5, cp.mappings.size))
          .flatMap(n => Gen.pick(n, cp.mappings))
          .flatMap(_.toList.traverse { targetMapping =>
            for {
              id          <- noDashUuid.map(ParameterMapping.Identifier(_))
              mappingName <- commandParameterNames
              defval      <- parameterDefaultValues
              descr       <- Gen.some(commandParameterDescription)
            } yield ParameterMapping(
              id = id,
              name = mappingName,
              description = descr,
              defaultValue = defval.value,
              plan = parent,
              mappedParam = NonEmptyList.one(targetMapping)
            )
          })
    }

  def parameterLinksGen(linkFor: (Plan, Plan)): Gen[Option[ParameterLink]] = {
    val (from, to) = linkFor

    val id = noDashUuid.map(ParameterLink.Identifier(_))
    val source = from match {
      case sp: StepPlan      => pickOne(sp.outputs)
      case _:  CompositePlan => Gen.const(None)
    }

    val sink = to match {
      case sp: StepPlan =>
        Gen
          .either(pickOne(sp.inputs), pickOne(sp.parameters))
          .map {
            case Right(Some(param)) => ParameterLink.Sink(param).some
            case Left(Some(in))     => ParameterLink.Sink(in).some
            case _                  => None
          }

      case _: CompositePlan => Gen.const(None)
    }

    (id, source, sink).mapN {
      case (ident, Some(src), Some(snk)) => ParameterLink(ident, src, NonEmptyList.one(snk)).some
      case _                             => None
    }
  }

  private def pickOne[A](list: Seq[A]): Gen[Option[A]] =
    if (list.isEmpty) Gen.const(None)
    else Gen.oneOf(list).map(_.some)

  def executionPlanners(planGen: StepPlanGenFactory, project: RenkuProject): Gen[ExecutionPlanner] =
    executionPlanners(planGen, project.topAncestorDateCreated)

  def executionPlanners(planGen: StepPlanGenFactory, projectDateCreated: projects.DateCreated): Gen[ExecutionPlanner] =
    for {
      plan       <- planGen.run(projectDateCreated)
      author     <- personEntities
      cliVersion <- cliVersions
    } yield ExecutionPlanner.of(plan,
                                activityStartTimes(plan.dateCreated).generateOne,
                                author,
                                cliVersion,
                                projectDateCreated
    )

  def executionPlannersDecoupledFromProject(planGen: StepPlanGenFactory): Gen[ExecutionPlanner] =
    executionPlanners(planGen, projectCreatedDates().generateOne)

  implicit class ActivityGenFactoryOps(factory: ActivityGenFactory) {

    def generateList(projectDateCreated: projects.DateCreated): List[Activity] =
      factory(projectDateCreated).generateList()

    def multiple: List[ActivityGenFactory] = List.fill(positiveInts(5).generateOne.value)(factory)

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

  def setPlanCreator(person: Person): Activity => Activity = activity =>
    activity.copy(associationFactory = activity.associationFactory.andThen {
      case p: Association.WithPersonAgent =>
        Association.WithPersonAgent(p.activity, p.agent, p.plan.replaceCreators(person :: Nil))
      case p: Association.WithRenkuAgent =>
        Association.WithRenkuAgent(p.activity, p.agent, p.plan.replaceCreators(person :: Nil))
    })
}
