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

package ch.datascience.rdfstore.entities

import ch.datascience.generators.CommonGraphGenerators.cliVersions
import ch.datascience.generators.Generators.Implicits.GenOps
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators.{projectCreatedDates, projectNames, projectPaths, projectSchemaVersions, userAffiliations, userEmails, userGitLabIds, userNames}
import ch.datascience.graph.model.users.{Email, GitLabId}
import eu.timepit.refined.api.Refined.unsafeApply
import eu.timepit.refined.auto._
import org.scalacheck.Gen

object EntitiesGenerators extends EntitiesGenerators

trait EntitiesGenerators {

  val activityIds:            Gen[Activity.Id]        = Gen.uuid.map(uuid => Activity.Id(uuid.toString))
  val activityStartTimes:     Gen[Activity.StartTime] = timestampsNotInTheFuture.map(Activity.StartTime.apply)
  val locations:              Gen[Location]           = relativePaths() map Location.apply
  val cwlFiles:               Gen[WorkflowFile]       = nonBlankStrings() map (n => WorkflowFile.cwl(unsafeApply(s"$n.cwl")))
  val yamlFiles:              Gen[WorkflowFile]       = nonBlankStrings() map (n => WorkflowFile.yaml(unsafeApply(s"$n.yaml")))
  implicit val workflowFiles: Gen[WorkflowFile]       = Gen.oneOf(cwlFiles, yamlFiles)

  implicit val runPlanCommands: Gen[RunPlan.Command] = nonBlankStrings() map (c => RunPlan.Command(c.value))

  implicit val projectEntities: Gen[Project] = for {
    path         <- projectPaths
    name         <- projectNames
    agent        <- cliVersions
    dateCreated  <- projectCreatedDates
    maybeCreator <- persons.toGeneratorOfOptions
    members      <- persons(userGitLabIds.toGeneratorOfSomes).toGeneratorOfSet(minElements = 0)
    version      <- projectSchemaVersions
  } yield Project(path,
                  name,
                  agent,
                  dateCreated,
                  maybeCreator,
                  maybeVisibility = None,
                  members = members,
                  maybeParentProject = None,
                  version
  )

  implicit val agentEntities: Gen[Agent] = cliVersions map Agent.apply

  implicit val activityEntities: Gen[Activity] = for {
    activityId <- activityIds
    startTime  <- activityStartTimes
    committer  <- persons
    project    <- projectEntities
    agent      <- agentEntities
  } yield Activity(activityId, startTime, committer, project, agent)

  implicit lazy val persons: Gen[Person] = persons()

  def persons(
      maybeGitLabIds: Gen[Option[GitLabId]] = userGitLabIds.toGeneratorOfNones,
      maybeEmails:    Gen[Option[Email]] = userEmails.toGeneratorOfOptions
  ): Gen[Person] = for {
    name             <- userNames
    maybeEmail       <- maybeEmails
    maybeAffiliation <- userAffiliations.toGeneratorOfOptions
    maybeGitLabId    <- maybeGitLabIds
  } yield Person(name, maybeEmail, maybeAffiliation, maybeGitLabId)
}
