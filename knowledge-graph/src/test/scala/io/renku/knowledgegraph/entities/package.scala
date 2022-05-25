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

package io.renku.knowledgegraph

import entities.model._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{localDatesNotInTheFuture, nonBlankStrings}
import io.renku.graph.model.testentities.{Entity => _, _}
import io.renku.graph.model.{RenkuBaseUrl, testentities}
import org.scalacheck.Gen
import org.scalacheck.Gen.choose

package object entities {
  import Endpoint.Criteria._

  val queryParams: Gen[Filters.Query]      = nonBlankStrings(minLength = 5).map(v => Filters.Query(v.value))
  val typeParams:  Gen[Filters.EntityType] = Gen.oneOf(Filters.EntityType.all)
  val sinceParams: Gen[Filters.Since]      = localDatesNotInTheFuture.toGeneratorOf(Filters.Since)

  val matchingScores: Gen[MatchingScore] = choose(MatchingScore.min.value, 10f).toGeneratorOf(MatchingScore)

  val modelProjects: Gen[model.Entity.Project] = anyProjectEntities.map(_.to[model.Entity.Project])
  val modelDatasets: Gen[model.Entity.Dataset] =
    anyRenkuProjectEntities.addDataset(datasetEntities(provenanceNonModified)).map(_.to[model.Entity.Dataset])
  val modelWorkflows: Gen[model.Entity.Workflow] =
    anyRenkuProjectEntities.withActivities(activityEntities(planEntities())) map { project =>
      val plan :: Nil = project.plans.toList
      (plan -> project).to[model.Entity.Workflow]
    }
  val modelPersons:  Gen[model.Entity.Person] = personEntities.map(_.to[model.Entity.Person])
  val modelEntities: Gen[model.Entity]        = Gen.oneOf(modelProjects, modelDatasets, modelWorkflows, modelPersons)

  private[entities] implicit def projectConverter[P <: testentities.Project]: P => Entity.Project = project =>
    Entity.Project(
      MatchingScore.min,
      project.path,
      project.name,
      project.visibility,
      project.dateCreated,
      project.maybeCreator.map(_.name),
      project.keywords.toList.sorted,
      project.maybeDescription
    )

  private[entities] implicit def datasetConverter[P <: testentities.Project]
      : ((testentities.Dataset[testentities.Dataset.Provenance], P)) => Entity.Dataset = { case (dataset, project) =>
    Entity.Dataset(
      MatchingScore.min,
      dataset.identification.identifier,
      dataset.identification.name,
      project.visibility,
      dataset.provenance.date,
      dataset.provenance.creators.map(_.name).toList.sorted,
      dataset.additionalInfo.keywords.sorted,
      dataset.additionalInfo.maybeDescription,
      dataset.additionalInfo.images,
      exemplarProjectPath = project.path
    )
  }

  private[entities] implicit class ProjectDatasetOps[PROV <: testentities.Dataset.Provenance,
                                                     +P <: testentities.Project
  ](datasetAndProject: (testentities.Dataset[PROV], P))(implicit renkuBaseUrl: RenkuBaseUrl) {
    def to[T](implicit convert: ((testentities.Dataset[PROV], P)) => T): T = convert(datasetAndProject)
  }

  private[entities] implicit def planConverter[P <: testentities.Project]
      : ((testentities.Plan, P)) => Entity.Workflow = { case (plan, project) =>
    Entity.Workflow(MatchingScore.min,
                    plan.name,
                    project.visibility,
                    plan.dateCreated,
                    plan.keywords.sorted,
                    plan.maybeDescription
    )
  }

  private[entities] implicit class ProjectPlanOps[+P <: testentities.Project](planAndProject: (testentities.Plan, P)) {
    def to[T](implicit convert: ((testentities.Plan, P)) => T): T = convert(planAndProject)
  }

  private[entities] implicit def personConverter[P <: testentities.Person]: P => Entity.Person = person =>
    Entity.Person(MatchingScore.min, person.name)
}
