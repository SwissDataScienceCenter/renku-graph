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

package io.renku.entities.search

import Criteria._
import EntityConverters._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{localDatesNotInTheFuture, nonBlankStrings}
import io.renku.graph.model.{persons, testentities}
import model._
import org.scalacheck.Gen
import org.scalacheck.Gen.choose
import testentities._

object Generators {

  val queryParams: Gen[Filters.Query]      = nonBlankStrings(minLength = 5).map(v => Filters.Query(v.value))
  val typeParams:  Gen[Filters.EntityType] = Gen.oneOf(Filters.EntityType.all)
  def ownedParams(userId: persons.GitLabId): Gen[Filters.Owned] = Gen.oneOf(true, false).map(Filters.Owned(_, userId))
  val sinceParams:    Gen[Filters.Since] = localDatesNotInTheFuture.toGeneratorOf(Filters.Since)
  val untilParams:    Gen[Filters.Until] = localDatesNotInTheFuture.toGeneratorOf(Filters.Until)
  val matchingScores: Gen[MatchingScore] = choose(MatchingScore.min.value, 10f).toGeneratorOf(MatchingScore)

  val modelProjects: Gen[model.Entity.Project] = anyProjectEntities.map(_.to[model.Entity.Project])
  val modelDatasets: Gen[model.Entity.Dataset] =
    anyRenkuProjectEntities.addDataset(datasetEntities(provenanceNonModified)).map(_.to[model.Entity.Dataset])
  val modelWorkflows: Gen[model.Entity.Workflow] =
    anyRenkuProjectEntities.withActivities(activityEntities(stepPlanEntities())) map { project =>
      val plan :: Nil = project.plans
      (plan -> project).to[model.Entity.Workflow]
    }
  val modelPersons:  Gen[model.Entity.Person] = personEntities.map(_.to[model.Entity.Person])
  val modelEntities: Gen[model.Entity]        = Gen.oneOf(modelProjects, modelDatasets, modelWorkflows, modelPersons)
}
