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

import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.{GitLabApiUrl, RenkuBaseUrl}
import ch.datascience.rdfstore.entities.CommandParameterBase.{CommandInput, CommandOutput}
import ch.datascience.rdfstore.entities.RunPlan.{Command, CommandParameters}
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._
import org.scalatest.wordspec.AnyWordSpec

class ApiTest extends AnyWordSpec {

  implicit val renkuBaseUrl: RenkuBaseUrl = renkuBaseUrls.generateOne
  val dataSetFolder         = Location("data/zhbikes")
  val dataSetFolderChecksum = entityChecksums.generateOne
  val plotData              = Location("src/plot_data.py")
  val cleanData             = Location("src/clean_data.py")
  val cleanDataChecksum     = entityChecksums.generateOne
  val bikesParquet          = Location("data/preprocessed/zhbikes.parquet")
  val cumulativePng         = Location("figs/cumulative.png")
  val gridPlotPng           = Location("figs/grid_plot.png")
  val velo2018Location      = Location("data/zhbikes/2018velo.csv")
  val velo2019Location      = Location("data/zhbikes/2019velo.csv")

  "test" in {
    val runPlan = RunPlan(
      runPlanNames.generateOne,
      Command("python"),
      CommandParameters.of(CommandInput.fromLocation(cleanData),
                           CommandInput.fromLocation(dataSetFolder),
                           CommandOutput.fromLocation(bikesParquet)
      )
    )

    implicit val gitLabApiUrl: GitLabApiUrl = gitLabUrls.generateOne.apiV4

    val project = projectEntities[Project.ForksCount.Zero]().generateOne.copy(members = Set.empty)

    val activity = ExecutionPlanner
      .of(runPlan, activityStartTimes.generateOne, persons.generateOne, project.agent, project)
      .planParameterInputsValues(
        cleanData     -> cleanDataChecksum,
        dataSetFolder -> dataSetFolderChecksum
      )
      .buildProvenanceGraph
      .fold(errors => fail(errors.toList.mkString), identity)

    val planJson     = runPlan.asJsonLD
    val activityJson = activity.asJsonLD
    println(JsonLD.arr(planJson, activityJson).flatten.fold(throw _, identity).toJson)
  }
}
