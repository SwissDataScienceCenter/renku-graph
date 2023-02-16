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

package io.renku.cli.model.generators

import io.renku.cli.model.CliProject
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.images.Image
import io.renku.graph.model.{RenkuTinyTypeGenerators, RenkuUrl}
import org.scalacheck.Gen

import java.time.Instant

trait ProjectGenerators {

  def projectPlanGen(minCreated: Instant)(implicit renkuUrl: RenkuUrl): Gen[CliProject.ProjectPlan] =
    Gen.frequency(
      7 -> PlanGenerators.planGen(minCreated).map(CliProject.ProjectPlan.apply),
      1 -> PlanGenerators.compositePlanGen(minCreated).map(CliProject.ProjectPlan.apply),
      1 -> PlanGenerators.workflowFilePlanGen(minCreated).map(CliProject.ProjectPlan.apply),
      1 -> PlanGenerators.workflowFileCompositePlanGen(minCreated).map(CliProject.ProjectPlan.apply)
    )

  def projectGen(minCreated: Instant)(implicit renkuUrl: RenkuUrl): Gen[CliProject] = for {
    id          <- RenkuTinyTypeGenerators.projectResourceIds
    name        <- RenkuTinyTypeGenerators.projectNames.toGeneratorOfOptions
    description <- RenkuTinyTypeGenerators.projectDescriptions.toGeneratorOfOptions
    dateCreated <- RenkuTinyTypeGenerators.projectCreatedDates(minCreated)
    creator     <- PersonGenerators.cliPersonGen.toGeneratorOfOptions
    keywords    <- RenkuTinyTypeGenerators.projectKeywords.toGeneratorOfList(max = 3)
    images     <- RenkuTinyTypeGenerators.imageUris.toGeneratorOfList(max = 3).map(uris => Image.projectImage(id, uris))
    plans      <- projectPlanGen(minCreated).toGeneratorOfList(max = 3)
    activities <- ActivityGenerators.activityGen(minCreated).toGeneratorOfList(max = 3).map(_.sortBy(_.startTime))
    datasets   <- DatasetGenerators.datasetGen.toGeneratorOfList(max = 3)
    agentVersion  <- RenkuTinyTypeGenerators.cliVersions.toGeneratorOfOptions
    schemaVersion <- RenkuTinyTypeGenerators.projectSchemaVersions.toGeneratorOfOptions
  } yield CliProject(
    id,
    name,
    description,
    dateCreated,
    creator,
    keywords.toSet,
    images,
    plans,
    datasets,
    activities,
    agentVersion,
    schemaVersion
  )
}

object ProjectGenerators extends ProjectGenerators
