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

package io.renku.graph.model.cli

import cats.syntax.all._
import io.renku.cli.model.CliProject
import io.renku.graph.model.{testentities, RenkuUrl}
import io.renku.graph.model.testentities.ModelOps
import io.renku.jsonld.syntax._

trait CliProjectConverters extends CliActivityConverters with CliDatasetConverters {

  def from(project: testentities.Project)(implicit renkuUrl: RenkuUrl): CliProject =
    project.fold(renkuProject, renkuProject, nonRenkuProject, nonRenkuProject)

  private def renkuProject(p: testentities.RenkuProject)(implicit renkuUrl: RenkuUrl): CliProject =
    CliProject(
      p.name.some,
      p.maybeDescription,
      p.dateCreated,
      p.maybeCreator.map(from),
      p.keywords,
      ModelOps.convertImageUris(p.asEntityId)(p.images),
      p.plans.map(from(_)).map(CliProject.ProjectPlan.apply),
      p.datasets.map(from),
      p.activities.map(from(_)),
      p.agent.some,
      p.version.some
    )

  def nonRenkuProject(p: testentities.NonRenkuProject)(implicit renkuUrl: RenkuUrl): CliProject =
    CliProject(
      p.name.some,
      p.maybeDescription,
      p.dateCreated,
      p.maybeCreator.map(from),
      p.keywords,
      ModelOps.convertImageUris(p.asEntityId)(p.images),
      plans = Nil,
      datasets = Nil,
      activities = Nil,
      agentVersion = None,
      schemaVersion = None
    )
}

object CliProjectConverters extends CliProjectConverters
