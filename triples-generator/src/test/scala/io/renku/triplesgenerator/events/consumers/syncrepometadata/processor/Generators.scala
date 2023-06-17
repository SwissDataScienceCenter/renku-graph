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

package io.renku.triplesgenerator.events.consumers.syncrepometadata.processor

import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators.{projectNames, projectPaths, projectResourceIds}
import io.renku.graph.model.projects
import org.scalacheck.Gen

private object Generators {

  def tsDataExtracts(having: projects.Path = projectPaths.generateOne): Gen[DataExtract.TS] = for {
    id   <- projectResourceIds
    name <- projectNames
  } yield DataExtract.TS(id, having, name)

  def glDataExtracts(having: projects.Path = projectPaths.generateOne): Gen[DataExtract.GL] = for {
    name <- projectNames
  } yield DataExtract.GL(having, name)

  def payloadDataExtracts(having: projects.Path = projectPaths.generateOne): Gen[DataExtract.Payload] = for {
    name <- projectNames
  } yield DataExtract.Payload(having, name)
}
