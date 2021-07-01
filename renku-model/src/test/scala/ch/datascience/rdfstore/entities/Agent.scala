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

import ch.datascience.graph.model.CliVersion

final case class Agent(cliVersion: CliVersion)

object Agent {

  import ch.datascience.graph.model.RenkuBaseUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[Agent] = JsonLDEncoder.instance { entity =>
    JsonLD.entity(
      EntityId of s"https://github.com/swissdatasciencecenter/renku-python/tree/v${entity.cliVersion}",
      EntityTypes.of(
        prov / "SoftwareAgent",
        wfprov / "WorkflowEngine"
      ),
      rdfs / "label" -> s"renku ${entity.cliVersion}".asJsonLD
    )
  }
}
