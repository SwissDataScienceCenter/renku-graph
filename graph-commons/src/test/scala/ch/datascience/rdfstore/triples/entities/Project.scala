/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.rdfstore.triples
package entities

import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.projects._
import io.circe.Json
import io.circe.literal._

object Project {

  def apply(id: Id, name: Name, dateCreated: DateCreated, creator: Person.Id): Json = json"""
  {
    "@id": $id,
    "@type": [
      "http://www.w3.org/ns/prov#Location",
      "http://schema.org/Project"
    ],
    "http://schema.org/name": ${name.toString},
    "http://schema.org/dateCreated": ${dateCreated.toString}
  }""" deepMerge (creator toResource "http://schema.org/creator")

  final case class Id(renkuBaseUrl: RenkuBaseUrl, projectPath: ProjectPath) extends EntityId {
    override val value: String = ProjectResource(renkuBaseUrl, projectPath).value
  }

  def `schema:isPartOf`(projectId: Project.Id): Json = json"""{
    "http://schema.org/isPartOf": {
      "@id": ${projectId.toString}
    }
  }"""
}
