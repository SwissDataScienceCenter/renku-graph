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

package io.renku.entities.searchgraphs.commands

import io.renku.entities.searchgraphs.{Link, PersonInfo}
import io.renku.graph.model.Schemas.{rdf, renku}
import io.renku.graph.model.entities.Person
import io.renku.jsonld.syntax._
import io.renku.triplesstore.client.model.QuadsEncoder
import io.renku.triplesstore.client.syntax._

private object Encoders {

  implicit val personInfoEncoder: QuadsEncoder[PersonInfo] = QuadsEncoder.instance {
    case PersonInfo(resourceId, name) =>
      List(
        DatasetsQuad(resourceId, rdf / "type", Person.Ontology.typeClass.id),
        DatasetsQuad(resourceId, Person.Ontology.name, name.asObject)
      )
  }

  implicit val linkEncoder: QuadsEncoder[Link] = QuadsEncoder.instance { case Link(resourceId, dataset, project) =>
    List(
      DatasetsQuad(resourceId, rdf / "type", renku / "DatasetProjectLink"),
      DatasetsQuad(resourceId, renku / "project", project.asEntityId),
      DatasetsQuad(resourceId, renku / "dataset", dataset.asEntityId)
    )
  }
}
