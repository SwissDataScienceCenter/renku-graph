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

package ch.datascience.graphservice.lineage

import ch.datascience.graphservice.lineage.model._
import sangria.macros.derive._
import sangria.schema._

private object schema {

  private implicit val NodeType: ObjectType[Unit, Node] = ObjectType(
    name        = "node",
    description = "Lineage node",
    fields[Unit, Node](
      Field("id", StringType, Some("Node identifier"), resolve = _.value.id),
      Field("label", StringType, Some("Node label"), resolve   = _.value.label)
    )
  )

  private implicit val EdgeType: ObjectType[Unit, Edge] = ObjectType(
    name        = "edge",
    description = "Lineage edge",
    fields = fields[Unit, Edge](
      Field("id", StringType, Some("Edge identifier"), resolve = _.value.id)
    )
  )

  val LineageType: ObjectType[Unit, Lineage] = deriveObjectType[Unit, Lineage](
    ObjectTypeDescription("Lineage"),
    DocumentField("nodes", "Lineage nodes"),
    DocumentField("edges", "Lineage edges")
  )
}
