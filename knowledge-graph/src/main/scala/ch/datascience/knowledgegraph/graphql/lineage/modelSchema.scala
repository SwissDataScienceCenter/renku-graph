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

package ch.datascience.knowledgegraph.graphql.lineage

import ch.datascience.knowledgegraph.graphql.lineage.model._
import sangria.schema._

private object modelSchema {

  private implicit val nodeType: ObjectType[Unit, Node] = ObjectType(
    name        = "node",
    description = "Lineage node",
    fields[Unit, Node](
      Field("id", StringType, Some("Node identifier"), resolve = _.value.id.toString),
      Field("label", StringType, Some("Node label"), resolve   = _.value.label.toString)
    )
  )

  private implicit val edgeType: ObjectType[Unit, Edge] = ObjectType(
    name        = "edge",
    description = "Lineage edge",
    fields = fields[Unit, Edge](
      Field("source", StringType, Some("Source node"), resolve = _.value.source.id.toString),
      Field("target", StringType, Some("Target node"), resolve = _.value.target.id.toString)
    )
  )

  val lineageType: ObjectType[Unit, Lineage] = ObjectType[Unit, Lineage](
    name        = "lineage",
    description = "Lineage",
    fields = fields[Unit, Lineage](
      Field("nodes", ListType(nodeType), Some("Lineage nodes"), resolve = _.value.nodes.toList),
      Field("edges", ListType(edgeType), Some("Lineage edges"), resolve = _.value.edges.toList)
    )
  )
}
