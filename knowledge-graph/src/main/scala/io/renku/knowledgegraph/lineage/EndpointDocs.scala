/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.lineage

import cats.syntax.all._
import io.renku.http.InfoMessage
import io.renku.http.InfoMessage._
import io.renku.knowledgegraph.docs.model.Operation.GET
import io.renku.knowledgegraph.docs.model._
import io.renku.knowledgegraph.lineage.model.Node.{Label, Location, Type}
import io.renku.knowledgegraph.lineage.model.{Edge, Lineage, Node}

object EndpointDocs {

  lazy val path: Path = Path(
    "Lineage",
    "Finds lineage of the given file".some,
    GET(
      Uri / "projects" / group / projectName / "files" / location / "lineage",
      Status.Ok -> Response("Lineage found", Contents(MediaType.`application/json`("Sample Lineage", example))),
      Status.Unauthorized -> Response(
        "Unauthorized",
        Contents(MediaType.`application/json`("Invalid token", InfoMessage("Unauthorized")))
      ),
      Status.NotFound -> Response(
        "Lineage not found",
        Contents(
          MediaType.`application/json`("Reason",
                                       InfoMessage("No lineage for project: namespace/project file: some/file")
          )
        )
      ),
      Status.InternalServerError -> Response("Error",
                                             Contents(MediaType.`application/json`("Reason", InfoMessage("Message")))
      )
    )
  )

  private lazy val group = Parameter.in(
    "group(s)",
    Schema.String,
    description = "Group name(s). Names are url-encoded, slashes are not. (e.g. group1/group2/.../groupN)".some
  )

  private lazy val projectName = Parameter.in("project name", Schema.String, "Project name".some)
  private lazy val location    = Parameter.in("location", Schema.String, "The path of the file".some)

  private val example = {

    val inputNode = Node(Location("data/zhbikes"), Label("data/zhbikes@bbdc429"), Type.Directory)

    val processNode = Node(
      Location(".renku/workflow/3144e9a_python.cwl"),
      Label("renku run python src/clean_data.py data/zhbikes data/preprocessed/zhbikes.parquet"),
      Type.ProcessRun
    )
    val outputNode =
      Node(Location("data/preprocessed/zhbikes.parquet"), Label("data/preprocessed/zhbikes.parquet@1aaf360"), Type.File)

    Lineage(
      edges = Set(
        Edge(inputNode.location, processNode.location),
        Edge(processNode.location, outputNode.location)
      ),
      nodes = Set(inputNode, processNode, outputNode)
    )
  }
}