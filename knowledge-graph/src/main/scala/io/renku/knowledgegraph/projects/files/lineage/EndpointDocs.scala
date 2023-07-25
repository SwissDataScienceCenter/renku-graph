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

package io.renku.knowledgegraph.projects.files.lineage

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.data.Message
import io.renku.knowledgegraph.docs
import io.renku.knowledgegraph.docs.model.Operation.GET
import io.renku.knowledgegraph.docs.model._
import model.Node.Type.{Directory, File, ProcessRun}
import model.Node.{Label, Location}
import model.{Edge, Lineage, Node}

object EndpointDocs extends docs.EndpointDocs {

  override val path: Path = Path(
    GET(
      "Project File Lineage",
      "Finds lineage of the given file",
      Uri / "projects" / namespace / projectName / "files" / location / "lineage",
      Status.Ok -> Response("Lineage found", Contents(MediaType.`application/json`("Sample Lineage", example))),
      Status.Unauthorized -> Response(
        "Unauthorized",
        Contents(MediaType.`application/json`("Invalid token", Message.Info("Unauthorized")))
      ),
      Status.NotFound -> Response(
        "Lineage not found",
        Contents(
          MediaType.`application/json`("Reason",
                                       Message.Info("No lineage for project: namespace/project file: some/file")
          )
        )
      ),
      Status.InternalServerError -> Response("Error",
                                             Contents(MediaType.`application/json`("Reason", Message.Info("Message")))
      )
    )
  )

  private lazy val namespace = Parameter.Path(
    "namespace",
    Schema.String,
    description =
      "Namespace(s) as there might be multiple. Each namespace needs to be url-encoded and separated with a non url-encoded '/'".some
  )

  private lazy val projectName = Parameter.Path("projectName", Schema.String, "Project name".some)
  private lazy val location    = Parameter.Path("location", Schema.String, "The path of the file".some)

  private lazy val example = {

    val inputNode = Node(Location("data/zhbikes"), Label("data/zhbikes@bbdc429"), Directory)

    val processNode = Node(
      Location(".renku/workflow/3144e9a_python.cwl"),
      Label("renku run python src/clean_data.py data/zhbikes data/preprocessed/zhbikes.parquet"),
      ProcessRun
    )
    val outputNode =
      Node(Location("data/preprocessed/zhbikes.parquet"), Label("data/preprocessed/zhbikes.parquet@1aaf360"), File)

    Lineage(
      edges = Set(
        Edge(inputNode.location, processNode.location),
        Edge(processNode.location, outputNode.location)
      ),
      nodes = Set(inputNode, processNode, outputNode)
    )
  }
}
