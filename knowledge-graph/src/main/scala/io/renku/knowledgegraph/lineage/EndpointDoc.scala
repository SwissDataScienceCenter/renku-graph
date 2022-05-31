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
import io.renku.knowledgegraph.docs.model._

object EndpointDoc {
  lazy val path: Path = Path("Lineage".some, "Get the lineage of a files".some).addUri(uri).addGet(getOp)

  private lazy val uri = Uri / groupParam / projectParam / "files" / locationParam / "lineage"
  private lazy val groupParam = Parameter(
    "group(s)",
    In.Path,
    "Group name(s). Names are url-encoded, slashes are not. (e.g. group1/group2/.../groupN)".some,
    required = true,
    Schema("string")
  )
  private lazy val projectParam =
    Parameter("project name", In.Path, "Project name".some, required = true, Schema("string"))
  private lazy val locationParam =
    Parameter("location", In.Path, "The path of the file".some, required = true, Schema("string"))

  private lazy val getOp  = Operation.Get("/get".some, Nil, None, Map("200" -> response200), Nil)
  private val response200 = Response("Success", Map.empty, Map.empty, Map.empty)

}
