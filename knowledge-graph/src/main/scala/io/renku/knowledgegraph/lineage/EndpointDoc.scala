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
import io.circe.literal._
import io.renku.knowledgegraph.docs.Implicits._
import io.renku.knowledgegraph.docs.model.Example.JsonExample
import io.renku.knowledgegraph.docs.model.Operation.GET
import io.renku.knowledgegraph.docs.model._
import org.http4s

object EndpointDoc {
  lazy val path: Path = Path(
    "Lineage",
    "Get the lineage of a files".some,
    GET(
      Uri / "projects" / groupParam / projectParam / "files" / locationParam / "lineage",
      http4s.Status.Ok.asDocStatus,
      Response(
        "Lineage found",
        Map(
          "json" -> MediaType(http4s.MediaType.application.json.asDocMediaType, "Sample Lineage", JsonExample(example))
        )
      )
    )
  )

  private lazy val groupParam = Parameter.in(
    "group(s)",
    Schema.String,
    description = "Group name(s). Names are url-encoded, slashes are not. (e.g. group1/group2/.../groupN)".some
  )
  private lazy val projectParam =
    Parameter.in("project name", Schema.String, "Project name".some)
  private lazy val locationParam =
    Parameter.in("location", Schema.String, "The path of the file".some)

  private val example =
    json"""{
            "lineage": {
              "edges": [
                {
                  "source": "/blob/bbdc4293b79535ecce7c143b29538f7ff01db297/data/zhbikes",
                  "target": "/commit/1aaf360c2267bedbedb81900a214e6f36be04e87"
                },
                {
                  "source": "/commit/1aaf360c2267bedbedb81900a214e6f36be04e87",
                  "target": "/blob/1aaf360c2267bedbedb81900a214e6f36be04e87/data/preprocessed/zhbikes.parquet"
                }
              ],
              "nodes": [
                {
                  "id": "/blob/bbdc4293b79535ecce7c143b29538f7ff01db297/data/zhbikes",
                  "location": "data/zhbikes",
                  "label": "data/zhbikes@bbdc4293b79535ecce7c143b29538f7ff01db297",
                  "type": "Directory"
                },
                {
                  "id": "/commit/1aaf360c2267bedbedb81900a214e6f36be04e87",
                  "location": ".renku/workflow/3144e9aa470441cf905f94105e1d27ca_python.cwl",
                  "label": "renku run python src/clean_data.py data/zhbikes data/preprocessed/zhbikes.parquet",
                  "type": "ProcessRun"
                },
                {
                  "id": "/blob/1aaf360c2267bedbedb81900a214e6f36be04e87/data/preprocessed/zhbikes.parquet",
                  "location": "data/preprocessed/zhbikes.parquet",
                  "label": "data/preprocessed/zhbikes.parquet@1aaf360c2267bedbedb81900a214e6f36be04e87",
                  "type": "File"
                }
              ]
            }
          }
        """
}
