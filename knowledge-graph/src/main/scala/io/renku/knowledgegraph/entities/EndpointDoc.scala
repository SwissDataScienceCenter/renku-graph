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

package io.renku.knowledgegraph.entities

import cats.implicits._
import io.renku.knowledgegraph.docs.Implicits._
import io.renku.knowledgegraph.docs.model.Path.OpMapping
import io.renku.knowledgegraph.docs.model._
import org.http4s

object EndpointDoc {
  lazy val path: Path = Path("Entities", "Entities such as datasets, users, etc.".some, opMapping)

  private lazy val operation = Operation.Get("Get various kinds of entities".some, parameters, None, responses, Nil)
  private lazy val opMapping: OpMapping = OpMapping(template, operation)
  private lazy val uri      = Uri / "entities"
  private lazy val template = Uri.getTemplate(uri.parts)
  private lazy val parameters = List(
    Parameter("query",
              In.Query,
              "to filter by matching field (e.g., title, keyword, description, etc.)".some,
              required = false,
              Schema.String
    ),
    Parameter(
      "type",
      In.Query,
      "to filter by entity type(s); allowed values: project, dataset, workflow, and person; multiple type parameters allowed".some,
      required = false,
      Schema.String
    ),
    Parameter(
      "creator",
      In.Query,
      "to filter by creator(s); the filter would require creator's name; multiple creator parameters allowed".some,
      required = false,
      Schema.String
    ),
    Parameter(
      "visibility",
      In.Query,
      "to filter by visibility(ies) (restricted vs. public); allowed values: public, internal, private; multiple visibility parameters allowed".some,
      required = false,
      Schema.String
    ),
    Parameter("since",
              In.Query,
              "to filter by entity's creation date to >= the given date".some,
              required = false,
              Schema.String
    ),
    Parameter("until",
              In.Query,
              "to filter by entity's creation date to <= the given date".some,
              required = false,
              Schema.String
    )
  )

  private lazy val responses = Map(
    http4s.Status.Ok.asDocStatus -> Response("OK", Map.empty, responseHeaders, Map.empty)
  )

  private lazy val responseHeaders = Map(
    "Total"       -> Header("The total number of entities".some, Schema.Integer),
    "Total-Pages" -> Header("The total number of pages".some, Schema.Integer),
    "Per-Page"    -> Header("The number of items per page".some, Schema.Integer),
    "Page"        -> Header("The index of the current page (starting at 1)".some, Schema.Integer),
    "Next-Page"   -> Header("The index of the next page (optional)".some, Schema.Integer),
    "Prev-Page"   -> Header("The index of the previous page (optional)".some, Schema.Integer),
    "Link" -> Header("The set of prev/next/first/last link headers (prev and next are optional)".some, Schema.String)
  )
}
