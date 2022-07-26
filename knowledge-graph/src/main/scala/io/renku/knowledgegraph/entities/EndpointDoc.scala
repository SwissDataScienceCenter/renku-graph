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
import io.circe.literal._
import io.renku.knowledgegraph.docs.model.Example.JsonExample

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
    ),
    Parameter(
      "sort",
      In.Query,
      "to filter by entity's creation date to <= the given date: matchingScore, name, date. the sorting has to be requested by giving the sort query parameter with the property name and sorting order (asc or desc). The default order is ascending so sort=name means the same as sort=name:asc.".some,
      required = false,
      Schema.String
    ),
    Parameter("page",
              In.Query,
              "the page query parameter is optional and defaults to 1.".some,
              required = false,
              Schema.String
    ),
    Parameter("per_page",
              In.Query,
              "the per_page query parameter is optional and defaults to 20; max value is 100.".some,
              required = false,
              Schema.String
    )
  )

  private lazy val responses = Map(
    http4s.Status.Ok.asDocStatus -> Response(
      "OK",
      Map(
        "json" -> MediaType(http4s.MediaType.application.json.asDocMediaType, "Sample entities", JsonExample(example))
      ),
      responseHeaders,
      Map.empty
    )
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

  private val example =
    json"""
           [
        {
          "type": "project",
          "matchingScore": 1.0055376,
          "name": "name",
          "path": "group/subgroup/name",
          "namespace": "group/subgroup",
          "visibility": "public",
          "date": "2012-11-15T10:00:00.000Z",
          "creator": "Jan Kowalski",
          "keywords": [
            "keyword1",
            "keyword2"
          ],
          "description": "desc",
          "_links": [
            {
              "rel": "details",
              "href": "http://t:5511/projects/group/subgroup/name"
            }
          ]
        },
        {
          "type": "dataset",
          "matchingScore": 3.364836,
          "name": "name",
          "visibility": "public",
          "date": "2012-11-15T10:00:00.000Z", 
          "creators": [
            "Jan Kowalski",
            "Zoe"
          ],
          "keywords": [
            "keyword1",
            "keyword2"
          ],
          "description": "desc",
          "images": [
            {
              "location": "image.png",
              "_links":[
                {
                  "rel":  "view",
                  "href": "https://renkulab.io/gitlab/project_path/raw/master/data/mniouUnmal/image.png"
                }
              ]
            }
          ],
          "_links": [
            {
              "rel": "details",
              "href": "http://t:5511/datasets/122334344"
            }
          ]
        },
        {
          "type": "workflow",
          "matchingScore": 5.364836,
          "name": "name",
          "visibility": "public",
          "date": "2012-11-15T10:00:00.000Z",
          "keywords": [
            "keyword1",
            "keyword2"
          ],
          "description": "desc",
          "_links": []
        },
        {
          "type": "person",
          "matchingScore": 4.364836,
          "name": "name",
          "_links": []
        }
        ]
          """
}
