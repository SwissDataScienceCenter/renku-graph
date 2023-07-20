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

package io.renku.knowledgegraph.ontology

import cats.MonadThrow
import cats.implicits._
import io.circe.literal._
import io.renku.http.ErrorMessage
import io.renku.http.ErrorMessage._
import io.renku.jsonld.parser._
import io.renku.knowledgegraph.docs
import io.renku.knowledgegraph.docs.model.Operation.GET
import io.renku.knowledgegraph.docs.model._

object EndpointDocs {
  def apply[F[_]: MonadThrow]: F[docs.EndpointDocs] = new EndpointDocsImpl().pure[F].widen
}

private class EndpointDocsImpl() extends docs.EndpointDocs {

  override lazy val path: Path = Path(
    GET(
      "Knowledge Graph Ontology",
      "Returns details information about ontology used in the Knowledge Graph",
      Uri / "ontology",
      Status.Ok -> Response("Ontology",
                            Contents(MediaType.`text/html`, MediaType.`application/ld+json`("Sample response", example))
      ),
      Status.InternalServerError -> Response("Error",
                                             Contents(MediaType.`text/html`,
                                                      MediaType.`application/json`("Reason", ErrorMessage("Message"))
                                             )
      )
    )
  )

  private lazy val example = parse(json"""
    [
      {
        "@id" : "https://swissdatasciencecenter.github.io/renku-ontology",
        "@type" : "http://www.w3.org/2002/07/owl#Ontology",
        "http://www.w3.org/2002/07/owl#imports" : [
          {
            "@id" : "http://www.w3.org/ns/oa#"
          }
        ]
      },
      {
        "@id" : "http://ksuefnmujl:3230/ypwx/kMs_-Prju/ev/xp/Leaf",
        "@type" : "http://www.w3.org/2002/07/owl#Class"
      },
      {
        "@id" : "http://ksuefnmujl:3230/ypwx/kMs_-Prju/ev/xp/name",
        "@type" : "http://www.w3.org/2002/07/owl#DatatypeProperty",
        "http://www.w3.org/2000/01/rdf-schema#domain" : [
          {
            "@id" : "http://ksuefnmujl:3230/ypwx/kMs_-Prju/ev/xp/Leaf"
          }
        ],
        "http://www.w3.org/2000/01/rdf-schema#range" : [
          {
            "@id" : "http://www.w3.org/2001/XMLSchema#string"
          }
        ]
      },
      {
        "@id" : "http://ksuefnmujl:3230/ypwx/kMs_-Prju/ev/xp/number",
        "@type" : "http://www.w3.org/2002/07/owl#DatatypeProperty",
        "http://www.w3.org/2000/01/rdf-schema#domain" : [
          {
            "@id" : "http://ksuefnmujl:3230/ypwx/kMs_-Prju/ev/xp/Leaf"
          }
        ],
        "http://www.w3.org/2000/01/rdf-schema#range" : [
          {
            "@id" : "http://www.w3.org/2001/XMLSchema#number"
          }
        ]
      }
    ]""").fold(throw _, identity)
}
