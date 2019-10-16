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

package ch.datascience.rdfstore

import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.tinytypes.StringTinyType
import ch.datascience.tinytypes.json.TinyTypeEncoders
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}

package object triples {

  val renkuBaseUrl: RenkuBaseUrl = RenkuBaseUrl("https://dev.renku.ch")

  // format: off
  def triples(parts: List[Json]*): JsonLDTriples = JsonLDTriples { json"""
    {
      "@context": {
        "dcterms": "http://purl.org/dc/terms/",
        "foaf": "http://xmlns.com/foaf/0.1/",
        "prov": "http://www.w3.org/ns/prov#",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "schema": "http://schema.org/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
      },
      "@graph": ${Json.arr(parts.flatten: _*)}
    }"""
  }
  // format: on

  implicit class OptionOps[V](maybeValue: Option[V]) {
    def to(property: String)(implicit encoder: Encoder[V]): Json =
      maybeValue
        .map(value => Json.obj(property -> value.asJson))
        .getOrElse(Json.obj())

    def to(property: String, valueType: String)(implicit encoder: Encoder[V]): Json =
      maybeValue
        .map(value => Json.obj(property -> Json.obj(("@type" -> valueType.asJson), "@value" -> value.asJson)))
        .getOrElse(Json.obj())
  }

  implicit class IdOps[ID <: EntityId](id: ID) {

    lazy val toIdJson: Json = Json.obj("@id" -> id.asJson)

    def toResource(property: String): Json = Json.obj(property -> toIdJson)
  }

  implicit class OptionIdOps[V <: EntityId](maybeValue: Option[V]) {

    def toResource[ID <: EntityId](property: String): Json =
      maybeValue
        .map(id => Json.obj(property -> id.toIdJson))
        .getOrElse(Json.obj())
  }

  implicit class ListOps[V <: EntityId](values: List[V]) {
    def toResources(property: String, toId: V => EntityId = identity): Json =
      Json.obj(
        property -> Json.arr(
          values.map(toId).map(id => id.toIdJson): _*
        )
      )
  }

  trait EntityId extends StringTinyType

  implicit def entityIdEncoder[TT <: EntityId]: Encoder[TT] = TinyTypeEncoders.stringEncoder
}
