/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import java.time.{Instant, LocalDate}
import java.util.UUID

import cats.kernel.Semigroup
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.datasets.{IdSameAs, SameAs, UrlSameAs}
import ch.datascience.graph.model.projects.FilePath
import ch.datascience.tinytypes._
import ch.datascience.tinytypes.constraints.RelativePath
import io.renku.jsonld._
import io.renku.jsonld.syntax._

package object entities extends Schemas with EntitiesGenerators {

  final class GenerationPath private (val value: String) extends AnyVal with RelativePathTinyType
  object GenerationPath extends TinyTypeFactory[GenerationPath](new GenerationPath(_)) with RelativePath {
    def of(filePath: FilePath): GenerationPath = apply(s"tree/$filePath")
  }

  implicit val fusekiBaseUrlToEntityId: FusekiBaseUrl => EntityId = url => EntityId of url.value
  implicit val renkuBaseUrlToEntityId:  RenkuBaseUrl => EntityId  = url => EntityId of url.value

  implicit val sameAsEncoder: JsonLDEncoder[SameAs] = JsonLDEncoder.instance {
    case v: IdSameAs  => idSameAsEncoder(v)
    case v: UrlSameAs => urlSameAsEncoder(v)
  }

  private lazy val idSameAsEncoder: JsonLDEncoder[IdSameAs] = JsonLDEncoder.instance { sameAs =>
    JsonLD.entity(
      EntityId of s"_:${UUID.randomUUID()}",
      EntityTypes of (schema / "URL"),
      schema / "url" -> EntityId.of(sameAs.value).asJsonLD
    )
  }

  private lazy val urlSameAsEncoder: JsonLDEncoder[UrlSameAs] = JsonLDEncoder.instance { sameAs =>
    JsonLD.entity(
      EntityId of s"_:${UUID.randomUUID()}",
      EntityTypes of (schema / "URL"),
      schema / "url" -> sameAs.value.asJsonLD
    )
  }

  implicit def stringTTEncoder[TT <: StringTinyType]: JsonLDEncoder[TT] =
    JsonLDEncoder.instance(v => JsonLD.fromString(v.value))
  implicit def relativePathTTEncoder[TT <: RelativePathTinyType]: JsonLDEncoder[TT] =
    JsonLDEncoder.instance(v => JsonLD.fromString(v.value))
  implicit def instantTTEncoder[TT <: TinyType { type V = Instant }]: JsonLDEncoder[TT] =
    JsonLDEncoder.instance(v => JsonLD.fromInstant(v.value))
  implicit def localDateTTEncoder[TT <: TinyType { type V = LocalDate }]: JsonLDEncoder[TT] =
    JsonLDEncoder.instance(v => JsonLD.fromLocalDate(v.value))
  implicit def booleanTTEncoder[TT <: BooleanTinyType]: JsonLDEncoder[TT] =
    JsonLDEncoder.instance(v => JsonLD.fromBoolean(v.value))
  implicit def intTTEncoder[TT <: IntTinyType]: JsonLDEncoder[TT] =
    JsonLDEncoder.instance(v => JsonLD.fromInt(v.value))

  implicit class EntityOps[T](entity: T) {
    def asPartialJsonLD[S >: T](implicit converter: PartialEntityConverter[S]): Either[Exception, PartialEntity] =
      converter(entity)

    def getEntityId[S >: T](implicit converter: PartialEntityConverter[S]): Option[EntityId] =
      converter.toEntityId(entity)
  }

  implicit class PropertiesOps(x: List[(Property, JsonLD)]) {
    def merge(y: List[(Property, JsonLD)]): List[(Property, JsonLD)] =
      y.foldLeft(x) {
        case (originalList, (property, value)) =>
          val index = originalList.indexWhere(_._1 == property)

          if (index > -1) originalList.updated(index, property -> value)
          else originalList :+ (property -> value)
      }
  }

  implicit val reverseSemigroup: Semigroup[Reverse] = (x: Reverse, y: Reverse) =>
    Reverse.fromListUnsafe {
      x.properties merge y.properties
    }

}

trait Schemas {
  val prov:      Schema = Schema.from("http://www.w3.org/ns/prov", separator = "#")
  val wfprov:    Schema = Schema.from("http://purl.org/wf4ever/wfprov", separator = "#")
  val wfdesc:    Schema = Schema.from("http://purl.org/wf4ever/wfdesc", separator = "#")
  val rdfs:      Schema = Schema.from("http://www.w3.org/2000/01/rdf-schema", separator = "#")
  val xmlSchema: Schema = Schema.from("http://www.w3.org/2001/XMLSchema", separator = "#")
  val schema:    Schema = Schema.from("http://schema.org")
  val renku:     Schema = Schema.from("https://swissdatasciencecenter.github.io/renku-ontology", separator = "#")
}
