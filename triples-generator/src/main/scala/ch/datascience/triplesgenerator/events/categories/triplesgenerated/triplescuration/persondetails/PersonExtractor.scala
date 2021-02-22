/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration
package persondetails

import cats.MonadError
import cats.syntax.all._
import ch.datascience.graph.Schemas._
import ch.datascience.graph.model.users.{Email, Name, ResourceId}
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import ch.datascience.tinytypes.json.TinyTypeEncoders._
import io.circe.Json
import io.circe.optics.JsonOptics._
import io.circe.optics.JsonPath.root
import monocle.function.Plated.transform

import scala.collection.mutable

private trait PersonExtractor {
  type PersonData = (ResourceId, List[Name], List[Email])

  def extractPersons(triples: JsonLDTriples): (JsonLDTriples, Set[PersonData])
}

private class PersonExtractorImpl() extends PersonExtractor {

  override def extractPersons(triples: JsonLDTriples): (JsonLDTriples, Set[PersonData]) = {
    val persons     = mutable.HashSet[PersonData]()
    val updatedJson = transform(toJsonWithoutPersonDetails(persons))(triples.value)
    JsonLDTriples(updatedJson) -> persons.toSet
  }

  private def toJsonWithoutPersonDetails(
      persons: mutable.Set[PersonData]
  )(json:      Json): Json =
    root.`@type`.each.string.getAll(json) match {
      case types if types.contains("http://schema.org/Person") =>
        findPersonData(json) match {
          case None => json
          case Some(personData) =>
            persons.add(personData)
            removeNameAndEmail(json)
        }
      case _ => json
    }

  private def findPersonData(json: Json) =
    json
      .get[ResourceId]("@id")
      .flatMap { entityId =>
        Some(entityId, json.getValues[Name]("http://schema.org/name"), json.getValues[Email]("http://schema.org/email"))
      }

  private def removeNameAndEmail(json: Json) =
    json
      .remove(schema / "name")
      .remove(schema / "email")
      .remove(rdf / "label")
      .remove(rdfs / "label")
}

private object PersonExtractor {
  def apply[Interpretation[_]]()(implicit
      ME: MonadError[Interpretation, Throwable]
  ): PersonExtractor =
    new PersonExtractorImpl()
}
