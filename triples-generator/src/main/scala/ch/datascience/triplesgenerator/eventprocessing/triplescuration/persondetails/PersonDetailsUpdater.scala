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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails

import cats.MonadError
import cats.data.NonEmptyList
import cats.implicits._
import ch.datascience.graph.model.users.{Email, Name, ResourceId}
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples
import io.circe.Decoder.decodeList
import io.circe.Encoder.encodeList
import io.circe.optics.JsonOptics._
import io.circe.optics.JsonPath._
import io.circe.{Decoder, Encoder, Json}
import monocle.function.Plated

import scala.language.higherKinds

private[triplescuration] class PersonDetailsUpdater[Interpretation[_]](
    updatesCreator: UpdatesCreator
)(implicit ME:      MonadError[Interpretation, Throwable]) {

  import PersonDetailsUpdater._
  import updatesCreator._

  def curate(curatedTriples: CuratedTriples): Interpretation[CuratedTriples] =
    for {
      triplesAndPersons <- removePersonsAttributes(curatedTriples.triples)
      (newTriples, persons) = triplesAndPersons
      newUpdates <- ME.catchNonFatal(prepareUpdates(persons))
    } yield CuratedTriples(newTriples, curatedTriples.updates ++ newUpdates)

  private object removePersonsAttributes extends (JsonLDTriples => Interpretation[(JsonLDTriples, Set[Person])]) {
    import ch.datascience.tinytypes.json.TinyTypeDecoders._
    import ch.datascience.tinytypes.json.TinyTypeEncoders._

    import scala.collection.mutable

    override def apply(triples: JsonLDTriples): Interpretation[(JsonLDTriples, Set[Person])] = {
      val persons = mutable.HashSet[Person]()
      for {
        updatedJson <- Plated.transformM(toJsonWithoutPersonDetails(persons))(triples.value)
      } yield JsonLDTriples(updatedJson) -> persons.toSet
    }

    private def toJsonWithoutPersonDetails(persons: mutable.Set[Person])(json: Json): Interpretation[Json] =
      root.`@type`.each.string.getAll(json) match {
        case types if types.contains("http://schema.org/Person") =>
          findPersonData(json) match {
            case None => json.pure[Interpretation]
            case Some(personData) =>
              for {
                foundPerson <- personData.toPerson
                _ = persons add foundPerson
              } yield removeNameAndEmail(foundPerson.id, json)
          }
        case _ => json.pure[Interpretation]
      }

    private def findPersonData(json: Json) =
      for {
        entityId <- json.get[ResourceId]("@id") flatMap skipBlankNodes
        personNames  = json.getValues[Name]("http://schema.org/name")
        personEmails = json.getValues[Email]("http://schema.org/email")
      } yield (entityId, personNames, personEmails)

    private implicit class PersonDataOps(personData: (ResourceId, List[Name], List[Email])) {

      private val (entityId, personNames, personEmails) = personData

      lazy val toPerson: Interpretation[Person] = for {
        nonEmptyNames <- toNonEmptyList(personNames)
      } yield Person(entityId, nonEmptyNames, personEmails.toSet)

      private lazy val toNonEmptyList: List[Name] => Interpretation[NonEmptyList[Name]] = {
        case Nil =>
          ME.raiseError {
            new Exception(s"No names for person with '$entityId' id found in generated JSON-LD")
          }
        case first +: other =>
          NonEmptyList.of(first, other: _*).pure[Interpretation]
      }
    }

    private def removeNameAndEmail(resourceId: ResourceId, json: Json) =
      json
        .remove("http://schema.org/name")
        .remove("http://schema.org/email")
        .remove("http://www.w3.org/2000/01/rdf-schema#label")

    private lazy val skipBlankNodes: ResourceId => Option[ResourceId] = id =>
      if (id.value startsWith "_") None
      else Some(id)

    private implicit class JsonOps(json: Json) {

      def get[T](property: String)(implicit decode: Decoder[T], encode: Encoder[T]): Option[T] =
        root.selectDynamic(property).as[T].getOption(json)

      def getValues[T](
          property:      String
      )(implicit decode: Decoder[T], encode: Encoder[T]): List[T] = {
        import io.circe.literal._

        val valuesDecoder: Decoder[T] = _.downField("@value").as[T]
        val valuesEncoder: Encoder[T] = Encoder.instance[T](value => json"""{"@value": $value}""")
        val findListOfValues = root
          .selectDynamic(property)
          .as[List[T]](decodeList(valuesDecoder), encodeList(valuesEncoder))
          .getOption(json)
        val findSingleValue = root
          .selectDynamic(property)
          .as[T](valuesDecoder, valuesEncoder)
          .getOption(json)

        findListOfValues orElse findSingleValue.map(List(_)) getOrElse List.empty
      }

      def remove(property: String): Json = root.obj.modify(_.remove(property))(json)
    }
  }
}

private[triplescuration] object PersonDetailsUpdater {

  def apply[Interpretation[_]]()(
      implicit ME: MonadError[Interpretation, Throwable]
  ): PersonDetailsUpdater[Interpretation] = new PersonDetailsUpdater[Interpretation](new UpdatesCreator)

  final case class Person(id: ResourceId, names: NonEmptyList[Name], emails: Set[Email])
}
