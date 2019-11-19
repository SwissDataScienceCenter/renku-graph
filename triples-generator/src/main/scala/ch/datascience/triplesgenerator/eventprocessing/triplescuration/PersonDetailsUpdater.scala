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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration

import cats.MonadError
import ch.datascience.graph.model.users.{Email, Id, Name}
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.tinytypes.TinyType
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples.Update
import io.circe.Decoder.decodeList
import io.circe.Encoder.encodeList
import io.circe.optics.JsonOptics._
import io.circe.optics.JsonPath._
import io.circe.{Decoder, Encoder, Json}
import monocle.function.Plated

import scala.language.higherKinds

private class PersonDetailsUpdater[Interpretation[_]]()(implicit ME: MonadError[Interpretation, Throwable]) {

  import PersonDetailsUpdater._

  def curate(curatedTriples: CuratedTriples): Interpretation[CuratedTriples] = ME.catchNonFatal(
    removePersonsAttributes
      .andThen {
        case (newTriples, persons) => newTriples -> prepareUpdates(persons)
      }
      .andThen {
        case (newTriples, newUpdates) => CuratedTriples(newTriples, curatedTriples.updates ++ newUpdates)
      }
      .apply(curatedTriples.triples)
  )
}

private object PersonDetailsUpdater {
  final case class Person(id: Id, names: Set[Name], emails: Set[Email])

  private object removePersonsAttributes extends (JsonLDTriples => (JsonLDTriples, Set[Person])) {
    import ch.datascience.tinytypes.json.TinyTypeDecoders._
    import ch.datascience.tinytypes.json.TinyTypeEncoders._

    import scala.collection.mutable

    override def apply(triples: JsonLDTriples): (JsonLDTriples, Set[Person]) = {
      val persons     = mutable.HashSet[Person]()
      val updatedJson = Plated.transform(toJsonWithoutPersonDetails(persons))(triples.value)
      JsonLDTriples(updatedJson) -> persons.toSet
    }

    private def toJsonWithoutPersonDetails(persons: mutable.Set[Person])(json: Json): Json =
      root.`@type`.each.string.getAll(json) match {
        case types if types.contains("http://schema.org/Person") => {
          for {
            entityId <- json.get[Id]("@id") flatMap skipBlankNodes
            personNames  = json.getValues[Name]("http://schema.org/name")
            noNamesJson  = json remove "http://schema.org/name"
            personEmails = noNamesJson.getValues[Email]("http://schema.org/email")
            noEmailsJson = noNamesJson remove "http://schema.org/email"
            noLabelsJson = noEmailsJson remove "http://www.w3.org/2000/01/rdf-schema#label"
            _            = persons add Person(entityId, personNames.toSet, personEmails.toSet)
          } yield noLabelsJson
        } getOrElse json
        case _ => json
      }

    private lazy val skipBlankNodes: Id => Option[Id] = id =>
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
        root
          .selectDynamic(property)
          .as[List[T]](decodeList(valuesDecoder), encodeList(valuesEncoder))
          .getOption(json)
          .getOrElse(List.empty)
      }

      def remove(property: String): Json = root.obj.modify(_.remove(property))(json)
    }
  }

  private[triplescuration] object prepareUpdates extends (Set[Person] => List[Update]) {

    override def apply(persons: Set[Person]): List[Update] = persons.toList flatMap updates

    private lazy val updates: Person => List[Update] = {
      case Person(id, names, emails) =>
        List(
          namesDelete(id),
          namesInsert(id, names),
          emailsDelete(id),
          emailsInsert(id, emails),
          labelsDelete(id)
        )
    }

    private def namesDelete(id: Id) = {
      val resource = id.showAs[RdfResource]
      Update(
        s"Updating Person $resource schema:name",
        s"""|PREFIX schema: <http://schema.org/>
            |DELETE { $resource schema:name ?name }
            |WHERE  { $resource schema:name ?name }
            |""".stripMargin
      )
    }
    private def namesInsert(id: Id, names: Set[Name]) = {
      val resource = id.showAs[RdfResource]
      Update(
        s"Updating Person $resource schema:name",
        s"""|PREFIX schema: <http://schema.org/>
            |${`INSERT DATA`(resource, "schema:name", names)}
            |""".stripMargin
      )
    }

    private def emailsDelete(id: Id) = {
      val resource = id.showAs[RdfResource]
      Update(
        s"Updating Person $resource schema:email",
        s"""|PREFIX schema: <http://schema.org/>
            |DELETE { $resource schema:email ?email }
            |WHERE  { $resource schema:email ?email }
            |""".stripMargin
      )
    }

    private def emailsInsert(id: Id, emails: Set[Email]) = {
      val resource = id.showAs[RdfResource]
      Update(
        s"Updating Person $resource schema:email",
        s"""|PREFIX schema: <http://schema.org/>
            |${`INSERT DATA`(resource, "schema:email", emails)}
            |""".stripMargin
      )
    }

    private def labelsDelete(id: Id) = {
      val resource = id.showAs[RdfResource]
      Update(
        s"Deleting Person $resource rdfs:label",
        s"""|PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            |DELETE { $resource rdfs:label ?label }
            |WHERE  { $resource rdfs:label ?label }
            |""".stripMargin
      )
    }

    private def `INSERT DATA`[V <: TinyType](resource: String, property: String, values: Set[V]): String =
      values match {
        case set if set.isEmpty => ""
        case set                => set.map(value => s"$property '$value'").mkString(s"INSERT DATA { $resource ", " ; ", " }")
      }
  }
}
