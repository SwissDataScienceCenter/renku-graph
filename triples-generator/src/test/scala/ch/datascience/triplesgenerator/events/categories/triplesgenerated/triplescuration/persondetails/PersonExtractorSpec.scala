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

import cats.data.NonEmptyList
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.users.{Affiliation, Email, Name, ResourceId}
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.rdfstore.{FusekiBaseUrl, JsonLDTriples, entities}
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import ch.datascience.tinytypes.json.TinyTypeEncoders._
import eu.timepit.refined.auto._
import io.circe.Json
import io.circe.literal.JsonStringContext
import io.circe.optics.JsonOptics._
import io.circe.optics.JsonPath.root
import io.circe.literal._
import io.renku.jsonld.{JsonLD, Property}
import io.renku.jsonld.syntax._
import monocle.function.Plated
import org.scalamock.scalatest.MockFactory
import org.scalatest.AppendedClues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.mutable
import scala.tools.nsc.interactive.Lexer.MalformedInput
import scala.util.{Failure, Success, Try}

class PersonExtractorSpec extends AnyWordSpec with should.Matchers with MockFactory with AppendedClues {

  "extractPersons" should {

//    "remove name and email properties from all the Person entities found in the given Json " in new TestCase {
//      val jsonTriples = JsonLDTriples {
//        nonModifiedDataSetCommit(
//          committer = entities.Person(userNames.generateOne, userEmails.generateOne)
//        )(
//          projectPath = projectPaths.generateOne,
//          maybeProjectCreator = entities.Person(userNames.generateOne, userEmails.generateOne).some
//        )(
//          datasetCreators = nonEmptyList(entities.EntitiesGenerators.persons, minElements = 5, maxElements = 10)
//            .retryUntil(atLeastOneWithoutEmail)
//            .generateOne
//            .toList
//            .toSet
//        ).toJson
//      }
//
//      val Success((updatedTriples, foundPersons)) = personExtractor extractPersons jsonTriples
//
//      val originalPersons = jsonTriples.collectAllPersons
//
//      val actual   = foundPersons.map(person => (person.id, person.name.some, person.maybeEmail))
//      val expected = originalPersons.map(person => (person.id, person.maybeName, person.maybeEmail))
//      actual shouldBe expected
//
//      val updatedPersons = updatedTriples.collectAllPersons
//
//      updatedPersons.foldLeft(true) {
//        case (acc, Person(_, None, None, _)) => acc
//        case _                               => false
//      } shouldBe true withClue "One person contained a name or an email"
//    }

    "choose a single name when there are multiple" in new TestCase {

      val userName       = userNames.generateOne
      val secondUserName = userNames.generateOne

      val jsonTriples = JsonLDTriples {
        val dataSetCommit = nonModifiedDataSetCommit(
          committer = entities.Person(userName, userEmails.generateOne)
        )(
          projectPath = projectPaths.generateOne,
          maybeProjectCreator = entities.Person(userNames.generateOne, userEmails.generateOne).some
        )(
          datasetCreators = nonEmptyList(entities.EntitiesGenerators.persons, minElements = 5, maxElements = 10)
            .retryUntil(atLeastOneWithoutEmail)
            .generateOne
            .toList
            .toSet
        )

        val personJson = json"""[  {
    "@id" : "mailto:ooVt2tf%@hpirz",
    "@type" : [
      "http://www.w3.org/ns/prov#Person",
      "http://schema.org/Person"
    ],
    "http://www.w3.org/2000/01/rdf-schema#label" : {
      "@value" : "es mqlb"
    },
    "http://schema.org/affiliation" : {
      "@value" : "tefotcvry"
    },
    "http://schema.org/email" : {
      "@value" : "ooVt2tf%@hpirz"
    },
    "http://schema.org/name" : [
       { "@value": ${userName.value}},
       { "@value": ${secondUserName.value}}
    ]
  }]""" //,

        val togetherWithNewPerson: Either[JsonLD.MalformedJsonLD, Option[Json]] = dataSetCommit.asJsonLD.flatten.map {
          flattened =>
            for {
              rest        <- flattened.toJson.asArray
              personArray <- personJson.asArray
            } yield Json.fromValues(personArray ++ rest)

        }
        val Right(Some(value)) = togetherWithNewPerson
        value
      }

      val Success((updatedTriples, foundPersons)) = personExtractor extractPersons jsonTriples

    }

    "do not modify person objects if there are no names and emails" in new TestCase {
      val triples = JsonLDTriples(
        removePersonsNames(
          JsonLD
            .arr(
              entities.Person(userNames.generateOne, maybeEmail = None, maybeAffiliation = None).asJsonLD
            )
            .toJson
        )
      )

      val Success((updatedTriples, foundPersons)) = personExtractor extractPersons triples

      updatedTriples       shouldBe triples
      foundPersons.isEmpty shouldBe true
    }

    "fail if there's a Person entity without a name" in new TestCase {

      val noNamesJson = JsonLDTriples(
        removePersonsNames(
          JsonLD
            .arr(
              entities.Person(userNames.generateOne, userEmails.generateOne).asJsonLD
            )
            .toJson
        )
      )

      val result = personExtractor extractPersons noNamesJson

      result                       shouldBe a[Failure[_]]
      result.failed.get.getMessage shouldBe "No names for person in generated JSON-LD"
    }
  }

  private trait TestCase {
    implicit val renkuBaseUrl:  RenkuBaseUrl  = renkuBaseUrls.generateOne
    implicit val fusekiBaseUrl: FusekiBaseUrl = fusekiBaseUrls.generateOne

    val personExtractor = new PersonExtractorImpl[Try]()
  }

  private def removePersonsNames(json: Json): Json = Plated.transform[Json] { json =>
    root.`@type`.each.string.getAll(json) match {
      case types if types.contains("http://schema.org/Person") =>
        root.obj.modify(_.remove("http://schema.org/name"))(json)
      case _ => json
    }
  }(json)

  private implicit class TriplesOps(triples: JsonLDTriples) {

    lazy val collectAllPersons: Set[Person] = {
      val collected = mutable.HashSet.empty[Person]
      Plated.transform[Json] { json =>
        root.`@type`.each.string.getAll(json) match {
          case types if types.contains("http://schema.org/Person") =>
            collected add Person(
              root.`@id`.as[ResourceId].getOption(json).getOrElse(fail("Person '@id' not found")),
              json.getValue[Try, Name](schema / "name").value.fold(throw _, identity),
              json.getValue[Try, Email](schema / "email").value.fold(throw _, identity),
              json.getValue[Try, Affiliation](schema / "affiliation").value.fold(throw _, identity)
            )
          case _ => ()
        }
        json
      }(triples.value)
      collected.toSet
    }
  }

  private case class Person(id:               ResourceId,
                            maybeName:        Option[Name],
                            maybeEmail:       Option[Email],
                            maybeAffiliation: Option[Affiliation]
  )

  private lazy val atLeastOneWithoutEmail: NonEmptyList[entities.Person] => Boolean = _.exists(_.maybeEmail.isEmpty)

}
