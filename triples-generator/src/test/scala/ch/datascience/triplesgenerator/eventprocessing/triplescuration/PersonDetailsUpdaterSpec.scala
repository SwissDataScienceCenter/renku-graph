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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration

import cats.data.NonEmptyList
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.users.{Affiliation, Email, Id, Name}
import ch.datascience.rdfstore.triples._
import ch.datascience.rdfstore.{FusekiBaseUrl, JsonLDTriples}
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import ch.datascience.tinytypes.json.TinyTypeEncoders._
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.PersonDetailsUpdater.{Person => UpdatePerson, _}
import eu.timepit.refined.auto._
import io.circe.optics.JsonOptics._
import io.circe.optics.JsonPath.root
import io.circe.{Decoder, Encoder, Json}
import monocle.function.Plated
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.collection.mutable
import scala.util.{Success, Try}

class PersonDetailsUpdaterSpec extends WordSpec {

  "curate" should {

    "remove properties from all the Person entities found in the given Json except those which id starts with '_'" +
      "and create SPARQL updates for them" in new TestCase {
      val projectCreatorName  = names.generateOne
      val projectCreatorEmail = emails.generateOne
      val committerName       = names.generateOne
      val committerEmail      = emails.generateOne
      val datasetCreatorsSet = nonEmptyList(datasetCreators, minElements = 5, maxElements = 10)
        .retryUntil(atLeastOneWithoutEmail)
        .generateOne
        .toList
        .toSet
      val jsonTriples = JsonLDTriples {
        singleFileAndCommitWithDataset(
          projectPath          = projectPaths.generateOne,
          projectCreator       = projectCreatorName -> projectCreatorEmail,
          committerName        = committerName,
          committerEmail       = committerEmail,
          maybeDatasetCreators = datasetCreatorsSet
        )
      }

      val allPersons = jsonTriples.collectAllPersons
      allPersons.filter(blankIds)    should not be empty
      allPersons.filterNot(blankIds) should not be empty

      val Success(curatedTriples) = curator curate CuratedTriples(jsonTriples, updates = Nil)

      val curatedPersons = curatedTriples.triples.collectAllPersons
      curatedPersons.filter(blankIds)    shouldBe allPersons.filter(blankIds)
      curatedPersons.filterNot(blankIds) shouldBe allPersons.filterNot(blankIds).map(noEmailAndName)

      curatedTriples.updates should contain theSameElementsAs prepareUpdates(
        datasetCreatorsSet.map(maybeUpdatePerson).flatten + updatePerson(
          projectCreatorName,
          projectCreatorEmail
        ) + updatePerson(
          committerName,
          committerEmail
        )
      )
    }
  }

  private trait TestCase {
    implicit val fusekiBaseUrl: FusekiBaseUrl = fusekiBaseUrls.generateOne

    val curator = new PersonDetailsUpdater[Try]()
  }

  private implicit class TriplesOps(triples: JsonLDTriples) {

    lazy val collectAllPersons: Set[Person] = {
      val collected = mutable.HashSet.empty[Person]
      Plated.transform[Json] { json =>
        root.`@type`.each.string.getAll(json) match {
          case types if types.contains("http://schema.org/Person") =>
            collected add Person(
              root.`@id`.as[Id].getOption(json).getOrElse(fail("Person '@id' not found")),
              extractValue[Name]("http://schema.org/name")(json).headOption,
              extractValue[Email]("http://schema.org/email")(json).headOption,
              extractValue[Affiliation]("http://schema.org/affiliation")(json).headOption
            )
          case _ => ()
        }
        json
      }(triples.value)
      collected.toSet
    }

    private def extractValue[T](property: String)(json: Json)(implicit decoder: Decoder[T], encoder: Encoder[T]) =
      root.selectDynamic(property).each.`@value`.as[T].getAll(json)
  }

  private case class Person(id:               Id,
                            maybeName:        Option[Name],
                            maybeEmail:       Option[Email],
                            maybeAffiliation: Option[Affiliation])

  private lazy val maybeUpdatePerson: ((Name, Option[Email], Option[Affiliation])) => Option[UpdatePerson] = {
    case (name, maybeEmail, _) => maybeEmail map (updatePerson(name, _))
  }
  private def updatePerson(name: Name, email: Email) = UpdatePerson(
    Id(entities.Person.Id(Some(email)).value),
    Set(name),
    Set(email)
  )

  private lazy val atLeastOneWithoutEmail: NonEmptyList[(Name, Option[Email], Option[Affiliation])] => Boolean =
    _.exists(_._2.isEmpty)

  private lazy val blankIds:       Person => Boolean = _.id.value startsWith "_"
  private lazy val noEmailAndName: Person => Person  = _.copy(maybeName = None, maybeEmail = None)
}
