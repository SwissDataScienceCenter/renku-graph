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
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CurationGenerators._
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails.PersonDetailsUpdater.{Person => UpdatePerson}
import eu.timepit.refined.auto._
import io.circe.optics.JsonOptics._
import io.circe.optics.JsonPath.root
import io.circe.{Decoder, Encoder, Json}
import io.renku.jsonld.syntax._
import monocle.function.Plated
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class PersonDetailsUpdaterSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "curate" should {

    "remove name and email properties from all the Person entities found in the given Json " +
      "except those which id starts with '_' (blank nodes) " +
      "and create SPARQL updates for them" in new TestCase {
        val projectCreatorName  = userNames.generateOne
        val projectCreatorEmail = userEmails.generateOne
        val committerName       = userNames.generateOne
        val committerEmail      = userEmails.generateOne
        val datasetCreatorsSet = nonEmptyList(entities.Person.persons, minElements = 5, maxElements = 10)
          .retryUntil(atLeastOneWithoutEmail)
          .generateOne
          .toList
          .toSet
        val jsonTriples = JsonLDTriples {
          nonModifiedDataSetCommit(
            committer = entities.Person(committerName, committerEmail)
          )(
            projectPath = projectPaths.generateOne,
            maybeProjectCreator = entities.Person(projectCreatorName, projectCreatorEmail).some
          )(
            datasetCreators = datasetCreatorsSet
          ).toJson
        }

        val allPersons = jsonTriples.collectAllPersons
        allPersons.filter(blankIds)    should not be empty
        allPersons.filterNot(blankIds) should not be empty

        val allPersonsInPayload = datasetCreatorsSet +
          entities.Person(projectCreatorName, projectCreatorEmail) +
          entities.Person(committerName, committerEmail)

        val expectedUpdatesGroups = allPersonsInPayload
          .map(maybeUpdatedPerson)
          .flatten
          .map { person =>
            val updatesGroup = curationUpdatesGroups[Try].generateOne
            (updatesCreator
              .prepareUpdates[Try](_: UpdatePerson)(_: MonadError[Try, Throwable]))
              .expects(person, *)
              .returning(updatesGroup)
            updatesGroup
          }

        val Success(curatedTriples) = curator curate CuratedTriples(jsonTriples, updatesGroups = Nil)

        val curatedPersons = curatedTriples.triples.collectAllPersons
        curatedPersons.filter(blankIds)    shouldBe allPersons.filter(blankIds)
        curatedPersons.filterNot(blankIds) shouldBe allPersons.filterNot(blankIds).map(noEmailAndName)

        curatedTriples.updatesGroups shouldBe expectedUpdatesGroups.toList
      }

    "fail if there's a Person entity without a name" in new TestCase {

      val jsonTriples = JsonLDTriples(randomDataSetCommit.toJson)

      val noNamesJson = JsonLDTriples(jsonTriples.removePersonsNames)

      val result = curator curate CuratedTriples(noNamesJson, updatesGroups = Nil)

      result                     shouldBe a[Failure[_]]
      result.failed.get.getMessage should include regex "No names for person with '(.*)' id found in generated JSON-LD".r
    }
  }

  private trait TestCase {
    implicit val renkuBaseUrl:  RenkuBaseUrl  = renkuBaseUrls.generateOne
    implicit val fusekiBaseUrl: FusekiBaseUrl = fusekiBaseUrls.generateOne

    val updatesCreator = mock[UpdatesCreator]
    val curator        = new PersonDetailsUpdater[Try](updatesCreator)
  }

  private implicit class TriplesOps(triples: JsonLDTriples) {

    lazy val removePersonsNames: Json = Plated.transform[Json] { json =>
      root.`@type`.each.string.getAll(json) match {
        case types if types.contains("http://schema.org/Person") =>
          root.obj.modify(_.remove("http://schema.org/name"))(json)
        case _ => json
      }
    }(triples.value)

    lazy val collectAllPersons: Set[Person] = {
      val collected = mutable.HashSet.empty[Person]
      Plated.transform[Json] { json =>
        root.`@type`.each.string.getAll(json) match {
          case types if types.contains("http://schema.org/Person") =>
            collected add Person(
              root.`@id`.as[ResourceId].getOption(json).getOrElse(fail("Person '@id' not found")),
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

  private case class Person(id:               ResourceId,
                            maybeName:        Option[Name],
                            maybeEmail:       Option[Email],
                            maybeAffiliation: Option[Affiliation]
  )

  private lazy val maybeUpdatedPerson: entities.Person => Option[UpdatePerson] = { person =>
    person.maybeEmail map { email =>
      val entityId = person.asJsonLD.entityId getOrElse (throw new Exception(s"Cannot find entity id for $person"))
      UpdatePerson(ResourceId(entityId), NonEmptyList.of(person.name), Set(email))
    }
  }

  private lazy val atLeastOneWithoutEmail: NonEmptyList[entities.Person] => Boolean = _.exists(_.maybeEmail.isEmpty)

  private lazy val blankIds:       Person => Boolean = p => !(p.id.value startsWith "mailto:")
  private lazy val noEmailAndName: Person => Person  = _.copy(maybeName = None, maybeEmail = None)
}
