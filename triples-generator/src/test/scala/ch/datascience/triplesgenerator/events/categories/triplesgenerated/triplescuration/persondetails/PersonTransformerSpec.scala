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

import cats.MonadThrow
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.entities
import ch.datascience.graph.model.testentities._
import ch.datascience.http.client.RestClientError
import ch.datascience.http.client.RestClientError.UnauthorizedException
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.ProjectFunctions
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.ProjectFunctions._
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TransformationStep.ResultData
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.TriplesCurator.TransformationRecoverableError
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success, Try}

class PersonTransformerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "createTransformationStep" should {

    "go through all the Person entities found in Metadata, " +
      "try to find matching Person in KG, " +
      "merge the data and update the model " +
      "and generate relevant delete queries" in new TestCase {
        val persons = personEntities.generateSet()
        val project = anyProjectEntities
          .modify(membersLens.modify(_ => persons) andThen creatorLens.modify(_ => None))
          .generateOne
          .to[entities.Project]

        val expectedResultData =
          persons.map(_.to[entities.Person]).foldLeft(ResultData(project, List.empty)) { (resultData, person) =>
            val maybeKGPerson = personEntities.generateOption.map(_.to[entities.Person])
            (kgPersonFinder.find _).expects(person).returning(maybeKGPerson.pure[Try])
            maybeKGPerson match {
              case Some(kgPerson) =>
                val mergedPerson = personEntities.generateOne.to[entities.Person]
                (personMerger
                  .merge(_: entities.Person, _: entities.Person)(_: MonadThrow[Try]))
                  .expects(person, kgPerson, *)
                  .returning(mergedPerson.pure[Try])
                val queries = sparqlQueries.generateList()
                (updatesCreator.prepareUpdates _).expects(kgPerson).returning(queries)
                ResultData(update(person, mergedPerson)(resultData.project), resultData.queries ::: queries)
              case None => resultData
            }
          }

        val step = transformer.createTransformationStep

        step.name.value shouldBe "Person Details Updates"
        val Success(Right(updateResult)) = step.run(project).value
        updateResult shouldBe expectedResultData
      }

    "fail with RecoverableFailure if finding matching Person in KG fails with a network or HTTP error" in new TestCase {
      val person = personEntities.generateOne
      val project = anyProjectEntities
        .modify(membersLens.modify(_ => Set(person)) andThen creatorLens.modify(_ => None))
        .generateOne
        .to[entities.Project]

      val exception = recoverableClientErrors.generateOne
      (kgPersonFinder.find _)
        .expects(person.to[entities.Person])
        .returning(exception.raiseError[Try, Option[entities.Person]])

      val step = transformer.createTransformationStep

      val Success(Left(recoverableError)) = step.run(project).value

      recoverableError            shouldBe a[TransformationRecoverableError]
      recoverableError.getMessage shouldBe "Problem finding person details in KG"
    }

    "fail with NonRecoverableFailure if finding matching Person in KG fails with an unknown exception" in new TestCase {
      val person = personEntities.generateOne
      val project = anyProjectEntities
        .modify(membersLens.modify(_ => Set(person)) andThen creatorLens.modify(_ => None))
        .generateOne
        .to[entities.Project]

      val exception = exceptions.generateOne
      (kgPersonFinder.find _)
        .expects(person.to[entities.Person])
        .returning(exception.raiseError[Try, Option[entities.Person]])

      val step = transformer.createTransformationStep

      val Failure(nonRecoverableError) = step.run(project).value
      nonRecoverableError shouldBe exception
    }
  }

  private trait TestCase {

    val updatesCreator = mock[UpdatesCreator]
    val kgPersonFinder = mock[KGPersonFinder[Try]]
    val personMerger   = mock[PersonMerger]
    val transformer    = new PersonTransformerImpl[Try](kgPersonFinder, personMerger, updatesCreator, ProjectFunctions)
  }

  private lazy val recoverableClientErrors: Gen[RestClientError] =
    Gen.oneOf(clientExceptions, connectivityExceptions, unexpectedResponseExceptions, Gen.const(UnauthorizedException))
}
