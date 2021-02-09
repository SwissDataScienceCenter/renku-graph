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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import cats.MonadError
import cats.data.EitherT
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits.GenOps
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.Path
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import ch.datascience.graph.tokenrepository.IOAccessTokenFinder.projectPathToPath
import ch.datascience.http.client.AccessToken
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.CuratedTriples
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.CuratedTriples.CurationUpdatesGroup
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.CurationGenerators._
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.IOTriplesCurator.CurationRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails.PersonDetailsGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class PersonDetailsUpdaterSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "updatePersonDetails" should {

    "extract persons, match with project members and prepare updates for extracted persons" in new TestCase {

      val triplesWithoutPersonDetails = jsonLDTriples.generateOne
      val extractedPersons = persons.generateSet()
      (personExtractor.extractPersons _)
        .expects(curatedTriples.triples)
        .returning((triplesWithoutPersonDetails, extractedPersons).pure[Try])

      val maybeAccessToken = accessTokens.generateOption
      (accessTokenFinder
        .findAccessToken(_: Path)(_: Path => String))
        .expects(projectPath, projectPathToPath)
        .returning(maybeAccessToken.pure[Try])

      val projectMembers = gitLabProjectMembers.generateNonEmptyList().toList.toSet
      (projectMembersFinder
        .findProjectMembers(_: projects.Path)(_: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](projectMembers))

      val personsWithGitlabIds = persons.generateSet()
      (personsAndProjectMembersMatcher.merge _)
        .expects(extractedPersons, projectMembers)
        .returning(personsWithGitlabIds)

      val newUpdatesGroups = personsWithGitlabIds.foldLeft(List.empty[CurationUpdatesGroup[Try]]) { (acc, person) =>
        val updatesGroup = curationUpdatesGroups[Try].generateOne
        (updatesCreator
          .prepareUpdates[Try](_: Person)(_: MonadError[Try, Throwable]))
          .expects(person, *)
          .returning(updatesGroup)

        acc :+ updatesGroup
      }

      val Success(Right(CuratedTriples(actualTriples, actualUpdates))) =
        updater.updatePersonDetails(curatedTriples, projectPath).value

      actualTriples shouldBe triplesWithoutPersonDetails
      actualUpdates.take(curatedTriples.updatesGroups.length) shouldBe curatedTriples.updatesGroups
      actualUpdates.drop(curatedTriples.updatesGroups.length) should contain theSameElementsAs newUpdatesGroups
    }

    "fail if extracting persons fails" in new TestCase {

      val exception = exceptions.generateOne
      (personExtractor.extractPersons _)
        .expects(curatedTriples.triples)
        .returning(exception.raiseError[Try, (JsonLDTriples, Set[Person])])

      updater.updatePersonDetails(curatedTriples, projectPath).value shouldBe exception
        .raiseError[Try, (JsonLDTriples, Set[Person])]
    }

    "fail if finding project access token fails" in new TestCase {

      val triplesWithoutPersonDetails = jsonLDTriples.generateOne
      val extractedPersons = persons.generateSet()
      (personExtractor.extractPersons _)
        .expects(curatedTriples.triples)
        .returning((triplesWithoutPersonDetails, extractedPersons).pure[Try])

      val exception = exceptions.generateOne
      (accessTokenFinder
        .findAccessToken(_: Path)(_: Path => String))
        .expects(projectPath, projectPathToPath)
        .returning(exception.raiseError[Try, Option[AccessToken]])

      updater.updatePersonDetails(curatedTriples, projectPath).value shouldBe exception
        .raiseError[Try, (JsonLDTriples, Set[Person])]
    }

    "fail if finding project members fails" in new TestCase {

      val triplesWithoutPersonDetails = jsonLDTriples.generateOne
      val extractedPersons = persons.generateSet()
      (personExtractor.extractPersons _)
        .expects(curatedTriples.triples)
        .returning((triplesWithoutPersonDetails, extractedPersons).pure[Try])

      val maybeAccessToken = accessTokens.generateOption
      (accessTokenFinder
        .findAccessToken(_: Path)(_: Path => String))
        .expects(projectPath, projectPathToPath)
        .returning(maybeAccessToken.pure[Try])

      val exception = exceptions.generateOne
      (projectMembersFinder
        .findProjectMembers(_: projects.Path)(_: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(
          EitherT(exception.raiseError[Try, Either[ProcessingRecoverableError, Set[GitLabProjectMember]]])
        )

      updater.updatePersonDetails(curatedTriples, projectPath).value shouldBe exception
        .raiseError[Try, (JsonLDTriples, Set[Person])]
    }

    "return ProcessingRecoverableError if finding project members returns one" in new TestCase {

      val triplesWithoutPersonDetails = jsonLDTriples.generateOne
      val extractedPersons = persons.generateSet()
      (personExtractor.extractPersons _)
        .expects(curatedTriples.triples)
        .returning((triplesWithoutPersonDetails, extractedPersons).pure[Try])

      val maybeAccessToken = accessTokens.generateOption
      (accessTokenFinder
        .findAccessToken(_: Path)(_: Path => String))
        .expects(projectPath, projectPathToPath)
        .returning(maybeAccessToken.pure[Try])

      val exception = CurationRecoverableError(nonBlankStrings().generateOne.value, exceptions.generateOne)
      (projectMembersFinder
        .findProjectMembers(_: projects.Path)(_: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(
          EitherT.leftT[Try, Set[GitLabProjectMember]](exception)
        )

      updater.updatePersonDetails(curatedTriples, projectPath).value shouldBe Left(exception).pure[Try]
    }
  }

  private trait TestCase {
    val projectPath = projectPaths.generateOne
    val curatedTriples = curatedTriplesObjects[Try].generateOne

    val personExtractor = mock[PersonExtractor[Try]]
    val accessTokenFinder = mock[AccessTokenFinder[Try]]
    val updatesCreator = mock[UpdatesCreator]
    val projectMembersFinder = mock[GitLabProjectMembersFinder[Try]]
    val personsAndProjectMembersMatcher = mock[PersonsAndProjectMembersMatcher]

    val updater = new PersonDetailsUpdaterImpl[Try](
      personExtractor,
      accessTokenFinder,
      projectMembersFinder,
      personsAndProjectMembersMatcher,
      updatesCreator
    )
  }

}
