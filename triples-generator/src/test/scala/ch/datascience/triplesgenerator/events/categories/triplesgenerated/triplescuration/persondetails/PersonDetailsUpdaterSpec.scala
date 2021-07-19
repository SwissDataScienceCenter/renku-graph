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

//import cats.MonadError
//import cats.data.EitherT
//import cats.syntax.all._
//import ch.datascience.events.consumers.ConsumersModelGenerators.projects
//import ch.datascience.generators.CommonGraphGenerators._
//import ch.datascience.generators.Generators.Implicits._
//import ch.datascience.generators.Generators._
//import ch.datascience.graph.model.EventsGenerators._
//import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TransformationData.TransformationStep
//import ch.datascience.triplesgenerator.events.categories.triplesgenerated.ProjectMetadata
//import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TriplesGeneratedGenerators.projectMetadatas
////import ch.datascience.graph.model.projects.Path
//import ch.datascience.graph.model.entities
//import ch.datascience.graph.model.testentities._
//import ch.datascience.http.client.AccessToken
//import ch.datascience.rdfstore.JsonLDTriples
////import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
////import ch.datascience.triplesgenerator.events.categories.triplesgenerated.CuratedTriples
////import ch.datascience.triplesgenerator.events.categories.triplesgenerated.CuratedTriples.CurationUpdatesGroup
//import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.CurationGenerators._
////import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.TriplesCurator.CurationRecoverableError
////import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails.PersonDetailsGenerators._
//import ch.datascience.http.client.AccessToken
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

//import scala.util.{Success, Try}

class PersonDetailsUpdaterSpec extends AnyWordSpec with should.Matchers with MockFactory {

//  "updatePersonDetails" should {
//
//    "go through all the Person entities found in Metadata, " +
//      "try to find matching Person in KG, " +
//      "merge the data and update the model " +
//      "and generate relevant delete queries" in new TestCase {
//        val projectMetadata = mock[ProjectMetadata]
//        val curatedTriples  = transformationDataObjects[Try].generateOne.copy(projectMetadata = projectMetadata)
//
//        val persons = projectMetadata.findAllPersons
//
//        val (finalProjectMetadata, allUpdates) =
//          persons.foldLeft(projectMetadata -> List.empty[TransformationStep[Try]]) {
//            case ((previousMetadata, allUpdates), person) =>
//              val kgPerson = personEntities.generateOne.to[entities.Person]
//              (kgPersonFinder.find _).expects(person).returning(kgPerson.some.pure[Try])
//              val mergedPerson = personEntities.generateOne.to[entities.Person]
//              (personMerger.merge _).expects(person, kgPerson).returning(mergedPerson)
//              val updatedProjectMetadata = mock[ProjectMetadata]
//              (previousMetadata.update _).expects(person, mergedPerson).returning(updatedProjectMetadata)
//              val updatesGroup = transformationSteps[Try].generateOne
//              (updatesCreator.prepareUpdates _).expects(person, kgPerson).returning(updatesGroup)
//              updatedProjectMetadata -> (allUpdates ::: updatesGroup :: Nil)
//          }
//
//        val Success(updatedCuratedTriples) =
//          updater.updatePersonDetails(curatedTriples.copy(projectMetadata = projectMetadata))
//
//        updatedCuratedTriples.projectMetadata shouldBe finalProjectMetadata
//        updatedCuratedTriples.updatesGroups     should contain allOf allUpdates
//      }
//
////    "extract persons, match with project members and prepare updates for extracted persons" in new TestCase {
////
////      val triplesWithoutPersonDetails = jsonLDTriples.generateOne
////
////      (personTrimmer.getTriplesAndTrimmedPersons _)
////        .expects(curatedTriples.triples, project.id, eventId, maybeAccessToken)
////        .returning(EitherT.right((triplesWithoutPersonDetails, trimmedPersons).pure[Try]))
////
////      val projectMembers = gitLabProjectMembers.generateNonEmptyList().toList.toSet
////      (projectMembersFinder
////        .findProjectMembers(_: Path)(_: Option[AccessToken]))
////        .expects(project.path, maybeAccessToken)
////        .returning(EitherT.rightT[Try, ProcessingRecoverableError](projectMembers))
////
////      val personsWithGitlabIds = personEntities(withGitLabId).map(_.to[persondetails.Person]).generateFixedSizeSet()
////      (personsAndProjectMembersMatcher.merge _)
////        .expects(trimmedPersons, projectMembers)
////        .returning(personsWithGitlabIds)
////
////      val newUpdatesGroups = personsWithGitlabIds.foldLeft(List.empty[CurationUpdatesGroup[Try]]) { (acc, person) =>
////        val updatesGroup = curationUpdatesGroups[Try].generateOne
////        (updatesCreator
////          .prepareUpdates[Try](_: persondetails.Person)(_: MonadError[Try, Throwable]))
////          .expects(person, *)
////          .returning(updatesGroup)
////
////        acc :+ updatesGroup
////      }
////
////      val Success(Right(CuratedTriples(actualTriples, _, actualUpdates))) =
////        updater.updatePersonDetails(curatedTriples, project, eventId).value
////
////      actualTriples                                           shouldBe triplesWithoutPersonDetails
////      actualUpdates.take(curatedTriples.updatesGroups.length) shouldBe curatedTriples.updatesGroups
////      actualUpdates.drop(curatedTriples.updatesGroups.length)   should contain theSameElementsAs newUpdatesGroups
////    }
////
////    "fail if extracting persons fails" in new TestCase {
////
////      val exception = exceptions.generateOne
////      (personTrimmer.getTriplesAndTrimmedPersons _)
////        .expects(curatedTriples.triples, project.id, eventId, maybeAccessToken)
////        .returning(EitherT.right(exception.raiseError[Try, (JsonLDTriples, Set[persondetails.Person])]))
////
////      updater.updatePersonDetails(curatedTriples, project, eventId).value shouldBe exception
////        .raiseError[Try, (JsonLDTriples, Set[persondetails.Person])]
////    }
////
////    "fail if finding project members fails" in new TestCase {
////
////      val triplesWithoutPersonDetails = jsonLDTriples.generateOne
////      (personTrimmer.getTriplesAndTrimmedPersons _)
////        .expects(curatedTriples.triples, project.id, eventId, maybeAccessToken)
////        .returning(EitherT.right((triplesWithoutPersonDetails, trimmedPersons).pure[Try]))
////
////      val exception = exceptions.generateOne
////      (projectMembersFinder
////        .findProjectMembers(_: Path)(_: Option[AccessToken]))
////        .expects(project.path, maybeAccessToken)
////        .returning(
////          EitherT(exception.raiseError[Try, Either[ProcessingRecoverableError, Set[GitLabProjectMember]]])
////        )
////
////      updater.updatePersonDetails(curatedTriples, project, eventId).value shouldBe exception
////        .raiseError[Try, (JsonLDTriples, Set[persondetails.Person])]
////    }
////
////    "return ProcessingRecoverableError if finding project members returns one" in new TestCase {
////
////      val triplesWithoutPersonDetails = jsonLDTriples.generateOne
////      (personTrimmer.getTriplesAndTrimmedPersons _)
////        .expects(curatedTriples.triples, project.id, eventId, maybeAccessToken)
////        .returning(EitherT.right((triplesWithoutPersonDetails, trimmedPersons).pure[Try]))
////
////      val exception = CurationRecoverableError(nonBlankStrings().generateOne.value, exceptions.generateOne)
////      (projectMembersFinder
////        .findProjectMembers(_: Path)(_: Option[AccessToken]))
////        .expects(project.path, maybeAccessToken)
////        .returning(
////          EitherT.leftT[Try, Set[GitLabProjectMember]](exception)
////        )
////
////      updater.updatePersonDetails(curatedTriples, project, eventId).value shouldBe Left(exception).pure[Try]
////    }
//  }
//
//  private trait TestCase {
//
//    val trimmedPersons = personEntities().generateFixedSizeSet()
//    val eventId        = eventIds.generateOne
//
//    val personTrimmer                   = mock[PersonTrimmer[Try]]
//    val updatesCreator                  = mock[UpdatesCreator]
//    val projectMembersFinder            = mock[GitLabProjectMembersFinder[Try]]
//    val personsAndProjectMembersMatcher = mock[PersonsAndProjectMembersMatcher]
//
//    val kgPersonFinder = mock[KGPersonFinder[Try]]
//    val personMerger   = mock[PersonMerger]
//    val updater = new PersonDetailsUpdaterImpl[Try](
//      personTrimmer,
//      projectMembersFinder
////      personsAndProjectMembersMatcher,
////      updatesCreator
//    )
//  }
}
