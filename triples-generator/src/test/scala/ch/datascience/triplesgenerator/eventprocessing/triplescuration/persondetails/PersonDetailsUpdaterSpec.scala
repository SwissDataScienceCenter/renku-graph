package ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails

import cats.MonadError
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators._
import ch.datascience.generators.Generators.Implicits.GenOps
import ch.datascience.graph.model.GraphModelGenerators.{userEmails, userNames, userResourceIds}
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples.CurationUpdatesGroup
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CurationGenerators._
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails.PersonDetailsUpdater.Person
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class PersonDetailsUpdaterSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "curate" should {

    "extract persons and prepare updates for extracted persons" in new TestCase {
      val curatedTriples              = curatedTriplesObjects[Try].generateOne
      val triplesWithoutPersonDetails = jsonLDTriples.generateOne
      val extractedPersons            = persons.generateSet()

      (personExtractor.extractPersons _)
        .expects(curatedTriples.triples)
        .returning((triplesWithoutPersonDetails, extractedPersons).pure[Try])

      val newUpdatesGroups = extractedPersons.foldLeft(List.empty[CurationUpdatesGroup[Try]]) { (acc, person) =>
        val updatesGroup = curationUpdatesGroups[Try].generateOne
        (updatesCreator
          .prepareUpdates[Try](_: Person)(_: MonadError[Try, Throwable]))
          .expects(person, *)
          .returning(updatesGroup)

        acc :+ updatesGroup
      }

      val Success(CuratedTriples(actualTriples, actualUpdates)) = updater.curate(curatedTriples)

      actualTriples                                           shouldBe triplesWithoutPersonDetails
      actualUpdates.take(curatedTriples.updatesGroups.length) shouldBe curatedTriples.updatesGroups
      actualUpdates.drop(curatedTriples.updatesGroups.length)   should contain theSameElementsAs newUpdatesGroups
    }

    "fail if extractPersons fails" in new TestCase {

      val curatedTriples = curatedTriplesObjects[Try].generateOne
      val exception      = exceptions.generateOne
      (personExtractor.extractPersons _)
        .expects(curatedTriples.triples)
        .returning(exception.raiseError[Try, (JsonLDTriples, Set[Person])])

      updater.curate(curatedTriples) shouldBe exception.raiseError[Try, (JsonLDTriples, Set[Person])]
    }
  }

  private trait TestCase {
    val personExtractor = mock[PersonExtractor[Try]]
    val updatesCreator  = mock[UpdatesCreator]

    val updater = new PersonDetailsUpdaterImpl[Try](personExtractor, updatesCreator)

  }

  private val persons: Gen[Person] = for {
    id     <- userResourceIds
    names  <- userNames.toGeneratorOfNonEmptyList()
    emails <- userEmails.toGeneratorOfSet()
  } yield Person(id, names, emails)
}
