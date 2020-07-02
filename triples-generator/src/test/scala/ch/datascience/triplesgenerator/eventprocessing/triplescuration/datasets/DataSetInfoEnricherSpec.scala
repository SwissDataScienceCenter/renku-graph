package ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets

import cats.data.EitherT
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.datasets.{DerivedFrom, SameAs}
import ch.datascience.triplesgenerator.eventprocessing.CommitEventProcessor.ProcessingRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CurationGenerators._
import io.renku.jsonld.generators.JsonLDGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.{Success, Try}

class DataSetInfoEnricherSpec extends WordSpec with MockFactory {

  "enrichDataSetInfo" should {

    "do nothing if there's no DataSet entity in the given JsonLD" in new TestCase {

      (infoFinder.findEntityId _).expects(curatedTriples.triples).returning(Set.empty)

      enricher.enrichDataSetInfo(curatedTriples) shouldBe EitherT.rightT[Try, ProcessingRecoverableError](
        curatedTriples
      )
    }

    "add updates generated for dataset info extracted from the triples" in new TestCase {

      val entityId = entityIds.generateOne
      (infoFinder.findEntityId _).expects(curatedTriples.triples).returning(Set(entityId))

      val updates = curationUpdates.generateNonEmptyList().toList
      (updatesCreator.prepareUpdates _)
        .expects(entityId, SameAs(entityId), DerivedFrom(entityId))
        .returning(updates)

      val Success(Right(results)) = enricher.enrichDataSetInfo(curatedTriples).value

      results.triples shouldBe curatedTriples.triples
      results.updates shouldBe curatedTriples.updates ++: updates
    }
  }

  private trait TestCase {
    val curatedTriples = curatedTriplesObjects.generateOne

    val infoFinder     = mock[DataSetInfoFinder]
    val updatesCreator = mock[UpdatesCreator]
    val enricher       = new DataSetInfoEnricher[Try](infoFinder, updatesCreator)
  }
}
