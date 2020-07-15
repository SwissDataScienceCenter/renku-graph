package ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets
import io.renku.jsonld.generators.JsonLDGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{DerivedFrom, SameAs}
import org.scalatest.WordSpec
import org.scalatest.Matchers._
import cats.implicits._
import ch.datascience.triplesgenerator.eventprocessing.CommitEventProcessor.ProcessingRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets.TopmostDataFinder.TopmostData
import org.scalamock.scalatest.MockFactory

import scala.util.{Failure, Try}

class TopmostDataFinderSpec extends WordSpec with MockFactory {

  "findTopmostData" should {

    "return a TopmostDataInfo with sameAs and derivedFrom pointing to the dataset id " +
      "if there is no sameAs and derivedFrom in the DatasetInfo" in new TestCase {
      topmostDataFinder.findTopmostData(entityId, None, None) shouldBe TopmostData(
        entityId,
        SameAs(entityId),
        DerivedFrom(entityId)
      ).pure[Try]
    }

    "return a TopmostDataInfo with sameAs from DatasetInfo if sameAs is pointing to a non renku url" in new TestCase {
      val sameAs = datasetUrlSameAs.generateOne

      topmostDataFinder.findTopmostData(entityId, Some(sameAs), None) shouldBe TopmostData(
        entityId,
        sameAs,
        DerivedFrom(entityId)
      ).pure[Try]
    }

    "return a TopmostDataInfo with parent's topmostSameAs " +
      "if sameAs is pointing to a renku dataset and there's a parent dataset" in new TestCase {
      val sameAs = datasetIdSameAs.generateOne

      val parentSameAs = datasetSameAs.generateOne

      (kgDatasetInfoFinder.findTopmostSameAs _).expects(sameAs).returning(Some(parentSameAs).pure[Try])

      topmostDataFinder.findTopmostData(entityId, Some(sameAs), None) shouldBe TopmostData(
        entityId,
        parentSameAs,
        DerivedFrom(entityId)
      ).pure[Try]
    }

    "return a TopmostDataInfo with the given sameAs " +
      "if the parent dataset cannot be found" in new TestCase {
      val sameAs = datasetIdSameAs.generateOne

      (kgDatasetInfoFinder.findTopmostSameAs _).expects(sameAs).returning(None.pure[Try])

      topmostDataFinder.findTopmostData(entityId, Some(sameAs), None) shouldBe TopmostData(
        entityId,
        sameAs,
        DerivedFrom(entityId)
      ).pure[Try]
    }

    "return a TopmostDataInfo with derivedFrom from the parent " +
      "if there's a derivedFrom on the parent" in new TestCase {
      val derivedFrom       = datasetDerivedFroms.generateOne
      val parentDerivedFrom = datasetDerivedFroms.generateOne

      (kgDatasetInfoFinder.findTopmostDerivedFrom _).expects(derivedFrom).returning(Some(parentDerivedFrom).pure[Try])

      topmostDataFinder.findTopmostData(entityId, None, Some(derivedFrom)) shouldBe TopmostData(
        entityId,
        SameAs(entityId),
        parentDerivedFrom
      ).pure[Try]
    }

    "return a TopmostDataInfo with the given derivedFrom " +
      "if there's no derivedFrom on the parent" in new TestCase {
      val derivedFrom = datasetDerivedFroms.generateOne

      (kgDatasetInfoFinder.findTopmostDerivedFrom _).expects(derivedFrom).returning(None.pure[Try])

      topmostDataFinder.findTopmostData(entityId, None, Some(derivedFrom)) shouldBe TopmostData(
        entityId,
        SameAs(entityId),
        derivedFrom
      ).pure[Try]
    }

    "fail if there's both sameAs and derivedFrom given" in new TestCase {
      val Failure(exception) = topmostDataFinder.findTopmostData(
        entityId,
        datasetSameAs.generateSome,
        datasetDerivedFroms.generateSome
      )

      exception            should not be a[ProcessingRecoverableError]
      exception.getMessage shouldBe s"Dataset with $entityId found in the generated triples has both sameAs and derivedFrom"
    }
  }

  private trait TestCase {
    val entityId = entityIds.generateOne

    val kgDatasetInfoFinder = mock[KGDatasetInfoFinder[Try]]

    val topmostDataFinder = new TopmostDataFinderImpl[Try](kgDatasetInfoFinder)
  }
}
