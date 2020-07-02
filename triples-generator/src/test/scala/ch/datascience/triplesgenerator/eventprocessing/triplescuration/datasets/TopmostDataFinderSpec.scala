package ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets
import io.renku.jsonld.generators.JsonLDGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{DerivedFrom, SameAs}
import org.scalatest.WordSpec
import org.scalatest.Matchers._
import cats.implicits._
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets.TopmostDataFinder.TopmostData
import org.scalamock.scalatest.MockFactory

import scala.util.Try

class TopmostDataFinderSpec extends WordSpec with MockFactory {

  "findTopmostData" should {
    "return a TopmostDataInfo with sameAs and derivedFrom pointing to the dataset id " +
      "if there is no sameAs and derivedFrom in the DatasetInfo" in new TestCase {
      val entityId = entityIds.generateOne

      topmostDataFinder.findTopmostData(entityId, None, None) shouldBe TopmostData(
        entityId,
        SameAs(entityId),
        DerivedFrom(entityId)
      ).pure[Try]
    }

    "return a TopmostDataInfo with sameAs from DatasetInfo if sameAs is pointing to a non renku url" in new TestCase {
      val entityId = entityIds.generateOne
      val sameAs   = datasetUrlSameAs.generateOne

      topmostDataFinder.findTopmostData(entityId, Some(sameAs), None) shouldBe TopmostData(
        entityId,
        sameAs,
        DerivedFrom(entityId)
      ).pure[Try]
    }

    "return a TopmostDataInfo with parent's topmostSameAs if sameAs is pointing to a renku dataset" in new TestCase {
      val entityId = entityIds.generateOne
      val idSameAs = datasetIdSameAs.generateOne

      val parentSameAs = datasetSameAs.generateOne

      (kgDatasetInfoFinder.findTopmostSameAs _).expects(idSameAs).returning(Some(parentSameAs).pure[Try])

      topmostDataFinder.findTopmostData(entityId, Some(idSameAs), None) shouldBe TopmostData(
        entityId,
        parentSameAs,
        DerivedFrom(entityId)
      ).pure[Try]
    }

    "return a TopmostDataInfo with idSame from json if parent's topmostSameAs cannot be found" in new TestCase {
      val entityId = entityIds.generateOne
      val idSameAs = datasetIdSameAs.generateOne

      (kgDatasetInfoFinder.findTopmostSameAs _).expects(idSameAs).returning(None.pure[Try])

      topmostDataFinder.findTopmostData(entityId, Some(idSameAs), None) shouldBe TopmostData(
        entityId,
        idSameAs,
        DerivedFrom(entityId)
      ).pure[Try]
    }
  }

  private trait TestCase {

    val kgDatasetInfoFinder = mock[KGDatasetInfoFinder[Try]]

    val topmostDataFinder = new TopmostDataFinderImpl[Try](kgDatasetInfoFinder)
  }
}
