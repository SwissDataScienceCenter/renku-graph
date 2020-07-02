package ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets

import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.SameAs
import ch.datascience.rdfstore.entities.DataSet.DataSetEntity
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.rdfstore.{InMemoryRdfStore, JsonLDTriples}
import io.renku.jsonld.syntax._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.Try

class KGDatasetInfoFinderSpec extends WordSpec with InMemoryRdfStore {

  "findTopmostSameAs" should {

    "return None the sameAs entity is not found" in new TestCase {
      val sameAs = datasetIdSameAs.generateOne
      kgDatasetInfoFinder.findTopmostSameAs(sameAs) shouldBe Option.empty[SameAs].pure[Try]
    }

    "return the parent's topmostSameAs if there is one" in new TestCase {
      val activity = randomDataSetActivity
      loadToStore(JsonLDTriples(activity.asJsonLD.toJson))

      val parentDataset        = activity.entity[DataSetEntity]
      val datasetTopmostSameAs = SameAs(parentDataset.entityId)

      kgDatasetInfoFinder.findTopmostSameAs(datasetTopmostSameAs) shouldBe Some(parentDataset.topmostSameAs).pure[Try]
    }
  }

  private trait TestCase {
    val kgDatasetInfoFinder = new KGDatasetInfoFinderImpl[Try]
  }
}
