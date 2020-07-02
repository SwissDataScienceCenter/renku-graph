package ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets

import ch.datascience.generators.CommonGraphGenerators.{fusekiBaseUrls, jsonLDTriples}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators.datasetIdentifiers
import ch.datascience.rdfstore.entities.DataSet
import ch.datascience.rdfstore.entities.bundles.{nonModifiedDataSetCommit, renkuBaseUrl}
import ch.datascience.rdfstore.{FusekiBaseUrl, JsonLDTriples}
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class DataSetInfoFinderSpec extends WordSpec {

  private implicit val fusekiBaseUrl: FusekiBaseUrl = fusekiBaseUrls.generateOne

  "findEntityId" should {

    "return None if there's no Dataset entity in the Json" in new TestCase {
      infoFinder.findDatasetsInfo(jsonLDTriples.generateOne) shouldBe Set.empty
    }

    "return the DataSet's entityId present in the Json" in new TestCase {
      val identifier = datasetIdentifiers.generateOne
      val entityId   = DataSet.entityId(identifier)(renkuBaseUrl)
      val triples = JsonLDTriples {
        nonModifiedDataSetCommit()()(datasetIdentifier = identifier, maybeDatasetSameAs = None).toJson
      }

      infoFinder.findDatasetsInfo(triples) shouldBe Set(entityId)
    }
  }

  private trait TestCase {
    val infoFinder = new DataSetInfoFinder
  }
}
