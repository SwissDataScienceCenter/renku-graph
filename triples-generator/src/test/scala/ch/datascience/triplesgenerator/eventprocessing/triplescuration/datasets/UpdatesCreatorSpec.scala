package ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets

import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.validatedUrls
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.SameAs
import ch.datascience.rdfstore.entities.DataSet
import ch.datascience.rdfstore.entities.bundles.{nonModifiedDataSetCommit, renkuBaseUrl}
import ch.datascience.rdfstore.{InMemoryRdfStore, JsonLDTriples}
import io.renku.jsonld.EntityId
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class UpdatesCreatorSpec extends WordSpec with InMemoryRdfStore {

  "prepareUpdates" should {

    "generate a query inserting renku:topmostSameAs if not present yet" in new TestCase {

      val triples = JsonLDTriples {
        nonModifiedDataSetCommit()()(datasetIdentifier = identifier, maybeDatasetSameAs = None).toJson
      }

      loadToStore(triples)

      findTopmostSameAs(entityId) shouldBe None

      (updatesCreator.prepareUpdates(entityId, SameAs(entityId)) map (_.query) map runUpdate).sequence.unsafeRunSync()

      findTopmostSameAs(entityId) shouldBe entityId.toString.some
    }

    "generate a query updating renku:topmostSameAs if already present" in new TestCase {

      val triples = JsonLDTriples {
        nonModifiedDataSetCommit()()(datasetIdentifier = identifier, maybeDatasetSameAs = None).toJson
      }

      loadToStore(triples)

      (updatesCreator.prepareUpdates(entityId, SameAs(entityId)) map (_.query) map runUpdate).sequence.unsafeRunSync()

      findTopmostSameAs(entityId) shouldBe entityId.toString.some

      val Right(newTopmostSameAs) = SameAs.fromId(validatedUrls.generateOne.value)
      (updatesCreator.prepareUpdates(entityId, newTopmostSameAs) map (_.query) map runUpdate).sequence.unsafeRunSync()

      findTopmostSameAs(entityId) shouldBe newTopmostSameAs.toString.some
    }
  }

  private trait TestCase {
    val identifier = datasetIdentifiers.generateOne
    val entityId   = DataSet.entityId(identifier)(renkuBaseUrl)

    val updatesCreator = new UpdatesCreator()
  }

  private def findTopmostSameAs(entityId: EntityId): Option[String] =
    runQuery(s"""|SELECT ?topmostSameAs
                 |WHERE {
                 |  <$entityId> rdf:type schema:Dataset.
                 |  OPTIONAL { <$entityId> renku:topmostSameAs ?topmostSameAs }.
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(_.get("topmostSameAs")) match {
      case Nil                => None
      case maybeSameAs +: Nil => maybeSameAs
      case _                  => fail(s"More than one topmostSameAs for dataset with $entityId id")
    }
}
