package ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets

import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{DerivedFrom, SameAs}
import ch.datascience.rdfstore.entities.DataSet
import ch.datascience.rdfstore.entities.bundles.{nonModifiedDataSetCommit, renkuBaseUrl}
import ch.datascience.rdfstore.{InMemoryRdfStore, JsonLDTriples}
import io.renku.jsonld.EntityId
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class UpdatesCreatorSpec extends WordSpec with InMemoryRdfStore {

  "prepareUpdates" should {

    "generate a query inserting renku:topmostSameAs if not present yet" in new TestCase {

      `given Dataset present in the triple store`

      findTopmostSameAsAndDerivedFrom(entityId) shouldBe None -> None

      (updatesCreator.prepareUpdates(entityId, sameAs, derivedFrom) map (_.query) map runUpdate).sequence
        .unsafeRunSync()

      findTopmostSameAsAndDerivedFrom(entityId) shouldBe sameAs.some -> derivedFrom.some
    }

    "generate a query updating renku:topmostSameAs if already present" in new TestCase {

      `given Dataset present in the triple store`

      (updatesCreator.prepareUpdates(entityId, sameAs, derivedFrom) map (_.query) map runUpdate).sequence
        .unsafeRunSync()

      findTopmostSameAsAndDerivedFrom(entityId) shouldBe sameAs.some -> derivedFrom.some

      val newTopmostSameAs = datasetIdSameAs.generateOne

      (updatesCreator.prepareUpdates(entityId, newTopmostSameAs, derivedFrom) map (_.query) map runUpdate).sequence
        .unsafeRunSync()

      findTopmostSameAsAndDerivedFrom(entityId) shouldBe newTopmostSameAs.some -> derivedFrom.some
    }

    "generate a query updating renku:topmostDerivedFrom if already present" in new TestCase {

      `given Dataset present in the triple store`

      (updatesCreator.prepareUpdates(entityId, sameAs, derivedFrom) map (_.query) map runUpdate).sequence
        .unsafeRunSync()

      findTopmostSameAsAndDerivedFrom(entityId) shouldBe sameAs.some -> derivedFrom.some

      val newTopmostDerivedFrom = datasetDerivedFroms.generateOne

      (updatesCreator.prepareUpdates(entityId, sameAs, newTopmostDerivedFrom) map (_.query) map runUpdate).sequence
        .unsafeRunSync()

      findTopmostSameAsAndDerivedFrom(entityId) shouldBe sameAs.some -> newTopmostDerivedFrom.some
    }

    "not generate triples for a Dataset which does not exists" in new TestCase {

      findTopmostSameAsAndDerivedFrom(entityId) shouldBe None -> None

      (updatesCreator.prepareUpdates(entityId, sameAs, derivedFrom) map (_.query) map runUpdate).sequence
        .unsafeRunSync()

      findTopmostSameAsAndDerivedFrom(entityId) shouldBe None -> None

    }

  }

  private trait TestCase {
    val identifier  = datasetIdentifiers.generateOne
    val entityId    = DataSet.entityId(identifier)(renkuBaseUrl)
    val derivedFrom = DerivedFrom(entityId)
    val sameAs      = SameAs(entityId)

    val updatesCreator = new UpdatesCreator()

    def `given Dataset present in the triple store` =
      loadToStore(JsonLDTriples {
        nonModifiedDataSetCommit()()(datasetIdentifier = identifier, maybeDatasetSameAs = None).toJson
      })

  }

  private def findTopmostSameAsAndDerivedFrom(entityId: EntityId): (Option[SameAs], Option[DerivedFrom]) =
    runQuery(s"""|SELECT ?topmostSameAs ?topmostDerivedFrom
                 |WHERE {
                 |  <$entityId> rdf:type schema:Dataset.
                 |  OPTIONAL { <$entityId> renku:topmostSameAs ?topmostSameAs }.
                 |  OPTIONAL { <$entityId> renku:topmostDerivedFrom ?topmostDerivedFrom }.
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => row.get("topmostSameAs").map(SameAs(_)) -> row.get("topmostDerivedFrom").map(DerivedFrom(_))) match {
      case Nil                => (None, None)
      case maybeSameAs +: Nil => maybeSameAs
      case _                  => fail(s"More than one record for dataset with $entityId id")
    }
}
