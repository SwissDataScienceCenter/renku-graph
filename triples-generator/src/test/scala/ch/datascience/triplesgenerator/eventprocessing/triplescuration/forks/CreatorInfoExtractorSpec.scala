package ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks

import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.Schemas.schema
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.rdfstore.JsonLDTriples
import io.renku.jsonld._
import io.renku.jsonld.generators.JsonLDGenerators._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class CreatorInfoExtractorSpec extends AnyWordSpec with should.Matchers {

  "extract" should {
    "return the name and the email of the creator in the JSONLD triples" in new TestCase {
      val creatorName              = userNames.generateOne
      val creatorEmail             = userEmails.generateOne
      val creatorJsonLDEntityId    = entityIds.generateOne
      val creatorJsonLDIdContainer = JsonLD.fromEntityId(creatorJsonLDEntityId)

      val creatorJsonLDEntity =
        JsonLD.entity(
          creatorJsonLDEntityId,
          EntityTypes(entityTypes.generateNonEmptyList() :+ EntityType.of(schema / "Person")),
          Map(schema / "name"  -> JsonLD.fromString(creatorName.toString),
              schema / "email" -> JsonLD.fromString(creatorEmail.toString)
          )
        )
      val json = JsonLD.arr(
        JsonLD.entity(
          entityIds.generateOne,
          entityTypesObject.generateOne,
          valuesProperties
            .generateNonEmptyList()
            .toList
            .toMap
            .updated(schema / "creator", creatorJsonLDIdContainer)
        ),
        creatorJsonLDEntity
      )

      CreatorInfoExtratorImpl.extract(JsonLDTriples(json.toJson)) shouldBe (creatorName.some, creatorEmail.some)

    }
  }

  private trait TestCase {}
}
