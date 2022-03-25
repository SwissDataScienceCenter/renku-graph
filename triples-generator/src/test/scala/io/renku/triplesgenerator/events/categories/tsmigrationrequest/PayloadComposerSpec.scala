package io.renku.triplesgenerator.events.categories.tsmigrationrequest

import cats.syntax.all._
import io.circe.Json
import io.circe.literal._
import io.renku.generators.CommonGraphGenerators.{microserviceBaseUrls, serviceVersions}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.microservices.{MicroserviceBaseUrl, MicroserviceIdentifier, MicroserviceUrlFinder}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class PayloadComposerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "prepareSubscriptionPayload" should {

    "return Payload containing the given CategoryName, found subscriberUrl and capacity" in new TestCase {
      val microserviceUrl = microserviceBaseUrls.generateOne
      (urlFinder.findBaseUrl _)
        .expects()
        .returning(microserviceUrl.pure[Try])

      composer.prepareSubscriptionPayload() shouldBe json"""{
        "categoryName" : ${categoryName.value},
        "subscriber": {
          "url":     ${(microserviceUrl / "events").value},
          "id":      ${microserviceId.value},
          "version": ${serviceVersion.value}
        }
      }""".pure[Try]
    }

    "fail if finding subscriberUrl fails" in new TestCase {
      val exception = exceptions.generateOne
      (urlFinder.findBaseUrl _)
        .expects()
        .returning(exception.raiseError[Try, MicroserviceBaseUrl])

      composer.prepareSubscriptionPayload() shouldBe exception.raiseError[Try, Json]
    }
  }

  private trait TestCase {
    val urlFinder      = mock[MicroserviceUrlFinder[Try]]
    val microserviceId = MicroserviceIdentifier.generate
    val serviceVersion = serviceVersions.generateOne
    val composer       = new PayloadComposer[Try](urlFinder, microserviceId, serviceVersion)
  }
}
