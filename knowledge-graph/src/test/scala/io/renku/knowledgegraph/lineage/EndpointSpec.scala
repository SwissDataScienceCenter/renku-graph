package io.renku.knowledgegraph.lineage

import cats.effect.IO
import cats.implicits._
import org.scalatest.wordspec.AnyWordSpec

class EndpointSpec extends AnyWordSpec {
  "Endpoint" should {
    "be created from a string" in new TestCase {}
  }

  private trait TestCase {
    val endpoint = Endpoint[IO]()
  }

}
