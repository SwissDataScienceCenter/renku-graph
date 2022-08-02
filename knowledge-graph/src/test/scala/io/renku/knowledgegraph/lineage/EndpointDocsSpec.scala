package io.renku.knowledgegraph.lineage

import io.renku.knowledgegraph.docs.OpenApiTester._
import org.scalatest.wordspec.AnyWordSpec

class EndpointDocsSpec extends AnyWordSpec {

  "path" should {

    "return a valid Path object" in {
      validatePath(EndpointDocs.path)
    }
  }
}
