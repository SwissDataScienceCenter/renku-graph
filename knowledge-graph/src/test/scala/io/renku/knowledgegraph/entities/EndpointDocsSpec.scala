package io.renku.knowledgegraph.entities

import io.renku.generators.CommonGraphGenerators.renkuApiUrls
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.gitLabUrls
import io.renku.knowledgegraph.docs.OpenApiTester._
import org.scalatest.wordspec.AnyWordSpec

class EndpointDocsSpec extends AnyWordSpec {

  private lazy val renkuUrl  = renkuApiUrls.generateOne
  private lazy val gitLabUrl = gitLabUrls.generateOne

  "path" should {

    "return a valid Path object" in {
      validatePath(EndpointDocs(gitLabUrl, renkuUrl).path)
    }
  }
}
