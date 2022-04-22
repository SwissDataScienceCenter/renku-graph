package io.renku.graph.acceptancetests.knowledgegraph

import cats.syntax.all._
import io.circe.Json
import io.circe.literal._
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.graph.acceptancetests.data.{RdfStoreData, cliVersion, dataProjects}
import io.renku.graph.acceptancetests.flows.RdfStoreProvisioning
import io.renku.graph.acceptancetests.tooling.GraphServices
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.Schemas.prov
import io.renku.graph.model.projects
import io.renku.graph.model.testentities.LineageExemplarData.ExemplarData
import io.renku.graph.model.testentities.generators.EntitiesGenerators.{renkuProjectEntities, visibilityPublic}
import io.renku.graph.model.testentities.{LineageExemplarData, NodeDef}
import io.renku.http.client.AccessToken
import io.renku.jsonld.syntax._
import org.http4s.Status.Ok
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
//import org.http4s.implicits.http4sLiteralsSyntax

class LineageResourcesSpec
    extends AnyFeatureSpec
    with GivenWhenThen
    with GraphServices
    with RdfStoreProvisioning
    with RdfStoreData {

  Feature("GET knowledge-graph/projects/<namespace>/<name>/files/<location>/lineage to find a file's lineage") {
    val user = authUsers.generateOne
    implicit val accessToken: AccessToken = user.accessToken

    val (exemplarData, project) = {
      val lineageData = LineageExemplarData(
        renkuProjectEntities(visibilityPublic)
          .map(
            _.copy(
              path = projects.Path("public/lineage-project"),
              agent = cliVersion
            )
          )
          .generateOne
      )
      (lineageData, dataProjects(lineageData.project).generateOne)
    }

    /** Expected data structure when looking for the grid_plot file
     *
     * zhbikes folder clean_data \ / run plan 1 \ bikesParquet plot_data \ / run plan 2 / grid_plot
     */
    Scenario("As a user I would like to find project's lineage with a GraphQL query") {
      Given("some data in the RDF Store")
      val commitId = commitIds.generateOne
      mockDataOnGitLabAPIs(project, exemplarData.project.asJsonLD, commitId)
      `data in the RDF store`(project, commitId)

      When("user calls the lineage endpoint")
//      val stringUri = (uri"knowledge-graph" / "projects" / project.path.show / "files" / exemplarData.`clean_data entity`.location / "lineage").show
      val response =
        knowledgeGraphClient GET s"knowledge-graph/projects/${project.path}/files/${exemplarData.`clean_data entity`.location}/lineage"

      Then("they should get Ok response with project lineage in Json")
      response.status shouldBe Ok
      val lineageJson = response.jsonBody.hcursor.downField("data").downField("lineage")
      lineageJson.downField("edges").as[List[Json]].map(_.toSet) shouldBe theExpectedEdges(exemplarData)
      lineageJson.downField("nodes").as[List[Json]].map(_.toSet) shouldBe theExpectedNodes(exemplarData)

    }
  }

  private def theExpectedEdges(exemplarData: ExemplarData): Right[Nothing, Set[Json]] = {
    import exemplarData._
    Right {
      Set(
        json"""{"source": ${`zhbikes folder`.location},     "target": ${`activity3 node`.location}}""",
        json"""{"source": ${`clean_data entity`.location},  "target": ${`activity3 node`.location}}""",
        json"""{"source": ${`activity3 node`.location},     "target": ${`bikesparquet entity`.location}}""",
        json"""{"source": ${`bikesparquet entity`.location},"target": ${`activity4 node`.location}}""",
        json"""{"source": ${`plot_data entity`.location},   "target": ${`activity4 node`.location}}""",
        json"""{"source": ${`activity4 node`.location},     "target": ${`grid_plot entity`.location}}"""
      )
    }
  }

  private def theExpectedNodes(exemplarData: ExemplarData): Right[Nothing, Set[Json]] = {
    import exemplarData._
    Right {
      Set(
        json"""{"id": ${`zhbikes folder`.location},      "location": ${`zhbikes folder`.location},      "label": ${`zhbikes folder`.label},      "type": ${`zhbikes folder`.singleWordType}}""",
        json"""{"id": ${`activity3 node`.location},      "location": ${`activity3 node`.location},     "label": ${`activity3 node`.label},     "type": ${`activity3 node`.singleWordType}}""",
        json"""{"id": ${`clean_data entity`.location},   "location": ${`clean_data entity`.location},   "label": ${`clean_data entity`.label},   "type": ${`clean_data entity`.singleWordType}}""",
        json"""{"id": ${`bikesparquet entity`.location}, "location": ${`bikesparquet entity`.location}, "label": ${`bikesparquet entity`.label}, "type": ${`bikesparquet entity`.singleWordType}}""",
        json"""{"id": ${`plot_data entity`.location},    "location": ${`plot_data entity`.location},    "label": ${`plot_data entity`.label},    "type": ${`plot_data entity`.singleWordType}}""",
        json"""{"id": ${`activity4 node`.location},      "location": ${`activity4 node`.location},     "label": ${`activity4 node`.label},     "type": ${`activity4 node`.singleWordType}}""",
        json"""{"id": ${`grid_plot entity`.location},    "location": ${`grid_plot entity`.location},    "label": ${`grid_plot entity`.label},    "type": ${`grid_plot entity`.singleWordType}}"""
      )
    }
  }

  private implicit class NodeOps(node: NodeDef) {

    private lazy val FileTypes = Set((prov / "Entity").show)

    lazy val singleWordType: String = node.types match {
      case types if types contains (prov / "Activity").show   => "ProcessRun"
      case types if types contains (prov / "Collection").show => "Directory"
      case FileTypes                                          => "File"
    }
  }
}
