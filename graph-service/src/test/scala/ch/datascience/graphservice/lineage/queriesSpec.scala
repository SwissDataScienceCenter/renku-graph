package ch.datascience.graphservice.lineage

import cats.effect.IO
import ch.datascience.graph.model.events.ProjectPath
import ch.datascience.graphservice.lineage.model.Edge.SourceEdge
import ch.datascience.graphservice.lineage.model.Lineage
import ch.datascience.graphservice.lineage.model.Node.SourceNode
import ch.datascience.graphservice.lineage.queries.lineageQuerySchema
import io.circe.Json
import io.circe.literal._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.concurrent.ScalaFutures
import sangria.ast.Document
import sangria.execution.Executor
import sangria.macros._
import sangria.marshalling.circe._

import scala.concurrent.ExecutionContext.Implicits.global

class queriesSpec extends WordSpec with ScalaFutures with MockFactory {

  "query" should {

    "allow to search for lineage for a projectId" in new TestCase {
      val query = graphql"""
        {
          lineage(projectPath: "namespace/project") {
            nodes {
              id
              label
            }
            edges {
              id
            }
          }
        }"""

      (lineageRepository
        .findLineage(_: ProjectPath))
        .expects(ProjectPath("namespace/project"))
        .returning(
          IO.pure(
            Some(
              Lineage(
                nodes = List(SourceNode("node-id", "node-label")),
                edges = List(SourceEdge("edge-id"))
              )
            )
          )
        )

      execute(query) shouldBe json"""
        {
          "data" : {
            "lineage" : {
              "nodes" : [
                {
                  "id" : "node-id",
                  "label" : "node-label"
                }
              ],
              "edges" : [
                {
                  "id" : "edge-id"
                }
              ]
            }
          }
        }"""
    }
  }

  private trait TestCase {
    val lineageRepository = mock[IOLineageRepository]

    def execute(query: Document): Json =
      Executor
        .execute(
          lineageQuerySchema,
          query,
          lineageRepository
        )
        .futureValue

  }
}
