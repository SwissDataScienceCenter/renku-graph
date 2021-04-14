/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.datascience.knowledgegraph.lineage

import cats.effect._
import cats.syntax.all._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.projects.{Path, ResourceId}
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.knowledgegraph.lineage.model._
import ch.datascience.rdfstore._
import eu.timepit.refined.auto._
import org.typelevel.log4cats.Logger
import io.renku.jsonld.EntityId

import scala.concurrent.ExecutionContext

private trait EdgesFinder[Interpretation[_]] {
  def findEdges(projectPath: Path): Interpretation[EdgeMap]
}

private class IOEdgesFinder(
                             rdfStoreConfig: RdfStoreConfig,
                             renkuBaseUrl: RenkuBaseUrl,
                             logger: Logger[IO],
                             timeRecorder: SparqlQueryTimeRecorder[IO]
                           )(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
  extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder)
    with EdgesFinder[IO] {

  private type EdgeData = (EntityId, Option[Node.Location], Option[Node.Location])

  override def findEdges(projectPath: Path): IO[EdgeMap] =
    queryEdges(using = query(projectPath)) map (_.toNodesLocations)

  private def queryEdges(using: SparqlQuery): IO[Set[EdgeData]] = {
    val pageSize = 2000

    def fetchPaginatedResult(into: Set[EdgeData], using: SparqlQuery, offset: Int): IO[Set[EdgeData]] = {
      val queryWithOffset = using.copy(body = using.body + s"\nLIMIT $pageSize \nOFFSET $offset")
      for {
        edges <- queryExpecting[Set[EdgeData]](queryWithOffset)
        results <- edges match {
          case e if e.size < pageSize => (into ++ edges).pure[IO]
          case fullPage => fetchPaginatedResult(into ++ fullPage, using, offset + pageSize)
        }
      } yield results
    }

    fetchPaginatedResult(Set.empty[EdgeData], using, offset = 0)
  }

  private def query(path: Path) = SparqlQuery(
    name = "lineage - edges",
    Set(
      "PREFIX prov: <http://www.w3.org/ns/prov#>",
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
      "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>",
      "PREFIX wfprov: <http://purl.org/wf4ever/wfprov#>",
      "PREFIX schema: <http://schema.org/>"
    ),
    s"""|SELECT DISTINCT ?runPlan ?sourceEntityLocation ?targetEntityLocation
        |WHERE {
        |  {
        |    ?sourceEntity schema:isPartOf ${ResourceId(renkuBaseUrl, path).showAs[RdfResource]};
        |                  prov:atLocation ?sourceEntityLocation.
        |    ?runPlan renku:hasInputs/renku:consumes ?sourceEntity.
        |    ?activity prov:qualifiedAssociation/prov:hadPlan ?runPlan .
        |    FILTER NOT EXISTS {?activity rdf:type wfprov:WorkflowRun}
        |    FILTER NOT EXISTS {?activity wfprov:wasPartOfWorkflowRun ?workflow}
        |  } UNION {
        |    ?targetEntity schema:isPartOf ${ResourceId(renkuBaseUrl, path).showAs[RdfResource]};
        |                  prov:atLocation ?targetEntityLocation.
        |    ?runPlan renku:hasOutputs/renku:produces ?targetEntity.
        |    ?activity prov:qualifiedAssociation/prov:hadPlan ?runPlan .
        |    FILTER NOT EXISTS {?activity rdf:type wfprov:WorkflowRun}
        |    FILTER NOT EXISTS {?activity wfprov:wasPartOfWorkflowRun ?workflow}
        |  }
        |}
        |""".stripMargin
  )

  import io.circe.Decoder

  private implicit val edgesDecoder: Decoder[Set[EdgeData]] = {
    import ch.datascience.tinytypes.json.TinyTypeDecoders.stringDecoder

    implicit val locationDecoder: Decoder[Node.Location] = stringDecoder(Node.Location)

    implicit lazy val edgeDecoder: Decoder[EdgeData] = { implicit cursor =>
      for {
        runPlanId <- cursor.downField("runPlan").downField("value").as[EntityId]
        sourceLocation <- cursor.downField("sourceEntityLocation").downField("value").as[Option[Node.Location]]
        targetLocation <- cursor.downField("targetEntityLocation").downField("value").as[Option[Node.Location]]
      } yield (runPlanId, sourceLocation, targetLocation)
    }

    _.downField("results").downField("bindings").as[List[EdgeData]].map(_.toSet)
  }

  private implicit class EdgesAndLocationsOps(edges: Set[EdgeData]) {

    lazy val toNodesLocations: Map[EntityId, (Set[Node.Location], Set[Node.Location])] =
      edges.foldLeft(Map.empty[EntityId, (Set[Node.Location], Set[Node.Location])]) {
        case (locations, (runPlanId, Some(source), None)) =>
          locations.get(runPlanId) match {
            case None => locations + (runPlanId -> (Set(source), Set.empty))
            case Some((from, to)) => locations + (runPlanId -> (from + source, to))
          }
        case (locations, (runPlanId, None, Some(target))) =>
          locations.get(runPlanId) match {
            case None => locations + (runPlanId -> (Set.empty, Set(target)))
            case Some((from, to)) => locations + (runPlanId -> (from, to + target))
          }
        case (locations, _) => locations
      }
  }
}

private object IOEdgesFinder {

  def apply(
             timeRecorder: SparqlQueryTimeRecorder[IO],
             rdfStoreConfig: IO[RdfStoreConfig] = RdfStoreConfig[IO](),
             renkuBaseUrl: IO[RenkuBaseUrl] = RenkuBaseUrl[IO](),
             logger: Logger[IO]
           )(implicit
             executionContext: ExecutionContext,
             contextShift: ContextShift[IO],
             timer: Timer[IO]
           ): IO[EdgesFinder[IO]] =
    for {
      config <- rdfStoreConfig
      renkuBaseUrl <- renkuBaseUrl
    } yield new IOEdgesFinder(
      config,
      renkuBaseUrl,
      logger,
      timeRecorder
    )
}
