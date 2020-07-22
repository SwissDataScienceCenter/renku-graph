/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import cats.data.OptionT
import cats.effect._
import cats.implicits._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.projects.{Path, ResourceId}
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.knowledgegraph.lineage.model._
import ch.datascience.rdfstore._
import eu.timepit.refined.auto._
import io.chrisdavenport.log4cats.Logger
import io.renku.jsonld.EntityId

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait LineageDataFinder[Interpretation[_]] {
  def find(projectPath: Path): OptionT[Interpretation, Lineage]
}

private class IOLineageDataFinder(
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder)
    with LineageDataFinder[IO] {

  private type EdgeData = (EntityId, Option[Node.Location], Option[Node.Location])

  override def find(projectPath: Path): OptionT[IO, Lineage] = OptionT {
    for {
      edges <- queryExpecting[Set[EdgeData]](using = query(projectPath))
      nodes <- edges.toNodesLocations
                .flatMap(toNodeQueries(projectPath))
                .map {
                  case (location, query) =>
                    queryExpecting[Option[Node]](query).flatMap(toNodeOrError(projectPath, location))
                }
                .toList
                .parSequence
                .map(_.toSet)
      maybeLineage <- toLineage(edges, nodes)
    } yield maybeLineage
  }

  private def query(path: Path) = SparqlQuery(
    name = "lineage",
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

  private def toNodeQueries(project: Path)(commandAndEntities: (EntityId, Set[Node.Location])) = {
    val (runPlanId, entities) = commandAndEntities
    entities.map(location => location.toString -> toEntityDetailsQuery(project)(location)) +
      (runPlanId.toString -> toRunPlanDetailsQuery(runPlanId))
  }

  private def toRunPlanDetailsQuery(runPlanId: EntityId) = SparqlQuery(
    name = "lineage - runPlan details",
    Set(
      "PREFIX prov: <http://www.w3.org/ns/prov#>",
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>"
    ),
    s"""|SELECT DISTINCT  ?type (CONCAT(STR(?command), STR(' '), (GROUP_CONCAT(?commandParameter; separator=' '))) AS ?label) ?location
        |WHERE {
        |  {
        |SELECT DISTINCT ?command ?type ?location
        |WHERE{
        |    <$runPlanId> renku:command ?command.
        |    ?activity prov:qualifiedAssociation/prov:hadPlan <$runPlanId>;
        |              rdf:type ?type.
        |    BIND (<$runPlanId> AS ?location)
        |}  }
        |  {
        |    SELECT ?position ?commandParameter
        |    WHERE {
        |      {
        |        <$runPlanId> renku:hasInputs ?input .
        |        ?input renku:consumes/prov:atLocation ?maybeLocation.
        |        OPTIONAL { ?input renku:position ?maybePosition }.
        |        OPTIONAL { ?input renku:prefix ?maybePrefix }
        |        BIND (IF(bound(?maybePosition), STR(?maybeLocation), '') AS ?location) .
        |        BIND (IF(bound(?maybePosition), ?maybePosition, 1) AS ?position) .
        |        BIND (IF(bound(?maybePrefix), STR(?maybePrefix), '') AS ?prefix) .
        |        BIND (CONCAT(?prefix, STR(?location)) AS ?commandParameter) .
        |      } UNION {
        |        <$runPlanId> renku:hasOutputs ?output .
        |        ?output renku:produces/prov:atLocation ?maybeLocation .
        |        OPTIONAL { ?output renku:position ?maybePosition }.
        |        OPTIONAL { ?output renku:prefix ?maybePrefix }
        |        BIND (IF(bound(?maybePosition), STR(?maybeLocation), '') AS ?location) .
        |        BIND (IF(bound(?maybePosition), ?maybePosition, 1) AS ?position) .
        |        BIND (IF(bound(?maybePrefix), STR(?maybePrefix), '') AS ?prefix) .
        |        BIND (CONCAT(?prefix, STR(?location)) AS ?commandParameter) .
        |      } UNION {
        |        <$runPlanId> renku:hasArguments ?argument .
        |        ?argument renku:position ?position .
        |        OPTIONAL { ?argument renku:prefix ?maybePrefix }
        |        BIND (IF(bound(?maybePrefix), STR(?maybePrefix), '') AS ?commandParameter) .
        |      }
        |    }
        |    GROUP BY ?position ?commandParameter
        |    HAVING (COUNT(*) > 0)
        |    ORDER BY ?position
        |  }
        |}
        |GROUP BY ?command ?type ?location
        |""".stripMargin
  )

  private def toEntityDetailsQuery(path: Path)(entity: Node.Location) = SparqlQuery(
    name = "lineage - entity details",
    Set(
      "PREFIX prov: <http://www.w3.org/ns/prov#>",
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX schema: <http://schema.org/>"
    ),
    s"""|SELECT DISTINCT ?type ?location ?label
        |WHERE {
        |  ?entity schema:isPartOf ${ResourceId(renkuBaseUrl, path).showAs[RdfResource]};
        |          rdf:type prov:Entity;
        |          rdf:type ?type;
        |          prov:atLocation '$entity'.
        |  BIND ('$entity' AS ?location)
        |  BIND ('$entity' AS ?label)
        |}
        |""".stripMargin
  )

  import ch.datascience.tinytypes.json.TinyTypeDecoders
  import io.circe.Decoder
  import ch.datascience.knowledgegraph.lineage.IOLineageDataFinder._

  private implicit val nodeIdDecoder:   Decoder[Node.Id]       = TinyTypeDecoders.stringDecoder(Node.Id)
  private implicit val locationDecoder: Decoder[Node.Location] = TinyTypeDecoders.stringDecoder(Node.Location)

  private implicit val edgesDecoder: Decoder[Set[EdgeData]] = {
    implicit lazy val edgeDecoder: Decoder[EdgeData] = { implicit cursor =>
      for {
        runPlanId      <- cursor.downField("runPlan").downField("value").as[EntityId]
        sourceLocation <- cursor.downField("sourceEntityLocation").downField("value").as[Option[Node.Location]]
        targetLocation <- cursor.downField("targetEntityLocation").downField("value").as[Option[Node.Location]]
      } yield (runPlanId, sourceLocation, targetLocation)
    }

    _.downField("results").downField("bindings").as[List[EdgeData]].map(_.toSet)
  }

  private implicit val nodeDecoder: Decoder[Option[Node]] = {
    implicit val labelDecoder: Decoder[Node.Label] = TinyTypeDecoders.stringDecoder(Node.Label)
    implicit val typeDecoder:  Decoder[Node.Type]  = TinyTypeDecoders.stringDecoder(Node.Type)

    implicit lazy val fieldsDecoder: Decoder[Option[(Node.Location, Node.Type, Node.Label)]] = { cursor =>
      for {
        maybeNodeType <- cursor.downField("type").downField("value").as[Option[Node.Type]]
        maybeLocation <- cursor.downField("location").downField("value").as[Option[Node.Location]]
        maybeLabel    <- cursor.downField("label").downField("value").as[Option[Node.Label]].map(trimValue)
      } yield (maybeLocation, maybeNodeType, maybeLabel) mapN ((_, _, _))
    }

    lazy val trimValue: Option[Node.Label] => Option[Node.Label] = _.map(l => Node.Label(l.value.trim))

    lazy val maybeToNode: List[(Node.Location, Node.Type, Node.Label)] => Option[Node] = {
      case Nil => None
      case (location, typ, label) +: tail =>
        Some {
          tail.foldLeft(Node(location, label, Set(typ))) {
            case (node, (`location`, t, `label`)) => node.copy(types = node.types + t)
          }
        }
    }

    _.downField("results")
      .downField("bindings")
      .as[List[Option[(Node.Location, Node.Type, Node.Label)]]]
      .map(_.flatten)
      .map(maybeToNode)
  }

  private implicit class EdgesAndLocationsOps(edges: Set[EdgeData]) {

    lazy val toNodesLocations: Map[EntityId, Set[Node.Location]] =
      edges.foldLeft(Map.empty[EntityId, Set[Node.Location]]) {
        case (locations, (runPlanId, maybeSource, maybeTarget)) =>
          locations.get(runPlanId) match {
            case None           => locations + (runPlanId -> Set(maybeSource, maybeTarget).flatten)
            case Some(entities) => locations + (runPlanId -> (entities ++ Set(maybeSource, maybeTarget).flatten))
          }
      }
  }

  private def toNodeOrError(projectPath: Path, location: String): Option[Node] => IO[Node] = {
    case Some(node) => node.pure[IO]
    case _          => new Exception(s"Cannot find details of $location node for $projectPath").raiseError[IO, Node]
  }

  private lazy val toLineage: (Set[EdgeData], Set[Node]) => IO[Option[Lineage]] = {
    case (edgesAndLocations, _) if edgesAndLocations.isEmpty => IO.pure(Option.empty)
    case (edgesAndLocations, nodes) =>
      for {
        edges   <- edgesAndLocations.toEdges
        lineage <- Lineage.from[IO](edges, nodes) map Option.apply
      } yield lineage
  }

  private implicit class EdgeDataOps(edgesAndLocations: Set[EdgeData]) {
    lazy val toEdges: IO[Set[Edge]] = (edgesAndLocations map {
      case (runPlan, Some(sourceLocation), None) => Edge(sourceLocation, runPlan.toLocation).pure[IO]
      case (runPlan, None, Some(targetLocation)) => Edge(runPlan.toLocation, targetLocation).pure[IO]
      case _                                     => new Exception("Cannot instantiate an Edge").raiseError[IO, Edge]
    }).toList.sequence.map(_.toSet)
  }
}

private object IOLineageDataFinder {

  def apply(
      timeRecorder:            SparqlQueryTimeRecorder[IO],
      rdfStoreConfig:          IO[RdfStoreConfig] = RdfStoreConfig[IO](),
      renkuBaseUrl:            IO[RenkuBaseUrl] = RenkuBaseUrl[IO](),
      logger:                  Logger[IO]
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[LineageDataFinder[IO]] =
    for {
      config       <- rdfStoreConfig
      renkuBaseUrl <- renkuBaseUrl
    } yield new IOLineageDataFinder(
      config,
      renkuBaseUrl,
      logger,
      timeRecorder
    )

  implicit class EntityIdOps(entityId: EntityId) {
    lazy val toLocation: Node.Location = Node.Location(entityId.value.toString)
  }
}
