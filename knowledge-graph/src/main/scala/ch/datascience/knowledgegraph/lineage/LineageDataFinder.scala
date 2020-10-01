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
import cats.syntax.all._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.projects.{Path, ResourceId}
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.knowledgegraph.lineage.model._
import ch.datascience.rdfstore._
import eu.timepit.refined.auto._
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext

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

  private type EdgeData = (Node.Location, Option[Node.Location], Option[Node.Location])

  override def find(projectPath: Path): OptionT[IO, Lineage] = OptionT {
    for {
      edges <- queryExpecting[Set[EdgeData]](using = query(projectPath))
      nodes <- edges.toNodesLocations
                 .flatMap(toNodeQueries(projectPath))
                 .map(queryExpecting[Option[Node]](_).flatMap(toNodeOrError(projectPath)))
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
    s"""|SELECT DISTINCT ?command ?sourceEntityLocation ?targetEntityLocation
        |WHERE {
        |  {
        |    ?sourceEntity schema:isPartOf ${ResourceId(renkuBaseUrl, path).showAs[RdfResource]};
        |                  prov:atLocation ?sourceEntityLocation.
        |    ?runPlan renku:hasInputs/renku:consumes ?sourceEntity.
        |    FILTER NOT EXISTS {?runPlan renku:hasSubprocess | ^renku:hasSubprocess ?subprocess}
        |    ?activity prov:qualifiedAssociation/prov:hadPlan ?runPlan;
        |              rdfs:comment ?command.
        |  } UNION {
        |    ?targetEntity schema:isPartOf ${ResourceId(renkuBaseUrl, path).showAs[RdfResource]};
        |                  prov:atLocation ?targetEntityLocation.
        |    ?runPlan renku:hasOutputs/renku:produces ?targetEntity.
        |    FILTER NOT EXISTS {?runPlan renku:hasSubprocess | ^renku:hasSubprocess ?subprocess}
        |    ?activity prov:qualifiedAssociation/prov:hadPlan ?runPlan;
        |              rdfs:comment ?command.
        |  }
        |}
        |""".stripMargin
  )

  private def toNodeQueries(project: Path)(commandAndEntities: (Node.Location, Set[Node.Location])) = {
    val (command, entities) = commandAndEntities
    entities.map(toEntityDetailsQuery(project)) + toCommandDetailsQuery(project, command)
  }

  private def toCommandDetailsQuery(path: Path, command: Node.Location) = SparqlQuery(
    name = "lineage - command details",
    Set(
      "PREFIX prov: <http://www.w3.org/ns/prov#>",
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
      "PREFIX schema: <http://schema.org/>"
    ),
    s"""|SELECT DISTINCT ?type ?location ?label
        |WHERE {
        |  ?activity schema:isPartOf ${ResourceId(renkuBaseUrl, path).showAs[RdfResource]};
        |            rdf:type prov:Activity;
        |            rdf:type ?type;
        |            rdfs:comment '$command'.
        |  BIND ('$command' AS ?location)
        |  BIND ('$command' AS ?label)
        |}
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

  private implicit val nodeIdDecoder:   Decoder[Node.Id]       = TinyTypeDecoders.stringDecoder(Node.Id)
  private implicit val locationDecoder: Decoder[Node.Location] = TinyTypeDecoders.stringDecoder(Node.Location)

  private implicit val edgesDecoder: Decoder[Set[EdgeData]] = {
    implicit lazy val edgeDecoder: Decoder[EdgeData] = { implicit cursor =>
      for {
        command        <- cursor.downField("command").downField("value").as[Node.Location]
        sourceLocation <- cursor.downField("sourceEntityLocation").downField("value").as[Option[Node.Location]]
        targetLocation <- cursor.downField("targetEntityLocation").downField("value").as[Option[Node.Location]]
      } yield (command, sourceLocation, targetLocation)
    }

    _.downField("results").downField("bindings").as[List[EdgeData]].map(_.toSet)
  }

  private implicit val nodeDecoder: Decoder[Option[Node]] = {
    implicit val labelDecoder: Decoder[Node.Label] = TinyTypeDecoders.stringDecoder(Node.Label)
    implicit val typeDecoder:  Decoder[Node.Type]  = TinyTypeDecoders.stringDecoder(Node.Type)

    implicit lazy val fieldsDecoder: Decoder[(Node.Location, Node.Type, Node.Label)] = { implicit cursor =>
      for {
        nodeType <- cursor.downField("type").downField("value").as[Node.Type]
        location <- cursor.downField("location").downField("value").as[Node.Location]
        label    <- cursor.downField("label").downField("value").as[Node.Label]
      } yield (location, nodeType, label)
    }

    lazy val maybeToNode: List[(Node.Location, Node.Type, Node.Label)] => Option[Node] = {
      case Nil => None
      case (location, typ, label) +: tail =>
        Some {
          tail.foldLeft(Node(location, label, Set(typ))) { case (node, (`location`, t, `label`)) =>
            node.copy(types = node.types + t)
          }
        }
    }

    _.downField("results")
      .downField("bindings")
      .as[List[(Node.Location, Node.Type, Node.Label)]]
      .map(maybeToNode)
  }

  private implicit class EdgesAndLocationsOps(edges: Set[EdgeData]) {

    lazy val toNodesLocations: Map[Node.Location, Set[Node.Location]] =
      edges.foldLeft(Map.empty[Node.Location, Set[Node.Location]]) {
        case (locations, (command, maybeSource, maybeTarget)) =>
          locations.get(command) match {
            case None           => locations + (command -> Set(maybeSource, maybeTarget).flatten)
            case Some(entities) => locations + (command -> (entities ++ Set(maybeSource, maybeTarget).flatten))
          }
      }
  }

  private def toNodeOrError(projectPath: Path): Option[Node] => IO[Node] = {
    case Some(node) => node.pure[IO]
    case _          => new Exception(s"Cannot find node details for $projectPath").raiseError[IO, Node]
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
      case (command, Some(sourceLocation), None) => Edge(sourceLocation, command).pure[IO]
      case (command, None, Some(targetLocation)) => Edge(command, targetLocation).pure[IO]
      case _                                     => new Exception("Cannot instantiate an Edge").raiseError[IO, Edge]
    }).toList.sequence.map(_.toSet)
  }
}

private object IOLineageDataFinder {

  def apply(
      timeRecorder:   SparqlQueryTimeRecorder[IO],
      rdfStoreConfig: IO[RdfStoreConfig] = RdfStoreConfig[IO](),
      renkuBaseUrl:   IO[RenkuBaseUrl] = RenkuBaseUrl[IO](),
      logger:         Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[LineageDataFinder[IO]] =
    for {
      config       <- rdfStoreConfig
      renkuBaseUrl <- renkuBaseUrl
    } yield new IOLineageDataFinder(
      config,
      renkuBaseUrl,
      logger,
      timeRecorder
    )
}
