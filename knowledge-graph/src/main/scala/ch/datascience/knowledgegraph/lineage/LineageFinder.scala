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

import cats.effect._
import cats.implicits._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.projects.{FilePath, ProjectPath, ProjectResource}
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.logging.ApplicationLogger
import ch.datascience.rdfstore._
import eu.timepit.refined.auto._
import io.chrisdavenport.log4cats.Logger
import model._

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

trait LineageFinder[Interpretation[_]] {
  def findLineage(projectPath: ProjectPath, filePath: FilePath): Interpretation[Option[Lineage]]
}

class IOLineageFinder(
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder)
    with LineageFinder[IO] {

  override def findLineage(projectPath: ProjectPath, filePath: FilePath): IO[Option[Lineage]] =
    for {
      edges <- queryExpecting[Set[Edge]](using = query(projectPath, filePath))
      nodes <- edges.toNodeIdSet.toList
                .map(toNodeQuery(projectPath))
                .map(queryExpecting[Option[Node]](_).flatMap(toNodeOrError(projectPath)))
                .parSequence
                .map(_.toSet)
      maybeLineage <- toLineage(edges, nodes)
    } yield maybeLineage

  private def query(path: ProjectPath, filePath: FilePath) = SparqlQuery(
    name = "lineage",
    Set(
      "PREFIX prov: <http://www.w3.org/ns/prov#>",
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX wfprov: <http://purl.org/wf4ever/wfprov#>",
      "PREFIX schema: <http://schema.org/>"
    ),
    s"""|SELECT ?sourceId ?targetId
        |WHERE {
        |  {
        |    SELECT (MIN(?startedAt) AS ?minStartedAt)
        |    WHERE {
        |      ?qentity schema:isPartOf ${ProjectResource(renkuBaseUrl, path).showAs[RdfResource]};
        |               prov:atLocation "$filePath".
        |      ?qentity (prov:qualifiedGeneration/prov:activity | ^prov:entity/^prov:qualifiedUsage) ?activityId.
        |      ?activityId rdf:type <http://www.w3.org/ns/prov#Activity>;
        |                  prov:startedAtTime ?startedAt.
        |    }
        |  } {
        |    SELECT ?entity
        |    WHERE {
        |      ?qentity schema:isPartOf ${ProjectResource(renkuBaseUrl, path).showAs[RdfResource]};
        |               prov:atLocation "$filePath".
        |      ?qentity (prov:qualifiedGeneration/prov:activity | ^prov:entity/^prov:qualifiedUsage) ?activityId.
        |      ?activityId rdf:type <http://www.w3.org/ns/prov#Activity>;
        |                  prov:startedAtTime ?minStartedAt.
        |      ?qentity (
        |        ^(prov:qualifiedGeneration/prov:activity/prov:qualifiedUsage/prov:entity)* | (prov:qualifiedGeneration/prov:activity/prov:qualifiedUsage/prov:entity)*
        |      ) ?entity .
        |    }
        |    GROUP BY ?entity
        |  } {
        |    ?entity prov:qualifiedGeneration/prov:activity ?activity.
        |    FILTER NOT EXISTS {?activity rdf:type wfprov:WorkflowRun}
        |    FILTER EXISTS {?activity rdf:type wfprov:ProcessRun}
        |    BIND (?entity AS ?targetId)
        |    BIND (?activity AS ?sourceId)
        |  } UNION {
        |    ?activity prov:qualifiedUsage/prov:entity ?entity.
        |    FILTER NOT EXISTS {?activity rdf:type wfprov:WorkflowRun}
        |    FILTER EXISTS {?activity rdf:type wfprov:ProcessRun}
        |    BIND (?activity AS ?targetId)
        |    BIND (?entity AS ?sourceId)
        |  }
        |}
        |""".stripMargin
  )

  private def toNodeQuery(path: ProjectPath)(nodeId: Node.Id) = SparqlQuery(
    name = "lineage - node details",
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
    ),
    s"""|SELECT ?id ?type ?label ?maybeComment
        |WHERE {
        |  {
        |    ${nodeId.showAs[RdfResource]} rdf:type ?type;
        |                                  rdfs:label ?label.
        |    OPTIONAL { ${nodeId.showAs[RdfResource]} rdfs:comment ?maybeComment }
        |    BIND (${nodeId.showAs[RdfResource]} AS ?id)
        |  }
        |}
        |""".stripMargin
  )

  import ch.datascience.tinytypes.json.TinyTypeDecoders
  import io.circe.Decoder

  private implicit val nodeIdDecoder: Decoder[Node.Id] = TinyTypeDecoders.stringDecoder(Node.Id)

  private implicit val edgesDecoder: Decoder[Set[Edge]] = {
    implicit lazy val edgeDecoder: Decoder[Edge] = { implicit cursor =>
      for {
        sourceId <- cursor.downField("sourceId").downField("value").as[Node.Id]
        targetId <- cursor.downField("targetId").downField("value").as[Node.Id]
      } yield Edge(sourceId, targetId)
    }

    _.downField("results").downField("bindings").as[List[Edge]].map(_.toSet)
  }

  private implicit val nodeDecoder: Decoder[Option[Node]] = {
    implicit val labelDecoder: Decoder[Node.Label] = TinyTypeDecoders.stringDecoder(Node.Label)
    implicit val typeDecoder:  Decoder[Node.Type]  = TinyTypeDecoders.stringDecoder(Node.Type)

    implicit lazy val fieldsDecoder: Decoder[(Node.Id, Node.Type, Node.Label)] = { implicit cursor =>
      for {
        id           <- cursor.downField("id").downField("value").as[Node.Id]
        nodeType     <- cursor.downField("type").downField("value").as[Node.Type]
        label        <- cursor.downField("label").downField("value").as[Node.Label]
        maybeComment <- cursor.downField("maybeComment").downField("value").as[Option[Node.Label]]
      } yield (id, nodeType, maybeComment getOrElse label)
    }

    lazy val maybeToNode: List[(Node.Id, Node.Type, Node.Label)] => Option[Node] = {
      case Nil => None
      case (id, typ, label) +: tail =>
        Some {
          tail.foldLeft(Node(id, label, Set(typ))) {
            case (node, (`id`, t, `label`)) => node.copy(types = node.types + t)
          }
        }
    }

    _.downField("results")
      .downField("bindings")
      .as[List[(Node.Id, Node.Type, Node.Label)]]
      .map(maybeToNode)
  }

  private implicit class EdgesOps(edges: Set[Edge]) {
    lazy val toNodeIdSet: Set[Node.Id] = edges.foldLeft(Set.empty[Node.Id]) {
      case (acc, Edge(leftEdge, rightEdge)) => acc + leftEdge + rightEdge
    }
  }

  private def toNodeOrError(projectPath: ProjectPath): Option[Node] => IO[Node] = {
    case Some(node) => node.pure[IO]
    case _          => new Exception(s"Cannot find node details for $projectPath").raiseError[IO, Node]
  }

  private lazy val toLineage: (Set[Edge], Set[Node]) => IO[Option[Lineage]] = {
    case (edges, _) if edges.isEmpty => IO.pure(Option.empty)
    case (edges, nodes)              => Lineage.from[IO](edges, nodes) map Option.apply
  }
}

object IOLineageFinder {

  def apply(
      timeRecorder:            SparqlQueryTimeRecorder[IO],
      rdfStoreConfig:          IO[RdfStoreConfig] = RdfStoreConfig[IO](),
      renkuBaseUrl:            IO[RenkuBaseUrl] = RenkuBaseUrl[IO](),
      logger:                  Logger[IO] = ApplicationLogger
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[LineageFinder[IO]] =
    for {
      config       <- rdfStoreConfig
      renkuBaseUrl <- renkuBaseUrl
    } yield new IOLineageFinder(
      config,
      renkuBaseUrl,
      logger,
      timeRecorder
    )
}
