/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.knowledgegraph.graphql.lineage

import cats.effect._
import cats.implicits._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.events.{CommitId, ProjectPath}
import ch.datascience.knowledgegraph.graphql.lineage.QueryFields.FilePath
import ch.datascience.knowledgegraph.graphql.lineage.model.Node.{SourceNode, TargetNode}
import ch.datascience.knowledgegraph.graphql.lineage.model._
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import ch.datascience.logging.{ApplicationLogger, ExecutionTimeRecorder}
import ch.datascience.rdfstore.IORdfStoreClient.RdfQuery
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig}
import ch.datascience.tinytypes.{From, StringTinyType}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

trait LineageFinder[Interpretation[_]] {
  def findLineage(projectPath: ProjectPath, commitId: CommitId, filePath: FilePath): Interpretation[Option[Lineage]]
}

class IOLineageFinder(
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    executionTimeRecorder:   ExecutionTimeRecorder[IO],
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient[RdfQuery](rdfStoreConfig, logger)
    with LineageFinder[IO] {

  import IOLineageFinder._
  import executionTimeRecorder._

  override def findLineage(projectPath: ProjectPath, commitId: CommitId, filePath: FilePath): IO[Option[Lineage]] =
    measureExecutionTime {
      for {
        edges        <- queryExpecting[Set[Edge]](using = query(projectPath, commitId, filePath))
        maybeLineage <- toLineage(edges)
      } yield maybeLineage
    } flatMap logExecutionTime(projectPath, commitId, filePath)

  private lazy val toLineage: Set[Edge] => IO[Option[Lineage]] = {
    case edges if edges.isEmpty => IO.pure(Option.empty)
    case edges                  => Lineage.from[IO](edges, collectNodes(edges)) map Option.apply
  }

  private def collectNodes(edges: Set[Edge]): Set[Node] =
    edges.foldLeft(Set.empty[Node])(
      (nodes, edge) => nodes + edge.source + edge.target
    )

  private def logExecutionTime(
      projectPath: ProjectPath,
      commitId:    CommitId,
      filePath:    FilePath
  ): ((ElapsedTime, Option[Lineage])) => IO[Option[Lineage]] = {
    case (elapsedTime, maybeLineage) =>
      logger.info(s"Found lineage for $projectPath commit: $commitId filePath: $filePath in ${elapsedTime}ms")
      IO.pure(maybeLineage)
  }

  private def query(projectPath: ProjectPath, commitId: CommitId, filePath: FilePath): String =
    s"""
       |PREFIX prov: <http://www.w3.org/ns/prov#>
       |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       |PREFIX wfdesc: <http://purl.org/wf4ever/wfdesc#>
       |PREFIX wf: <http://www.w3.org/2005/01/wf/flow#>
       |PREFIX wfprov: <http://purl.org/wf4ever/wfprov#>
       |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
       |PREFIX schema: <http://schema.org/>
       |PREFIX dcterms: <http://purl.org/dc/terms/>
       |
       |SELECT ?target ?source ?target_label ?source_label
       |WHERE {
       |  {
       |    SELECT ?entity
       |    WHERE {
       |      ?qentity dcterms:isPartOf|schema:isPartOf ?project .
       |      FILTER (?project = <${renkuBaseUrl / projectPath}>)
       |      ?qentity (prov:qualifiedGeneration/prov:activity | ^prov:entity/^prov:qualifiedUsage) ?qactivity .
       |      FILTER (?qactivity = <file:///commit/$commitId>)
       |      FILTER (?qentity = <file:///blob/$commitId/$filePath>)
       |      ?qentity (
       |        ^(prov:qualifiedGeneration/prov:activity/prov:qualifiedUsage/prov:entity)* | (prov:qualifiedGeneration/prov:activity/prov:qualifiedUsage/prov:entity)*
       |      ) ?entity .
       |    }
       |    GROUP BY ?entity
       |  }
       |  {
       |    ?entity prov:qualifiedGeneration/prov:activity ?activity ;
       |            rdfs:label ?target_label .
       |    ?activity rdfs:comment ?source_label .
       |    FILTER NOT EXISTS {?activity rdf:type wfprov:WorkflowRun}
       |    FILTER EXISTS {?activity rdf:type wfprov:ProcessRun}
       |    BIND (?entity AS ?target)
       |    BIND (?activity AS ?source)
       |  } UNION {
       |    ?activity prov:qualifiedUsage/prov:entity ?entity ;
       |              rdfs:comment ?target_label .
       |    ?entity rdfs:label ?source_label .
       |    FILTER NOT EXISTS {?activity rdf:type wfprov:WorkflowRun}
       |    FILTER EXISTS {?activity rdf:type wfprov:ProcessRun}
       |    BIND (?activity AS ?target)
       |    BIND (?entity AS ?source)
       |  }
       |}""".stripMargin
}

object IOLineageFinder {

  def apply(
      rdfStoreConfig:          IO[RdfStoreConfig] = RdfStoreConfig[IO](),
      renkuBaseUrl:            IO[RenkuBaseUrl] = RenkuBaseUrl[IO](),
      logger:                  Logger[IO] = ApplicationLogger
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[LineageFinder[IO]] =
    for {
      config       <- rdfStoreConfig
      renkuBaseUrl <- renkuBaseUrl
    } yield
      new IOLineageFinder(
        config,
        renkuBaseUrl,
        new ExecutionTimeRecorder[IO],
        logger
      )

  import io.circe.{Decoder, DecodingFailure, HCursor}

  private implicit val edgesDecoder: Decoder[Set[Edge]] = {

    def to[TT <: StringTinyType](tinyTypeFactory: From[TT])(value: String): DecodingFailure Either TT =
      tinyTypeFactory
        .from(value)
        .leftMap(ex => DecodingFailure(ex.getMessage, Nil))

    def nodeIdAndLabel[N <: Node](parentField: String, apply: (NodeId, NodeLabel) => N)(
        implicit cursor:                       HCursor): Decoder.Result[N] =
      (
        cursor.downField(parentField).downField("value").as[String].flatMap(to(NodeId)),
        cursor.downField(s"${parentField}_label").downField("value").as[String].flatMap(to(NodeLabel))
      ) mapN apply

    implicit lazy val edgeDecoder: Decoder[Edge] = { implicit cursor =>
      for {
        sourceNode <- nodeIdAndLabel("source", SourceNode.apply)
        targetNode <- nodeIdAndLabel("target", TargetNode.apply)
      } yield Edge(sourceNode, targetNode)
    }

    _.downField("results").downField("bindings").as[List[Edge]].map(_.toSet)
  }
}
