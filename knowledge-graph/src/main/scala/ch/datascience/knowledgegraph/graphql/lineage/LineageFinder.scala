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
import ch.datascience.control.Throttler
import ch.datascience.graph.model.events.{CommitId, ProjectPath}
import ch.datascience.http.client.IORestClient.validateUri
import ch.datascience.http.client.{BasicAuthCredentials, IORestClient}
import ch.datascience.knowledgegraph.config.RenkuBaseUrl
import ch.datascience.knowledgegraph.graphql.lineage.QueryFields.FilePath
import ch.datascience.knowledgegraph.graphql.lineage.model.Node.{SourceNode, TargetNode}
import ch.datascience.knowledgegraph.graphql.lineage.model._
import ch.datascience.knowledgegraph.rdfstore.RDFStoreConfig
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import ch.datascience.logging.{ApplicationLogger, ExecutionTimeRecorder}
import ch.datascience.tinytypes.{From, TinyType}
import io.chrisdavenport.log4cats.Logger
import org.http4s.Uri

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

trait LineageFinder[Interpretation[_]] {
  def findLineage(projectPath:   ProjectPath,
                  maybeCommitId: Option[CommitId],
                  maybeFilePath: Option[FilePath]): Interpretation[Option[Lineage]]
}

class IOLineageFinder(
    sparqlEndpoint:          Uri,
    userCredentials:         BasicAuthCredentials,
    renkuBaseUrl:            RenkuBaseUrl,
    executionTimeRecorder:   ExecutionTimeRecorder[IO],
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORestClient[Any](Throttler.noThrottling, logger)
    with LineageFinder[IO] {

  import IOLineageFinder._
  import executionTimeRecorder._
  import org.http4s.MediaType.application
  import org.http4s.MediaType.application._
  import org.http4s.Method.POST
  import org.http4s.Status.Ok
  import org.http4s.headers._
  import org.http4s.{Request, Response, Status}

  override def findLineage(projectPath:   ProjectPath,
                           maybeCommitId: Option[CommitId],
                           maybeFilePath: Option[FilePath]): IO[Option[Lineage]] =
    measureExecutionTime {
      for {
        edges        <- send(queryRequest(projectPath, maybeCommitId, maybeFilePath))(mapResponse)
        maybeLineage <- toLineage(edges)
      } yield maybeLineage
    } flatMap logExecutionTime(projectPath, maybeCommitId, maybeFilePath)

  private def queryRequest(projectPath: ProjectPath, maybeCommitId: Option[CommitId], maybeFilePath: Option[FilePath]) =
    request(POST, sparqlEndpoint, userCredentials)
      .withEntity(s"query=${Query.create(projectPath, maybeCommitId, maybeFilePath)}")
      .putHeaders(`Content-Type`(`x-www-form-urlencoded`), Accept(application.json))

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Set[Edge]]] = {
    case (Ok, _, response) => response.as[Set[Edge]]
  }

  private lazy val toLineage: Set[Edge] => IO[Option[Lineage]] = {
    case edges if edges.isEmpty => IO.pure(Option.empty)
    case edges =>
      val nodes                              = collectNodes(edges)
      val (edgesForRemoval, nodesForRemoval) = orphanRenkuNodes(edges, nodes)
      Lineage
        .from[IO](edges diff edgesForRemoval, nodes diff nodesForRemoval)
        .map(Option.apply)
  }

  private def collectNodes(edges: Set[Edge]): Set[Node] =
    edges.foldLeft(Set.empty[Node])((nodes, edge) => nodes + edge.source + edge.target)

  private def orphanRenkuNodes(edges: Set[Edge], nodes: Set[Node]): (Set[Edge], Set[Node]) =
    nodes.filter(_.label.value.startsWith("renku")).foldLeft(Set.empty[Edge] -> Set.empty[Node]) {
      case ((edgesForRemoval, nodesForRemoval), renkuNode) =>
        val nodesMatchingTargetEdges = edges filter (_.target == renkuNode)
        val nodesMatchingSourceEdges = edges filter (_.source == renkuNode)

        if (nodesMatchingTargetEdges.size == 1 && nodesMatchingSourceEdges.isEmpty)
          edgesForRemoval + nodesMatchingTargetEdges.head -> (nodesForRemoval + renkuNode)
        else if (nodesMatchingSourceEdges.size == 1 && nodesMatchingTargetEdges.isEmpty)
          (edgesForRemoval + nodesMatchingSourceEdges.head) -> (nodesForRemoval + renkuNode)
        else
          edgesForRemoval -> nodesForRemoval
    }

  private def logExecutionTime(
      projectPath:   ProjectPath,
      maybeCommitId: Option[CommitId],
      maybeFilePath: Option[FilePath]
  ): ((ElapsedTime, Option[Lineage])) => IO[Option[Lineage]] = {
    case (elapsedTime, maybeLineage) =>
      logger.info(
        s"Found lineage for $projectPath " +
          s"${maybeCommitId.map(commit => s"$commit: commitId ").getOrElse("")}" +
          s"${maybeFilePath.map(filePath => s"$filePath: file ").getOrElse("")}" +
          s"in ${elapsedTime}ms"
      )
      IO.pure(maybeLineage)
  }

  private object Query {

    def create(projectPath: ProjectPath, maybeCommitId: Option[CommitId], maybeFilePath: Option[FilePath]): String =
      createQuery(withFilterOn(projectPath, maybeCommitId, maybeFilePath))

    private def createQuery(filter: String) =
      s"""
         |PREFIX prov: <http://www.w3.org/ns/prov#>
         |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX wfdesc: <http://purl.org/wf4ever/wfdesc#>
         |PREFIX wf: <http://www.w3.org/2005/01/wf/flow#>
         |PREFIX wfprov: <http://purl.org/wf4ever/wfprov#>
         |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
         |PREFIX dcterms: <http://purl.org/dc/terms/>
         |SELECT ?target ?source ?target_label ?source_label
         |WHERE {
         |  {
         |    SELECT ?entity
         |    WHERE {
         |      $filter
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

    private def withFilterOn(projectPath:   ProjectPath,
                             maybeCommitId: Option[CommitId],
                             maybeFilePath: Option[FilePath]): String =
      s"""
         |      ?qentity dcterms:isPartOf ?project .
         |      FILTER (?project = <${renkuBaseUrl / projectPath}>)
         |      ${filterOn(maybeCommitId)}
         |      ${filterOn(maybeCommitId, maybeFilePath)}
         |""".stripMargin

    private def filterOn(maybeCommitId: Option[CommitId]): String =
      maybeCommitId
        .map { commitId =>
          s"""
             |    ?qentity (prov:qualifiedGeneration/prov:activity | ^prov:entity/^prov:qualifiedUsage) ?qactivity .
             |    FILTER (?qactivity = <file:///commit/$commitId>)
       """.stripMargin
        }
        .getOrElse("")

    private def filterOn(maybeCommitId: Option[CommitId], maybeFilePath: Option[FilePath]): String =
      (maybeCommitId, maybeFilePath)
        .mapN { (commitId, filePath) =>
          s"""
             |    FILTER (?qentity = <file:///blob/$commitId/$filePath>)
       """.stripMargin
        }
        .getOrElse("")
  }
}

object IOLineageFinder {

  def apply(
      rdfStoreConfig:          IO[RDFStoreConfig] = RDFStoreConfig[IO](),
      renkuBaseUrl:            IO[RenkuBaseUrl] = RenkuBaseUrl[IO](),
      logger:                  Logger[IO] = ApplicationLogger
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[LineageFinder[IO]] =
    for {
      config         <- rdfStoreConfig
      sparqlEndpoint <- validateUri(s"${config.fusekiBaseUrl}/${config.datasetName}/sparql")
      renkuBaseUrl   <- renkuBaseUrl
    } yield
      new IOLineageFinder(
        sparqlEndpoint,
        config.authCredentials,
        renkuBaseUrl,
        new ExecutionTimeRecorder[IO],
        logger
      )

  import io.circe.{Decoder, DecodingFailure, HCursor}
  import org.http4s.EntityDecoder
  import org.http4s.circe._

  private implicit val edgesDecoder: EntityDecoder[IO, Set[Edge]] = {

    def to[TT <: TinyType[String]](tinyTypeFactory: From[String, TT])(value: String): DecodingFailure Either TT =
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

    implicit lazy val edgesDecoder: Decoder[Set[Edge]] =
      _.downField("results").downField("bindings").as[List[Edge]].map(_.toSet)

    jsonOf[IO, Set[Edge]]
  }
}
