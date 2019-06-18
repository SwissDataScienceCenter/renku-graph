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

package ch.datascience.graphservice.graphql.lineage

import cats.effect._
import ch.datascience.graph.model.events.{CommitId, ProjectPath}
import ch.datascience.graphservice.config.RenkuBaseUrl
import ch.datascience.graphservice.graphql.lineage.QueryFields.FilePath
import ch.datascience.graphservice.graphql.lineage.model.Node.{SourceNode, TargetNode}
import ch.datascience.graphservice.graphql.lineage.model._
import ch.datascience.graphservice.rdfstore.{IORDFConnectionResourceBuilder, RDFConnectionResourceBuilder}
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import ch.datascience.logging.{ApplicationLogger, ExecutionTimeRecorder}
import io.chrisdavenport.log4cats.Logger

import scala.collection.JavaConverters._
import scala.language.higherKinds

trait LineageFinder[Interpretation[_]] {
  def findLineage(projectPath:   ProjectPath,
                  maybeCommitId: Option[CommitId],
                  maybeFilePath: Option[FilePath]): Interpretation[Option[Lineage]]
}

class IOLineageFinder(
    connectionResourceBuilder: RDFConnectionResourceBuilder[IO],
    renkuBaseUrl:              RenkuBaseUrl,
    executionTimeRecorder:     ExecutionTimeRecorder[IO],
    logger:                    Logger[IO]
) extends LineageFinder[IO] {

  import cats.implicits._
  import executionTimeRecorder._

  override def findLineage(projectPath:   ProjectPath,
                           maybeCommitId: Option[CommitId],
                           maybeFilePath: Option[FilePath]): IO[Option[Lineage]] =
    measureExecutionTime {
      runQuery(createQuery(queryFilter(projectPath, maybeCommitId, maybeFilePath)))
    } flatMap logExecutionTime(projectPath, maybeCommitId, maybeFilePath)

  private def runQuery(query: String): IO[Option[Lineage]] = connectionResourceBuilder.resource.use { connection =>
    IO {
      val querySolutions = connection
        .query(query)
        .execSelect()
        .asScala

      if (querySolutions.isEmpty) None
      else {
        val (allNodes, allEdges) = querySolutions.foldLeft((Set.empty[Node], Set.empty[Edge])) {
          case ((nodes, edges), querySolution) =>
            val sourceNode = SourceNode(
              NodeId(querySolution.get("source").asResource().getURI),
              NodeLabel(querySolution.get("source_label").asLiteral().toString)
            )
            val targetNode = TargetNode(
              NodeId(querySolution.get("target").asResource().getURI),
              NodeLabel(querySolution.get("target_label").asLiteral().toString)
            )

            val newNodes = nodes + targetNode + sourceNode
            val newEdges = edges + Edge(sourceNode, targetNode)

            newNodes -> newEdges
        }

        val (nodesToRemove, edgesToRemove) =
          allNodes.filter(_.label.value.startsWith("renku")).foldLeft((Set.empty[Node], Set.empty[Edge])) {
            case ((nodesForRemoval, edgesForRemoval), node) =>
              val nodeMatchingTargetEdges = allEdges filter (_.target == node)
              val nodeMatchingSourceEdges = allEdges filter (_.source == node)

              if (nodeMatchingTargetEdges.size == 1 && nodeMatchingSourceEdges.isEmpty)
                (nodesForRemoval + node) -> (edgesForRemoval + nodeMatchingTargetEdges.head)
              else if (nodeMatchingSourceEdges.size == 1 && nodeMatchingTargetEdges.isEmpty)
                (nodesForRemoval + node) -> (edgesForRemoval + nodeMatchingSourceEdges.head)
              else
                nodesForRemoval -> edgesForRemoval
          }

        Some(Lineage(allNodes diff nodesToRemove, allEdges diff edgesToRemove))
      }
    }
  }

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

  private def queryFilter(projectPath:   ProjectPath,
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
}

object IOLineageFinder {

  def apply()(implicit clock: Clock[IO]): IO[LineageFinder[IO]] =
    for {
      connectionResourceBuilder <- IORDFConnectionResourceBuilder()
      renkuBaseUrl              <- RenkuBaseUrl[IO]()
    } yield
      new IOLineageFinder(
        connectionResourceBuilder,
        renkuBaseUrl,
        new ExecutionTimeRecorder[IO],
        ApplicationLogger
      )
}
