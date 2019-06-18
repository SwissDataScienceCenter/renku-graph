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

import cats.MonadError
import cats.effect.{Clock, IO}
import cats.implicits._
import ch.datascience.graph.model.events.{CommitId, ProjectPath}
import ch.datascience.graphservice.config.GitLabBaseUrl
import ch.datascience.graphservice.graphql.lineage.QueryFields.FilePath
import ch.datascience.graphservice.graphql.lineage.model.Edge.{SourceEdge, TargetEdge}
import ch.datascience.graphservice.graphql.lineage.model.Node.{SourceNode, TargetNode}
import ch.datascience.graphservice.graphql.lineage.model.{Edge, Lineage, Node}
import ch.datascience.graphservice.rdfstore.{IORDFConnectionResource, RDFConnectionResource}
import ch.datascience.logging.{ApplicationLogger, ExecutionTimeRecorder}
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import io.chrisdavenport.log4cats.Logger

import scala.collection.JavaConverters._
import scala.language.higherKinds
import scala.util.Try

class LineageFinder[Interpretation[_]](
    rdfConnectionResource: RDFConnectionResource[Interpretation],
    gitLabBaseUrl:         GitLabBaseUrl,
    executionTimeRecorder: ExecutionTimeRecorder[Interpretation],
    logger:                Logger[Interpretation]
)(implicit ME:             MonadError[Interpretation, Throwable]) {

  import executionTimeRecorder._

  def findLineage(projectPath:   ProjectPath,
                  maybeCommitId: Option[CommitId],
                  maybeFilePath: Option[FilePath]): Interpretation[Option[Lineage]] =
    measureExecutionTime {
      runQuery(createQuery(queryFilter(projectPath, maybeCommitId, maybeFilePath)))
    } flatMap logExecutionTime(projectPath, maybeCommitId, maybeFilePath)

  private def runQuery(query: String): Interpretation[Option[Lineage]] = rdfConnectionResource.use { connection =>
    ME.fromTry {
      Try {
        val querySolutions = connection
          .query(query)
          .execSelect()
          .asScala

        if (querySolutions.isEmpty) None
        else {
          val (allNodes, allEdges) = querySolutions.foldLeft((List.empty[Node], List.empty[Edge])) {
            case ((nodes, edges), querySolution) =>
              val target      = querySolution.get("target").asResource().getURI
              val targetLabel = querySolution.get("target_label").asLiteral().toString
              val source      = querySolution.get("source").asResource().getURI
              val sourceLabel = querySolution.get("source_label").asLiteral().toString

              val newNodes = nodes :+ TargetNode(target, targetLabel) :+ SourceNode(source, sourceLabel)
              val newEdges = edges :+ TargetEdge(target) :+ SourceEdge(source)

              newNodes -> newEdges
          }

          val (nodesToRemove, edgesToRemove) =
            allNodes.filter(_.label.startsWith("renku")).foldLeft((List.empty[Node], List.empty[Edge])) {
              case ((nodesForRemoval, edgesForRemoval), node) =>
                val nodeMatchingTargetEdges = allEdges.filter {
                  case TargetEdge(node.id) => true
                  case _                   => false
                }
                val nodeMatchingSourceEdges = allEdges.filter {
                  case SourceEdge(node.id) => true
                  case _                   => false
                }

                if (nodeMatchingTargetEdges.size == 1 && nodeMatchingSourceEdges.isEmpty)
                  (nodesForRemoval :+ node) -> (edgesForRemoval :+ nodeMatchingTargetEdges.head)
                else if (nodeMatchingSourceEdges.size == 1 && nodeMatchingTargetEdges.isEmpty)
                  (nodesForRemoval :+ node) -> (edgesForRemoval :+ nodeMatchingSourceEdges.head)
                else
                  nodesForRemoval -> edgesForRemoval
            }

          Some(Lineage(allNodes diff nodesToRemove, allEdges diff edgesToRemove))
        }
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
       |        ^(prov:qualifiedGeneration/prov:activity/prov:qualifiedUsage/prov:entity)* |
       |        (prov:qualifiedGeneration/prov:activity/prov:qualifiedUsage/prov:entity)*
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
       |      FILTER (?project = <${gitLabBaseUrl / projectPath}>)
       |      ${filterOn(maybeCommitId)}
       |      ${filterOn(maybeCommitId, maybeFilePath)}
       |""".stripMargin

  private def filterOn(maybeCommitId: Option[CommitId]): String =
    maybeCommitId
      .map { commitId =>
        s"""
           |    ?qentity (prov:qualifiedGeneration/prov:activity | 
           |    ^prov:entity/^prov:qualifiedUsage) ?qactivity .
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
  ): ((ElapsedTime, Option[Lineage])) => Interpretation[Option[Lineage]] = {
    case (elapsedTime, maybeLineage) =>
      logger.info(
        s"Found lineage for $projectPath " +
          s"${maybeCommitId.map(commit => s"$commit: commitId ").getOrElse("")}" +
          s"${maybeFilePath.map(filePath => s"$filePath: file ").getOrElse("")}" +
          s"in ${elapsedTime}ms"
      )
      ME pure maybeLineage
  }
}

object IOLineageFinder {
  def apply()(implicit clock: Clock[IO]): IO[LineageFinder[IO]] =
    for {
      connectionResource <- IORDFConnectionResource()
      gitLabBaseUrl      <- GitLabBaseUrl[IO]()
    } yield new LineageFinder[IO](connectionResource, gitLabBaseUrl, new ExecutionTimeRecorder[IO], ApplicationLogger)
}
