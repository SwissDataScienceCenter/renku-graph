package ch.datascience.graphservice.lineage

import cats.MonadError
import cats.effect.IO
import ch.datascience.graph.model.events.ProjectPath
import ch.datascience.graphservice.config.GitLabBaseUrl
import ch.datascience.graphservice.lineage.model.Edge.{SourceEdge, TargetEdge}
import ch.datascience.graphservice.lineage.model.Node.{SourceNode, TargetNode}
import ch.datascience.graphservice.lineage.model.{Edge, Lineage, Node}
import ch.datascience.graphservice.rdfstore.{IORDFConnectionResource, RDFConnectionResource}

import scala.collection.JavaConverters._
import scala.language.higherKinds
import scala.util.Try

private class LineageRepository[Interpretation[_]](
    rdfConnectionResource: RDFConnectionResource[Interpretation],
    gitLabBaseUrl:         GitLabBaseUrl
)(implicit ME:             MonadError[Interpretation, Throwable]) {

  def findLineage(projectPath: ProjectPath): Interpretation[Option[Lineage]] =
    rdfConnectionResource.use { connection =>
      ME.fromTry {
        Try {
          val querySolutions = connection
            .query(createQuery(projectPath))
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

  private def createQuery(projectPath: ProjectPath) =
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
       |      ?qentity dcterms:isPartOf ?project .
       |      FILTER (?project = <${gitLabBaseUrl / projectPath}>)
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
}

private class IOLineageRepository(
    rdfConnectionResource: RDFConnectionResource[IO],
    gitLabBaseUrl:         GitLabBaseUrl
) extends LineageRepository[IO](rdfConnectionResource, gitLabBaseUrl)

private object IOLineageRepository {
  def apply(): IO[IOLineageRepository] =
    for {
      connectionResource <- IORDFConnectionResource()
      gitLabBaseUrl      <- GitLabBaseUrl[IO]()
    } yield new IOLineageRepository(connectionResource, gitLabBaseUrl)
}
