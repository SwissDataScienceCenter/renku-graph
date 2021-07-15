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
import ch.datascience.graph.Schemas._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.projects.{Path, ResourceId, Visibility}
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.http.server.security.model.AuthUser
import ch.datascience.knowledgegraph.lineage.model._
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import ch.datascience.rdfstore._
import eu.timepit.refined.auto._
import io.renku.jsonld.EntityId
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private trait EdgesFinder[Interpretation[_]] {
  def findEdges(projectPath: Path, maybeUser: Option[AuthUser]): Interpretation[EdgeMap]
}

private class EdgesFinderImpl(
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends RdfStoreClientImpl(rdfStoreConfig, logger, timeRecorder)
    with EdgesFinder[IO] {

  private type EdgeData = (RunInfo, Option[Node.Location], Option[Node.Location])

  override def findEdges(projectPath: Path, maybeUser: Option[AuthUser]): IO[EdgeMap] =
    queryEdges(using = query(projectPath, maybeUser: Option[AuthUser])) map toNodesLocations

  private def queryEdges(using: SparqlQuery): IO[Set[EdgeData]] = {
    val pageSize = 2000

    def fetchPaginatedResult(into: Set[EdgeData], using: SparqlQuery, offset: Int): IO[Set[EdgeData]] = {
      val queryWithOffset = using.copy(body = using.body + s"\nLIMIT $pageSize \nOFFSET $offset")
      for {
        edges <- queryExpecting[Set[EdgeData]](queryWithOffset)
        results <- edges match {
                     case e if e.size < pageSize => (into ++ edges).pure[IO]
                     case fullPage               => fetchPaginatedResult(into ++ fullPage, using, offset + pageSize)
                   }
      } yield results
    }

    fetchPaginatedResult(Set.empty[EdgeData], using, offset = 0)
  }

  private def query(path: Path, maybeUser: Option[AuthUser]) = SparqlQuery.of(
    name = "lineage - edges",
    Prefixes.of(prov -> "prov", renku -> "renku", wfprov -> "wfprov", schema -> "schema"),
    s"""SELECT DISTINCT ?runPlan ?date ?sourceEntityLocation ?targetEntityLocation
       |WHERE {
       |  {
       |  ${projectMemberFilterQuery(ResourceId(renkuBaseUrl, path).showAs[RdfResource])(maybeUser)}
       |    ?runPlan schema:isPartOf ${ResourceId(renkuBaseUrl, path).showAs[RdfResource]};
       |             renku:hasInputs/renku:consumes/prov:atLocation ?sourceEntityLocation;
       |             ^(prov:qualifiedAssociation/prov:hadPlan) ?activity.
       |    ?activity prov:startedAtTime ?date.
       |    FILTER NOT EXISTS {?activity a wfprov:WorkflowRun}
       |  } UNION {
       |  ${projectMemberFilterQuery(ResourceId(renkuBaseUrl, path).showAs[RdfResource])(maybeUser)}
       |    ?runPlan schema:isPartOf ${ResourceId(renkuBaseUrl, path).showAs[RdfResource]};
       |             renku:hasOutputs/renku:produces/prov:atLocation ?targetEntityLocation;
       |             ^(prov:qualifiedAssociation/prov:hadPlan) ?activity.
       |    ?activity prov:startedAtTime ?date.
       |    FILTER NOT EXISTS {?activity a wfprov:WorkflowRun}
       |  }
       |}
       |ORDER BY ASC(?date)""".stripMargin
  )

  import io.circe.Decoder

  private implicit val edgesDecoder: Decoder[Set[EdgeData]] = {
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    implicit val locationDecoder: Decoder[Node.Location] = stringDecoder(Node.Location)

    implicit lazy val edgeDecoder: Decoder[EdgeData] = { cursor =>
      for {
        runPlanId      <- cursor.downField("runPlan").downField("value").as[EntityId]
        date           <- cursor.downField("date").downField("value").as[RunDate]
        sourceLocation <- cursor.downField("sourceEntityLocation").downField("value").as[Option[Node.Location]]
        targetLocation <- cursor.downField("targetEntityLocation").downField("value").as[Option[Node.Location]]
      } yield (RunInfo(runPlanId, date), sourceLocation, targetLocation)
    }

    _.downField("results").downField("bindings").as[List[EdgeData]].map(_.toSet)
  }

  private def toNodesLocations(edges: Set[EdgeData]): EdgeMap =
    edges.foldLeft(Map.empty[RunInfo, FromAndToNodes]) {
      case (edgesMap, (runInfo, Some(source), None)) =>
        edgesMap.find(matching(runInfo)) match {
          case None =>
            edgesMap + (runInfo -> (Set(source), Set.empty))
          case Some((RunInfo(_, date), (from, to))) if runInfo.date == date =>
            edgesMap + (runInfo -> (from + source, to))
          case Some((oldInfo, _)) if (runInfo.date compareTo oldInfo.date) > 0 =>
            edgesMap - oldInfo + (runInfo -> (Set(source), Set.empty))
          case _ => edgesMap
        }
      case (edgesMap, (runInfo, None, Some(target))) =>
        edgesMap.find(matching(runInfo)) match {
          case None =>
            edgesMap + (runInfo -> (Set.empty, Set(target)))
          case Some((RunInfo(_, date), (from, to))) if runInfo.date == date =>
            edgesMap + (runInfo -> (from, to + target))
          case Some((oldInfo, _)) if (runInfo.date compareTo oldInfo.date) > 0 =>
            edgesMap - oldInfo + (runInfo -> (Set.empty, Set(target)))
          case _ => edgesMap
        }
      case (edgesMap, _) => edgesMap
    }

  private def matching(runInfo: RunInfo): ((RunInfo, FromAndToNodes)) => Boolean = {
    case (RunInfo(runInfo.entityId, _), _) => true
    case _                                 => false
  }

  private def projectMemberFilterQuery(projectResourceId: String): Option[AuthUser] => String = {
    case Some(user) =>
      s"""
         |OPTIONAL { 
         |    $projectResourceId renku:projectVisibility ?visibility .
         |}
         |OPTIONAL {
         |    $projectResourceId schema:member/schema:sameAs ?memberId.
         |    ?memberId  schema:additionalType 'GitLab';
         |               schema:identifier ?userGitlabId .
         |}
         |
         |BIND (IF (BOUND (?visibility), ?visibility,  '${Visibility.Public.value}') as ?projectVisibility)
         |FILTER (
         |  lcase(str(?projectVisibility)) = '${Visibility.Public.value}' || 
         |  (lcase(str(?projectVisibility)) != '${Visibility.Public.value}'  && ?userGitlabId = ${user.id.value})
         |)
         |""".stripMargin
    case _ =>
      s"""
         |OPTIONAL {
         |  $projectResourceId renku:projectVisibility ?visibility .
         |}
         |BIND(IF (BOUND (?visibility), ?visibility, '${Visibility.Public.value}' ) as ?projectVisibility)
         |FILTER(lcase(str(?projectVisibility)) = '${Visibility.Public.value}')
         |""".stripMargin
  }
}

private object EdgesFinder {

  def apply(timeRecorder: SparqlQueryTimeRecorder[IO], logger: Logger[IO])(implicit
      executionContext:   ExecutionContext,
      contextShift:       ContextShift[IO],
      timer:              Timer[IO]
  ): IO[EdgesFinder[IO]] = for {
    config       <- RdfStoreConfig[IO]()
    renkuBaseUrl <- RenkuBaseUrl[IO]()
  } yield new EdgesFinderImpl(config, renkuBaseUrl, logger, timeRecorder)
}
