/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.lineage

import cats.effect._
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.config.RenkuBaseUrlLoader
import io.renku.graph.model.RenkuBaseUrl
import io.renku.graph.model.Schemas._
import io.renku.graph.model.projects.{Path, ResourceId, Visibility}
import io.renku.graph.model.views.RdfResource
import io.renku.http.server.security.model.AuthUser
import io.renku.jsonld.EntityId
import io.renku.knowledgegraph.lineage.model._
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

private trait EdgesFinder[F[_]] {
  def findEdges(projectPath: Path, maybeUser: Option[AuthUser]): F[EdgeMap]
}

private class EdgesFinderImpl[F[_]: Async: Logger](
    rdfStoreConfig: RdfStoreConfig,
    renkuBaseUrl:   RenkuBaseUrl,
    timeRecorder:   SparqlQueryTimeRecorder[F]
) extends RdfStoreClientImpl(rdfStoreConfig, timeRecorder)
    with EdgesFinder[F] {

  private type EdgeData = (ExecutionInfo, Option[Node.Location], Option[Node.Location])

  override def findEdges(projectPath: Path, maybeUser: Option[AuthUser]): F[EdgeMap] =
    queryEdges(using = query(projectPath, maybeUser)) map toNodesLocations

  private def queryEdges(using: SparqlQuery): F[Set[EdgeData]] = {
    val pageSize = 2000

    def fetchPaginatedResult(into: Set[EdgeData], using: SparqlQuery, offset: Int): F[Set[EdgeData]] = {
      val queryWithOffset = using.copy(body = using.body + s"\nLIMIT $pageSize \nOFFSET $offset")
      for {
        edges <- queryExpecting[Set[EdgeData]](queryWithOffset)
        results <- edges match {
                     case e if e.size < pageSize => (into ++ edges).pure[F]
                     case fullPage               => fetchPaginatedResult(into ++ fullPage, using, offset + pageSize)
                   }
      } yield results
    }

    fetchPaginatedResult(Set.empty[EdgeData], using, offset = 0)
  }

  private def query(path: Path, maybeUser: Option[AuthUser]) = SparqlQuery.of(
    name = "lineage - edges",
    Prefixes.of(prov -> "prov", renku -> "renku", schema -> "schema"),
    s"""|SELECT DISTINCT ?activity ?date ?sourceEntityLocation ?targetEntityLocation
        |WHERE {
        |   ${projectMemberFilterQuery(ResourceId(path)(renkuBaseUrl).showAs[RdfResource])(maybeUser)}
        |   ?activity a prov:Activity;
        |             ^renku:hasActivity ${ResourceId(path)(renkuBaseUrl).showAs[RdfResource]};
        |             prov:startedAtTime ?date;
        |             renku:parameter ?paramValue.
        |   ?paramValue schema:valueReference ?parameter.
        |   OPTIONAL {
        | 	  ?parameter a renku:CommandInput.
        |     ?paramValue schema:value ?sourceEntityLocation.
        |   }
        |   OPTIONAL {
        | 	  ?parameter a renku:CommandOutput.
        |     ?paramValue schema:value ?targetEntityLocation.
        |   }
        |}
        |ORDER BY ASC(?date)
        |""".stripMargin
  )

  import io.circe.Decoder

  private implicit val edgesDecoder: Decoder[Set[EdgeData]] = {
    import io.renku.tinytypes.json.TinyTypeDecoders._

    implicit val locationDecoder: Decoder[Node.Location] = stringDecoder(Node.Location)

    implicit lazy val edgeDecoder: Decoder[EdgeData] = { cursor =>
      for {
        activityId     <- cursor.downField("activity").downField("value").as[EntityId]
        date           <- cursor.downField("date").downField("value").as[RunDate]
        sourceLocation <- cursor.downField("sourceEntityLocation").downField("value").as[Option[Node.Location]]
        targetLocation <- cursor.downField("targetEntityLocation").downField("value").as[Option[Node.Location]]
      } yield (ExecutionInfo(activityId, date), sourceLocation, targetLocation)
    }

    _.downField("results").downField("bindings").as[List[EdgeData]].map(_.toSet)
  }

  private def toNodesLocations(edges: Set[EdgeData]): EdgeMap =
    edges.foldLeft(Map.empty[ExecutionInfo, FromAndToNodes]) {
      case (edgesMap, (runInfo, Some(source), None)) =>
        edgesMap.find(matching(runInfo)) match {
          case None =>
            edgesMap + (runInfo -> (Set(source), Set.empty))
          case Some((ExecutionInfo(_, date), (from, to))) if runInfo.date == date =>
            edgesMap + (runInfo -> (from + source, to))
          case Some((oldInfo, _)) if (runInfo.date compareTo oldInfo.date) > 0 =>
            edgesMap - oldInfo + (runInfo -> (Set(source), Set.empty))
          case _ => edgesMap
        }
      case (edgesMap, (runInfo, None, Some(target))) =>
        edgesMap.find(matching(runInfo)) match {
          case None =>
            edgesMap + (runInfo -> (Set.empty, Set(target)))
          case Some((ExecutionInfo(_, date), (from, to))) if runInfo.date == date =>
            edgesMap + (runInfo -> (from, to + target))
          case Some((oldInfo, _)) if (runInfo.date compareTo oldInfo.date) > 0 =>
            edgesMap - oldInfo + (runInfo -> (Set.empty, Set(target)))
          case _ => edgesMap
        }
      case (edgesMap, _) => edgesMap
    }

  private def matching(runInfo: ExecutionInfo): ((ExecutionInfo, FromAndToNodes)) => Boolean = {
    case (ExecutionInfo(runInfo.entityId, _), _) => true
    case _                                       => false
  }

  private def projectMemberFilterQuery(projectResourceId: String): Option[AuthUser] => String = {
    case Some(user) =>
      s"""|$projectResourceId renku:projectVisibility ?visibility .
          |OPTIONAL {
          |  $projectResourceId schema:member/schema:sameAs ?memberId.
          |  ?memberId  schema:additionalType 'GitLab';
          |             schema:identifier ?userGitlabId .
          |}
          |FILTER ( ?visibility != '${Visibility.Private.value}' || ?userGitlabId = ${user.id.value} )
          |""".stripMargin
    case _ =>
      s"""|$projectResourceId renku:projectVisibility ?visibility .
          |FILTER(?visibility = '${Visibility.Public.value}')
          |""".stripMargin
  }
}

private object EdgesFinder {

  def apply[F[_]: Async: Logger](timeRecorder: SparqlQueryTimeRecorder[F]): F[EdgesFinder[F]] = for {
    config       <- RdfStoreConfig[F]()
    renkuBaseUrl <- RenkuBaseUrlLoader[F]()
  } yield new EdgesFinderImpl[F](config, renkuBaseUrl, timeRecorder)
}
