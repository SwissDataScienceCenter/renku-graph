/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.entities.searchgraphs.datasets.commands

import cats.effect.Async
import io.circe.DecodingFailure.Reason.CustomReason
import io.renku.entities.searchgraphs.datasets.{Link, links}
import io.renku.graph.model.datasets.TopmostSameAs
import io.renku.graph.model.{GraphClass, projects}
import io.renku.jsonld.syntax._
import io.renku.projectauth.ProjectAuth
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder, TSClientImpl}
import org.typelevel.log4cats.Logger

private trait TSSearchInfoFetcher[F[_]] {
  def findTSInfosByProject(projectId: projects.ResourceId): F[List[TSDatasetSearchInfo]]
}

private object TSSearchInfoFetcher {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      connectionConfig: ProjectsConnectionConfig
  ): TSSearchInfoFetcher[F] =
    new TSSearchInfoFetcherImpl[F](connectionConfig)
}

private class TSSearchInfoFetcherImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    storeConfig: ProjectsConnectionConfig
) extends TSClientImpl(storeConfig)
    with TSSearchInfoFetcher[F] {

  import cats.syntax.all._
  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.graph.model.Schemas._
  import io.renku.triplesstore.ResultsDecoder._
  import io.renku.triplesstore.SparqlQuery.Prefixes
  import io.renku.triplesstore._
  import io.renku.triplesstore.client.syntax._

  override def findTSInfosByProject(projectId: projects.ResourceId): F[List[TSDatasetSearchInfo]] =
    queryExpecting[List[TSDatasetSearchInfo]](query(projectId))

  private def query(resourceId: projects.ResourceId) = SparqlQuery.of(
    name = "ds search infos by project",
    Prefixes of (renku -> "renku", schema -> "schema"),
    sparql"""|SELECT DISTINCT ?topSameAs 
             |       (GROUP_CONCAT(DISTINCT ?link; separator=${rowsSeparator.asTripleObject}) AS ?links)
             |WHERE {
             |  {
             |    SELECT DISTINCT ?topSameAs
             |    WHERE {
             |      GRAPH ${GraphClass.Datasets.id} {
             |        ?linkId a renku:DatasetProjectLink;
             |                renku:project ${resourceId.asEntityId}.
             |        ?topSameAs renku:datasetProjectLink ?linkId
             |      }
             |    }
             |  } {
             |    GRAPH ${GraphClass.Datasets.id} {
             |      ?topSameAs renku:datasetProjectLink ?linkId.
             |      ?linkId renku:project ?linkProjectId;
             |              renku:dataset ?linkDatasetId.
             |      GRAPH ${ProjectAuth.graph} {
             |        ?linkProjectId renku:slug ?slug;
             |                       renku:visibility ?visibility.
             |      }
             |      BIND (CONCAT(STR(?linkId), STR(';;'), STR(?linkProjectId), STR(';;'), STR(?linkDatasetId), STR(';;'), STR(?slug), STR(';;'), STR(?visibility)) AS ?link)
             |    }
             |  }
             |}
             |GROUP BY ?topSameAs
             |ORDER BY ?name
             |""".stripMargin
  )

  private lazy val rowsSeparator = '\u0000'

  private implicit lazy val recordsDecoder: Decoder[List[TSDatasetSearchInfo]] =
    ResultsDecoder[List, TSDatasetSearchInfo] { implicit cur =>
      import Decoder._
      import io.circe.DecodingFailure
      import io.renku.graph.model.{datasets, projects}
      import io.renku.tinytypes.json.TinyTypeDecoders._

      val splitRows: String => List[String] = _.split(rowsSeparator).toList.distinct

      def decode[O](map: String => Decoder.Result[O], sort: List[O] => List[O]): String => Decoder.Result[List[O]] =
        splitRows(_).map(map).sequence.map(_.distinct).map(sort)

      val toLink: String => Decoder.Result[Link] = {
        case s"$id;;$projectId;;$datasetId;;$slug;;$visibility" =>
          (links.ResourceId.from(id).leftMap(ex => DecodingFailure(CustomReason(ex.getMessage), cur)),
           datasets.ResourceId.from(datasetId).leftMap(ex => DecodingFailure(CustomReason(ex.getMessage), cur)),
           projects.ResourceId.from(projectId).leftMap(ex => DecodingFailure(CustomReason(ex.getMessage), cur)),
           projects.Slug.from(slug).leftMap(ex => DecodingFailure(CustomReason(ex.getMessage), cur)),
           projects.Visibility.from(visibility).leftMap(ex => DecodingFailure(CustomReason(ex.getMessage), cur))
          ).mapN(Link.apply)
        case other =>
          DecodingFailure(CustomReason(s"'$other' not a valid link record"), cur).asLeft[Link]
      }

      val toListOfLinks: String => Decoder.Result[List[Link]] =
        decode[Link](map = toLink, sort = _.sortBy(_.projectId))(_)

      (
        extract[datasets.TopmostSameAs]("topSameAs"),
        extract[String]("links") >>= toListOfLinks
      ).mapN(TSDatasetSearchInfo.apply)
    }
}
