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

package io.renku.knowledgegraph.projects.datasets

import Endpoint.Criteria
import cats.NonEmptyParallel
import cats.effect.kernel.Async
import io.renku.graph.model.datasets.{DerivedFrom, Identifier, Name, OriginalIdentifier, SameAs, Title}
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.{RenkuUrl, projects}
import io.renku.http.rest.paging.Paging.PagedResultsFinder
import io.renku.http.rest.paging.{Paging, PagingResponse}
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger

private trait ProjectDatasetsFinder[F[_]] {
  def findProjectDatasets(criteria: Criteria): F[PagingResponse[ProjectDataset]]
}

private object ProjectDatasetsFinder {

  def apply[F[_]: Async: NonEmptyParallel: Logger: SparqlQueryTimeRecorder](conCfg: ProjectsConnectionConfig)(implicit
      renkuUrl: RenkuUrl
  ): ProjectDatasetsFinder[F] = new ProjectDatasetsFinderImpl[F](TSClient[F](conCfg))
}

private class ProjectDatasetsFinderImpl[F[_]: Async: NonEmptyParallel: Logger: SparqlQueryTimeRecorder](
    tsClient: TSClient[F]
)(implicit renkuUrl: RenkuUrl)
    extends ProjectDatasetsFinder[F]
    with Paging[ProjectDataset] {

  import ResultsDecoder._
  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.graph.model.Schemas._
  import io.renku.jsonld.syntax._
  import io.renku.triplesstore.client.syntax._

  def findProjectDatasets(criteria: Criteria): F[PagingResponse[ProjectDataset]] = {
    implicit val resultsFinder: PagedResultsFinder[F, ProjectDataset] =
      tsClient.pagedResultsFinder(query(criteria))
    findPage[F](criteria.paging)
  }

  private def query(criteria: Criteria) = SparqlQuery.of(
    name = "ds projects",
    Prefixes of (renku -> "renku", schema -> "schema", prov -> "prov"),
    sparql"""|SELECT ?identifier ?name ?slug ?topmostSameAs ?maybeDerivedFrom ?originalId
             | (GROUP_CONCAT(?encodedImageUrl; separator=',') AS ?images)
             |WHERE {
             |   BIND (${projects.ResourceId(criteria.projectPath).asEntityId} AS ?projectId)
             |   Graph ?projectId {
             |     ?projectId renku:hasDataset ?datasetId.
             |     ?datasetId a schema:Dataset;
             |               schema:identifier ?identifier;
             |               schema:name ?name;
             |               renku:slug ?slug;
             |               renku:topmostSameAs ?topmostSameAs;
             |               renku:topmostDerivedFrom/schema:identifier ?originalId.
             |     OPTIONAL { ?datasetId prov:wasDerivedFrom/schema:url ?maybeDerivedFrom }.
             |     FILTER NOT EXISTS { ?otherDsId prov:wasDerivedFrom/schema:url ?datasetId }
             |     FILTER NOT EXISTS { ?datasetId prov:invalidatedAtTime ?invalidationTime. }
             |     OPTIONAL {
             |       ?imageId schema:position ?imagePosition ;
             |                schema:contentUrl ?imageUrl ;
             |                ^schema:image ?datasetId .
             |       BIND(CONCAT(STR(?imagePosition), STR(':'), STR(?imageUrl)) AS ?encodedImageUrl)
             |     }
             |   }
             |}
             |GROUP BY ?identifier ?name ?slug ?topmostSameAs ?maybeDerivedFrom ?originalId
             |ORDER BY ASC(?name)
             |""".stripMargin
  )

  private implicit val recordDecoder: Decoder[ProjectDataset] = { implicit cur =>
    import io.renku.tinytypes.json.TinyTypeDecoders._

    def sameAsOrDerived(from: SameAs, and: Option[DerivedFrom]): ProjectDataset.SameAsOrDerived = from -> and match {
      case (_, Some(derivedFrom)) => Right(derivedFrom)
      case (sameAs, _)            => Left(sameAs)
    }

    def toListOfImageUrls(urlString: Option[String]): List[ImageUri] =
      urlString
        .map(
          _.split(",")
            .map(_.trim)
            .map { case s"$position:$url" => position.toIntOption.getOrElse(0) -> ImageUri(url) }
            .toSet[(Int, ImageUri)]
            .toList
            .sortBy(_._1)
            .map(_._2)
        )
        .getOrElse(Nil)

    for {
      id               <- extract[Identifier]("identifier")
      title            <- extract[Title]("name")
      name             <- extract[Name]("slug")
      sameAs           <- extract[SameAs]("topmostSameAs")
      maybeDerivedFrom <- extract[Option[DerivedFrom]]("maybeDerivedFrom")
      originalId       <- extract[OriginalIdentifier]("originalId")
      images           <- extract[Option[String]]("images").map(toListOfImageUrls)
    } yield ProjectDataset(id, originalId, title, name, sameAsOrDerived(from = sameAs, and = maybeDerivedFrom), images)
  }
}
