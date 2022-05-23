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

package io.renku.knowledgegraph.datasets.rest

import cats.MonadThrow
import cats.effect.kernel.Async
import io.renku.graph.model.datasets.{DerivedFrom, Identifier, ImageUri, Name, OriginalIdentifier, SameAs, Title}
import io.renku.graph.model.projects.Path
import io.renku.knowledgegraph.datasets.rest.ProjectDatasetsFinder.{ProjectDataset, SameAsOrDerived}
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

private trait ProjectDatasetsFinder[F[_]] {
  def findProjectDatasets(projectPath: Path): F[List[ProjectDataset]]
}

private object ProjectDatasetsFinder {
  type SameAsOrDerived = Either[SameAs, DerivedFrom]
  type ProjectDataset  = (Identifier, OriginalIdentifier, Title, Name, SameAsOrDerived, List[ImageUri])

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](rdfStoreConfig: RdfStoreConfig) =
    MonadThrow[F].catchNonFatal(
      new ProjectDatasetsFinderImpl[F](rdfStoreConfig)
    )
}

private class ProjectDatasetsFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    rdfStoreConfig: RdfStoreConfig
) extends RdfStoreClientImpl(rdfStoreConfig)
    with ProjectDatasetsFinder[F] {

  import ProjectDatasetsFinderImpl._
  import eu.timepit.refined.auto._
  import io.renku.graph.model.Schemas._

  def findProjectDatasets(projectPath: Path): F[List[ProjectDataset]] =
    queryExpecting[List[ProjectDataset]](using = query(projectPath))

  private def query(path: Path) = SparqlQuery.of(
    name = "ds projects",
    Prefixes of (renku -> "renku", schema -> "schema", prov -> "prov"),
    s"""|SELECT ?identifier ?name ?slug ?topmostSameAs ?maybeDerivedFrom ?originalId
        | (GROUP_CONCAT(?encodedImageUrl; separator=',') AS ?images)
        |WHERE {
        |   ?projectId a schema:Project;
        |              renku:projectPath '$path';
        |              renku:hasDataset ?datasetId.
        |    ?datasetId a schema:Dataset;
        |               schema:identifier ?identifier;
        |               schema:name ?name;
        |               renku:slug ?slug;
        |               renku:topmostSameAs ?topmostSameAs;
        |               renku:topmostDerivedFrom/schema:identifier ?originalId.
        |    OPTIONAL { ?datasetId prov:wasDerivedFrom/schema:url ?maybeDerivedFrom }.
        |    FILTER NOT EXISTS { ?otherDsId prov:wasDerivedFrom/schema:url ?datasetId }
        |    FILTER NOT EXISTS { ?datasetId prov:invalidatedAtTime ?invalidationTime. }
        |    OPTIONAL { 
        |      ?imageId schema:position ?imagePosition ;
        |               schema:contentUrl ?imageUrl ;
        |               ^schema:image ?datasetId .
        |      BIND(CONCAT(STR(?imagePosition), STR(':'), STR(?imageUrl)) AS ?encodedImageUrl)
        |    }
        |}
        |GROUP BY ?identifier ?name ?slug ?topmostSameAs ?maybeDerivedFrom ?originalId
        |ORDER BY ?name
        |""".stripMargin
  )
}

private object ProjectDatasetsFinderImpl {

  import ResultsDecoder._
  import io.circe.Decoder

  private implicit val recordsDecoder: Decoder[List[ProjectDataset]] = ResultsDecoder[List, ProjectDataset] {
    implicit cur =>
      import io.renku.tinytypes.json.TinyTypeDecoders._

      def sameAsOrDerived(from: SameAs, and: Option[DerivedFrom]): SameAsOrDerived = from -> and match {
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
      } yield (id, originalId, title, name, sameAsOrDerived(from = sameAs, and = maybeDerivedFrom), images)
  }
}
