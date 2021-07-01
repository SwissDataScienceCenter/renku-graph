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

package ch.datascience.knowledgegraph.datasets.rest

import cats.effect.{ConcurrentEffect, IO, Timer}
import ch.datascience.graph.model.RenkuBaseUrl
import ch.datascience.graph.model.datasets.{DerivedFrom, Identifier, ImageUri, InitialVersion, Name, SameAs, Title}
import ch.datascience.graph.model.projects.{Path, ResourceId}
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.knowledgegraph.datasets.rest.ProjectDatasetsFinder.{ProjectDataset, SameAsOrDerived}
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import ch.datascience.rdfstore._
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private trait ProjectDatasetsFinder[Interpretation[_]] {
  def findProjectDatasets(projectPath: Path): Interpretation[List[ProjectDataset]]
}

private object ProjectDatasetsFinder {
  type SameAsOrDerived = Either[SameAs, DerivedFrom]
  type ProjectDataset  = (Identifier, InitialVersion, Title, Name, SameAsOrDerived, List[ImageUri])

  def apply(rdfStoreConfig:    RdfStoreConfig,
            renkuBaseUrl:      RenkuBaseUrl,
            logger:            Logger[IO],
            timeRecorder:      SparqlQueryTimeRecorder[IO]
  )(implicit executionContext: ExecutionContext, concurrentEffect: ConcurrentEffect[IO], timer: Timer[IO]) = IO(
    new ProjectDatasetsFinderImpl[IO](rdfStoreConfig, renkuBaseUrl, logger, timeRecorder)
  )
}

private class ProjectDatasetsFinderImpl[Interpretation[_]: ConcurrentEffect: Timer](
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[Interpretation],
    timeRecorder:            SparqlQueryTimeRecorder[Interpretation]
)(implicit executionContext: ExecutionContext)
    extends RdfStoreClientImpl(rdfStoreConfig, logger, timeRecorder)
    with ProjectDatasetsFinder[Interpretation] {

  import ProjectDatasetsFinderImpl._
  import ch.datascience.graph.model.Schemas._
  import eu.timepit.refined.auto._

  def findProjectDatasets(projectPath: Path): Interpretation[List[ProjectDataset]] =
    queryExpecting[List[ProjectDataset]](using = query(projectPath))

  private def query(path: Path) = SparqlQuery.of(
    name = "ds projects",
    Prefixes.of(renku -> "renku", schema -> "schema", prov -> "prov"),
    s"""|SELECT ?identifier ?name ?alternateName ?topmostSameAs ?maybeDerivedFrom ?initialVersion (GROUP_CONCAT(?encodedImageUrl; separator=',') AS ?images)
        |WHERE {
        |    ?datasetId a schema:Dataset;
        |               schema:isPartOf ${ResourceId(renkuBaseUrl, path).showAs[RdfResource]};
        |               schema:identifier ?identifier;
        |               schema:name ?name;
        |               schema:alternateName  ?alternateName;
        |               renku:topmostSameAs ?topmostSameAs;
        |               renku:topmostDerivedFrom/schema:identifier ?initialVersion.
        |    OPTIONAL { ?datasetId prov:wasDerivedFrom/schema:url ?maybeDerivedFrom }.
        |    FILTER NOT EXISTS { ?otherDsId prov:wasDerivedFrom/schema:url ?datasetId }
        |    FILTER NOT EXISTS { ?datasetId prov:invalidatedAtTime ?invalidationTime. }
        |    OPTIONAL { 
        |      ?datasetId   schema:image ?imageId .
        |      ?imageId     schema:position ?imagePosition ;
        |                   schema:contentUrl ?imageUrl .
        |      BIND(CONCAT(STR(?imagePosition), STR(':'), STR(?imageUrl)) AS ?encodedImageUrl)
        |    }
        |}
        |
        |GROUP BY ?identifier ?name ?alternateName ?topmostSameAs ?maybeDerivedFrom ?initialVersion 
        |ORDER BY ?name
        |""".stripMargin
  )
}

private object ProjectDatasetsFinderImpl {

  import io.circe.Decoder
  import io.circe.Decoder.decodeList

  private implicit val recordsDecoder: Decoder[List[ProjectDataset]] = {
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

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

    implicit val recordDecoder: Decoder[ProjectDataset] = { cursor =>
      for {
        id               <- cursor.downField("identifier").downField("value").as[Identifier]
        title            <- cursor.downField("name").downField("value").as[Title]
        name             <- cursor.downField("alternateName").downField("value").as[Name]
        sameAs           <- cursor.downField("topmostSameAs").downField("value").as[SameAs]
        maybeDerivedFrom <- cursor.downField("maybeDerivedFrom").downField("value").as[Option[DerivedFrom]]
        initialVersion   <- cursor.downField("initialVersion").downField("value").as[InitialVersion]
        images           <- cursor.downField("images").downField("value").as[Option[String]].map(toListOfImageUrls)
      } yield (id, initialVersion, title, name, sameAsOrDerived(from = sameAs, and = maybeDerivedFrom), images)
    }

    _.downField("results").downField("bindings").as(decodeList[ProjectDataset])
  }
}
