/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import ProjectDatasetsFinder._
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.datasets.{DerivedFrom, Identifier, Name, SameAs, Title}
import ch.datascience.graph.model.projects.{Path, ResourceId}
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.rdfstore._
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait ProjectDatasetsFinder[Interpretation[_]] {
  def findProjectDatasets(projectPath: Path): Interpretation[List[(Identifier, Title, Name, SameAsOrDerived)]]
}

private object ProjectDatasetsFinder {
  type SameAsOrDerived = Either[SameAs, DerivedFrom]
}

private class IOProjectDatasetsFinder(
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder)
    with ProjectDatasetsFinder[IO] {

  import IOProjectDatasetsFinder._
  import eu.timepit.refined.auto._

  def findProjectDatasets(projectPath: Path): IO[List[(Identifier, Title, Name, SameAsOrDerived)]] =
    queryExpecting[List[(Identifier, Title, Name, SameAsOrDerived)]](using = query(projectPath))

  private def query(path: Path) = SparqlQuery(
    name = "ds projects",
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>",
      "PREFIX schema: <http://schema.org/>",
      "PREFIX prov: <http://www.w3.org/ns/prov#>"
    ),
    s"""|SELECT DISTINCT ?identifier ?name ?alternateName ?topmostSameAs ?maybeDerivedFrom
        |WHERE {
        |    ?datasetId rdf:type <http://schema.org/Dataset>;
        |               schema:isPartOf ${ResourceId(renkuBaseUrl, path).showAs[RdfResource]};
        |               schema:identifier ?identifier;
        |               schema:name ?name;
        |               schema:alternateName  ?alternateName;
        |               renku:topmostSameAs ?topmostSameAs .
        |    OPTIONAL { ?datasetId prov:wasDerivedFrom/schema:url ?maybeDerivedFrom }.
        |    FILTER NOT EXISTS { ?otherDsId prov:wasDerivedFrom/schema:url ?datasetId }
        |}
        |ORDER BY ?name
        |""".stripMargin
  )
}

private object IOProjectDatasetsFinder {

  import io.circe.Decoder
  import io.circe.Decoder.decodeList

  private implicit val recordsDecoder: Decoder[List[(Identifier, Title, Name, SameAsOrDerived)]] = {
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    def sameAsOrDerived(from: SameAs, and: Option[DerivedFrom]): SameAsOrDerived = from -> and match {
      case (_, Some(derivedFrom)) => Right(derivedFrom)
      case (sameAs, _)            => Left(sameAs)
    }

    implicit val recordDecoder: Decoder[(Identifier, Title, Name, SameAsOrDerived)] = { cursor =>
      for {
        id               <- cursor.downField("identifier").downField("value").as[Identifier]
        title            <- cursor.downField("name").downField("value").as[Title]
        name             <- cursor.downField("alternateName").downField("value").as[Name]
        sameAs           <- cursor.downField("topmostSameAs").downField("value").as[SameAs]
        maybeDerivedFrom <- cursor.downField("maybeDerivedFrom").downField("value").as[Option[DerivedFrom]]
      } yield (id, title, name, sameAsOrDerived(from = sameAs, and = maybeDerivedFrom))
    }

    _.downField("results").downField("bindings").as(decodeList[(Identifier, Title, Name, SameAsOrDerived)])
  }
}
