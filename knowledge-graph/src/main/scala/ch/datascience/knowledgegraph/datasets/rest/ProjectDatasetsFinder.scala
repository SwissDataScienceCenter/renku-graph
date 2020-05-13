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
import ch.datascience.graph.model.datasets.{DerivedFrom, Identifier, Name, SameAs}
import ch.datascience.graph.model.projects.{Path, ResourceId}
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.rdfstore._
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait ProjectDatasetsFinder[Interpretation[_]] {
  def findProjectDatasets(projectPath: Path): Interpretation[List[(Identifier, Name, SameAsOrDerived)]]
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

  def findProjectDatasets(projectPath: Path): IO[List[(Identifier, Name, SameAsOrDerived)]] =
    queryExpecting[List[(Identifier, Name, SameAsOrDerived)]](using = query(projectPath))

  private def query(path: Path) = SparqlQuery(
    name = "ds projects",
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX schema: <http://schema.org/>",
      "PREFIX prov: <http://www.w3.org/ns/prov#>"
    ),
    s"""|SELECT DISTINCT ?identifier ?name (?topmostSameAs AS ?sameAs) ?maybeDerivedFrom
        |WHERE {
        |  {
        |    ?datasetId rdf:type <http://schema.org/Dataset>;
        |               schema:isPartOf ${ResourceId(renkuBaseUrl, path).showAs[RdfResource]};
        |               schema:identifier ?identifier;
        |               schema:name ?name.
        |    OPTIONAL { ?datasetId prov:wasDerivedFrom ?maybeDerivedFrom }.
        |    FILTER NOT EXISTS { ?otherDsId prov:wasDerivedFrom ?datasetId } 
        |  } {
        |    SELECT ?datasetId ?topmostSameAs
        |    WHERE {
        |      {
        |        {
        |          ?datasetId schema:sameAs+/schema:url ?l1.
        |          FILTER NOT EXISTS { ?l1 schema:sameAs ?l2 }
        |          BIND (?l1 AS ?topmostSameAs)
        |        } UNION {
        |          ?datasetId rdf:type <http://schema.org/Dataset>.
        |          FILTER NOT EXISTS { ?datasetId schema:sameAs ?l1 }
        |          BIND (?datasetId AS ?topmostSameAs)
        |        }
        |      } UNION {
        |        ?datasetId schema:sameAs+/schema:url ?l1.
        |        ?l1 schema:sameAs+/schema:url ?l2
        |        FILTER NOT EXISTS { ?l2 schema:sameAs ?l3 }
        |        BIND (?l2 AS ?topmostSameAs)
        |      } UNION {
        |        ?datasetId schema:sameAs+/schema:url ?l1.
        |        ?l1 schema:sameAs+/schema:url ?l2.
        |        ?l2 schema:sameAs+/schema:url ?l3
        |        FILTER NOT EXISTS { ?l3 schema:sameAs ?l4 }
        |        BIND (?l3 AS ?topmostSameAs)
        |      }
        |    }
        |    GROUP BY ?datasetId ?topmostSameAs
        |    HAVING (COUNT(*) > 0)
        |  }
        |}
        |""".stripMargin
  )
}

private object IOProjectDatasetsFinder {
  import io.circe.Decoder
  import io.circe.Decoder.decodeList

  private implicit val recordsDecoder: Decoder[List[(Identifier, Name, SameAsOrDerived)]] = {
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    def sameAsOrDerived(from: SameAs, and: Option[DerivedFrom]): SameAsOrDerived = from -> and match {
      case (_, Some(derivedFrom)) => Right(derivedFrom)
      case (sameAs, _)            => Left(sameAs)
    }

    implicit val recordDecoder: Decoder[(Identifier, Name, SameAsOrDerived)] = { cursor =>
      for {
        id               <- cursor.downField("identifier").downField("value").as[Identifier]
        name             <- cursor.downField("name").downField("value").as[Name]
        sameAs           <- cursor.downField("sameAs").downField("value").as[SameAs]
        maybeDerivedFrom <- cursor.downField("maybeDerivedFrom").downField("value").as[Option[DerivedFrom]]
      } yield (id, name, sameAsOrDerived(from = sameAs, and = maybeDerivedFrom))
    }

    _.downField("results").downField("bindings").as(decodeList[(Identifier, Name, SameAsOrDerived)])
  }
}
