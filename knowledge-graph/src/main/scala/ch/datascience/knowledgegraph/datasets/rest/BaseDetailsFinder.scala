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

import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import ch.datascience.graph.model.datasets.Identifier
import ch.datascience.knowledgegraph.datasets.model.Dataset
import ch.datascience.rdfstore._
import eu.timepit.refined.auto._
import io.chrisdavenport.log4cats.Logger
import io.circe.HCursor

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private class BaseDetailsFinder(
    rdfStoreConfig:          RdfStoreConfig,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder) {

  import BaseDetailsFinder._

  def findBaseDetails(identifier: Identifier): IO[Option[Dataset]] =
    queryExpecting[List[Dataset]](using = queryForDatasetDetails(identifier)) flatMap toSingleDataset

  private def queryForDatasetDetails(identifier: Identifier) = SparqlQuery(
    name = "ds by id - details",
    Set(
      "PREFIX prov: <http://www.w3.org/ns/prov#>",
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>",
      "PREFIX schema: <http://schema.org/>"
    ),
    s"""|SELECT DISTINCT ?identifier ?name ?url ?topmostSameAs ?maybeDerivedFrom ?description ?publishedDate
        |WHERE {
        |    ?datasetId schema:identifier "$identifier";
        |               schema:identifier ?identifier;
        |               rdf:type <http://schema.org/Dataset>;
        |               schema:url ?url;
        |               schema:name ?name;
        |               renku:topmostSameAs/schema:url ?topmostSameAs .
        |    OPTIONAL { ?datasetId prov:wasDerivedFrom ?maybeDerivedFrom }.
        |    OPTIONAL { ?datasetId schema:description ?description }.
        |    OPTIONAL { ?datasetId schema:datePublished ?publishedDate }.
        |}
        |""".stripMargin
  )

  private lazy val toSingleDataset: List[Dataset] => IO[Option[Dataset]] = {
    case Nil            => Option.empty[Dataset].pure[IO]
    case dataset +: Nil => Option(dataset).pure[IO]
    case dataset +: _   => new Exception(s"More than one dataset with ${dataset.id} id").raiseError[IO, Option[Dataset]]
  }
}

private object BaseDetailsFinder {
  import io.circe.Decoder
  import Decoder._
  import ch.datascience.graph.model.datasets._
  import ch.datascience.knowledgegraph.datasets.model._
  import ch.datascience.tinytypes.json.TinyTypeDecoders._

  private[rest] implicit val datasetsDecoder: Decoder[List[Dataset]] = {
    val dataset: Decoder[Dataset] = { implicit cursor =>
      for {
        identifier         <- extract[Identifier]("identifier")
        name               <- extract[Name]("name")
        url                <- extract[Url]("url")
        maybeDerivedFrom   <- extract[Option[DerivedFrom]]("maybeDerivedFrom")
        sameAs             <- extract[SameAs]("topmostSameAs")
        maybePublishedDate <- extract[Option[PublishedDate]]("publishedDate")
        maybeDescription <- extract[Option[String]]("description")
                             .map(blankToNone)
                             .flatMap(toOption[Description])
      } yield maybeDerivedFrom match {
        case Some(derivedFrom) =>
          ModifiedDataset(
            identifier,
            name,
            url,
            derivedFrom,
            maybeDescription,
            DatasetPublishing(maybePublishedDate, Set.empty),
            parts    = List.empty,
            projects = List.empty
          )
        case None =>
          NonModifiedDataset(
            identifier,
            name,
            url,
            sameAs,
            maybeDescription,
            DatasetPublishing(maybePublishedDate, Set.empty),
            parts    = List.empty,
            projects = List.empty
          )
      }

    }

    _.downField("results").downField("bindings").as(decodeList(dataset))
  }

  private def extract[T](property: String)(implicit cursor: HCursor, decoder: Decoder[T]): Result[T] =
    cursor.downField(property).downField("value").as[T]
}
