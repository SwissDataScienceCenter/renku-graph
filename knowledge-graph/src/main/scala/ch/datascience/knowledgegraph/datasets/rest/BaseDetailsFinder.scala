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

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.graph.config.RenkuBaseUrl
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
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO]
)(implicit executionContext: ExecutionContext,
  contextShift:              ContextShift[IO],
  timer:                     Timer[IO],
  ME:                        MonadError[IO, Throwable])
    extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder) {

  import BaseDetailsFinder._

  def findBaseDetails(identifier: Identifier): IO[Option[Dataset]] =
    queryExpecting[List[Dataset]](using = query(identifier)) flatMap toSingleDataset

  private def query(identifier: Identifier) = SparqlQuery(
    name = "base dataset details",
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
      "PREFIX schema: <http://schema.org/>"
    ),
    s"""|SELECT DISTINCT ?identifier ?name ?url ?sameAs ?description ?publishedDate
        |WHERE {
        |  ?dataset schema:identifier "$identifier" ;
        |           schema:identifier ?identifier ;
        |           rdf:type <http://schema.org/Dataset> ;
        |           schema:name ?name .
        |  OPTIONAL { ?dataset schema:url ?url } .
        |  OPTIONAL { ?dataset schema:sameAs/schema:url ?sameAs } .
        |  OPTIONAL { ?dataset schema:description ?description } .
        |  OPTIONAL { ?dataset schema:datePublished ?publishedDate } .
        |}""".stripMargin
  )

  private lazy val toSingleDataset: List[Dataset] => IO[Option[Dataset]] = {
    case Nil            => ME.pure(None)
    case dataset +: Nil => ME.pure(Some(dataset))
    case datasets       => ME.raiseError(new RuntimeException(s"More than one dataset with ${datasets.head.id} id"))
  }
}

private object BaseDetailsFinder {
  import io.circe.Decoder

  private[rest] implicit val maybeRecordDecoder: Decoder[List[Dataset]] = {
    import Decoder._
    import ch.datascience.graph.model.datasets._
    import ch.datascience.knowledgegraph.datasets.model._
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    def extract[T](property: String, from: HCursor)(implicit decoder: Decoder[T]): Result[T] =
      from.downField(property).downField("value").as[T]

    val dataset: Decoder[Dataset] = { cursor =>
      for {
        id                 <- extract[Identifier]("identifier", from = cursor)
        name               <- extract[Name]("name", from = cursor)
        maybeUrl           <- extract[Option[String]]("url", from = cursor).map(blankToNone).flatMap(toOption[Url])
        maybeSameAs        <- extract[Option[String]]("sameAs", from = cursor).map(blankToNone).flatMap(toOption[SameAs])
        maybePublishedDate <- extract[Option[PublishedDate]]("publishedDate", from = cursor)
        maybeDescription <- extract[Option[String]]("description", from = cursor)
                             .map(blankToNone)
                             .flatMap(toOption[Description])
      } yield Dataset(
        id,
        name,
        maybeUrl,
        maybeSameAs,
        maybeDescription,
        DatasetPublishing(maybePublishedDate, Set.empty),
        parts    = List.empty,
        projects = List.empty
      )
    }

    _.downField("results").downField("bindings").as(decodeList(dataset))
  }
}
