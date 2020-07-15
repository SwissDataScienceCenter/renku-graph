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
import ch.datascience.knowledgegraph.datasets.model.{Dataset, ModifiedDataset, NonModifiedDataset}
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
    queryExpecting[List[IdAndDerived]](using = queryFindingDerivedFrom(identifier)) flatMap toSingleTuple flatMap {
      case `no dataset found`() =>
        Option.empty[Dataset].pure[IO]
      case `not modified dataset`() =>
        queryExpecting[List[NonModifiedDataset]](using = queryForNonModified(identifier)) flatMap toSingleDataset
      case `modified dataset`() =>
        queryExpecting[List[ModifiedDataset]](using = queryForModified(identifier)) flatMap toSingleDataset
      case _ =>
        new Exception(s"Cannot find dataset with $identifier id").raiseError[IO, Option[Dataset]]
    }

  private def queryFindingDerivedFrom(identifier: Identifier) = SparqlQuery(
    name = "ds by id - is modified",
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX schema: <http://schema.org/>",
      "PREFIX prov: <http://www.w3.org/ns/prov#>"
    ),
    s"""|SELECT ?id ?maybeDerivedFrom
        |WHERE {
        |  ?dsId rdf:type <http://schema.org/Dataset>;
        |        schema:identifier "$identifier";
        |        schema:identifier ?id.
        |  OPTIONAL { ?dsId prov:wasDerivedFrom ?maybeDerivedFrom }.
        |}
        |""".stripMargin
  )

  private def queryForNonModified(identifier: Identifier) = SparqlQuery(
    name = "ds by id - non modified",
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX schema: <http://schema.org/>"
    ),
    s"""|SELECT DISTINCT ?datasetId ?identifier ?name ?url (?topmostSameAs AS ?sameAs) ?description ?publishedDate
        |WHERE {
        |  {
        |    SELECT ?topmostSameAs
        |    WHERE {
        |      {
        |        ?l0 rdf:type <http://schema.org/Dataset>;
        |            schema:identifier "$identifier"
        |      } {
        |        {
        |          {
        |            ?l0 schema:sameAs+/schema:url ?l1.
        |            FILTER NOT EXISTS { ?l1 schema:sameAs ?l2 }
        |            BIND (?l1 AS ?topmostSameAs)
        |          } UNION {
        |            ?l0 rdf:type <http://schema.org/Dataset>.
        |            FILTER NOT EXISTS { ?l0 schema:sameAs ?l1 }
        |            BIND (?l0 AS ?topmostSameAs)
        |          }
        |        } UNION {
        |          ?l0 schema:sameAs+/schema:url ?l1.
        |          ?l1 schema:sameAs+/schema:url ?l2
        |          FILTER NOT EXISTS { ?l2 schema:sameAs ?l3 }
        |          BIND (?l2 AS ?topmostSameAs)
        |        } UNION {
        |          ?l0 schema:sameAs+/schema:url ?l1.
        |          ?l1 schema:sameAs+/schema:url ?l2.
        |          ?l2 schema:sameAs+/schema:url ?l3
        |          FILTER NOT EXISTS { ?l3 schema:sameAs ?l4 }
        |          BIND (?l3 AS ?topmostSameAs)
        |        }
        |      }
        |    }
        |    GROUP BY ?topmostSameAs
        |    HAVING (COUNT(*) > 0)
        |  } {
        |    ?datasetId schema:identifier "$identifier";
        |               schema:identifier ?identifier;
        |               rdf:type <http://schema.org/Dataset>;
        |               schema:url ?url;
        |               schema:name ?name.
        |    OPTIONAL { ?datasetId schema:description ?description }.
        |    OPTIONAL { ?datasetId schema:datePublished ?publishedDate }.
        |  }
        |}
        |""".stripMargin
  )

  private def queryForModified(identifier: Identifier) = SparqlQuery(
    name = "ds by id - modified",
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX schema: <http://schema.org/>",
      "PREFIX prov: <http://www.w3.org/ns/prov#>"
    ),
    s"""|SELECT DISTINCT ?datasetId ?identifier ?name ?url ?derivedFrom ?description ?publishedDate
        |WHERE {
        |  ?datasetId schema:identifier "$identifier";
        |             schema:identifier ?identifier;
        |             rdf:type <http://schema.org/Dataset>;
        |             schema:url ?url;
        |             prov:wasDerivedFrom ?derivedFrom;
        |             schema:name ?name.
        |  OPTIONAL { ?datasetId schema:description ?description } .
        |  OPTIONAL { ?datasetId schema:datePublished ?publishedDate } .
        |}
        |""".stripMargin
  )

  private lazy val toSingleDataset: List[Dataset] => IO[Option[Dataset]] = {
    case Nil            => Option.empty[Dataset].pure[IO]
    case dataset +: Nil => Option(dataset).pure[IO]
    case dataset +: _   => new Exception(s"More than one dataset with ${dataset.id} id").raiseError[IO, Option[Dataset]]
  }

  private lazy val toSingleTuple: List[IdAndDerived] => IO[Option[IdAndDerived]] = {
    case Nil          => Option.empty[IdAndDerived].pure[IO]
    case tuple +: Nil => Option(tuple).pure[IO]
    case (id, _) +: _ => new Exception(s"More than one dataset with $id id").raiseError[IO, Option[IdAndDerived]]
  }

  private object `no dataset found` {
    def unapply(tuple: Option[IdAndDerived]): Boolean = tuple match {
      case None => true
      case _    => false
    }
  }

  private object `not modified dataset` {
    def unapply(tuple: Option[IdAndDerived]): Boolean = tuple match {
      case Some((_, None)) => true
      case _               => false
    }
  }

  private object `modified dataset` {
    def unapply(tuple: Option[IdAndDerived]): Boolean = tuple match {
      case Some((_, Some(_))) => true
      case _                  => false
    }
  }
}

private object BaseDetailsFinder {
  import io.circe.Decoder
  import Decoder._
  import ch.datascience.graph.model.datasets._
  import ch.datascience.knowledgegraph.datasets.model._
  import ch.datascience.tinytypes.json.TinyTypeDecoders._

  private[rest] type IdAndDerived = (Identifier, Option[DerivedFrom])
  private[rest] implicit val idsAndDerivedDecoder: Decoder[List[IdAndDerived]] = {
    val idAndDerived: Decoder[IdAndDerived] = { implicit cursor =>
      for {
        id               <- extract[Identifier]("id")
        maybeDerivedFrom <- extract[Option[DerivedFrom]]("maybeDerivedFrom")
      } yield id -> maybeDerivedFrom
    }
    _.downField("results").downField("bindings").as(decodeList(idAndDerived))
  }

  private[rest] implicit val nonModifiedDecoder: Decoder[List[NonModifiedDataset]] = {

    val dataset: Decoder[NonModifiedDataset] = { implicit cursor =>
      for {
        id                 <- extract[String]("datasetId")
        identifier         <- extract[Identifier]("identifier")
        name               <- extract[Name]("name")
        url                <- extract[Url]("url")
        maybeSameAs        <- extract[Option[String]]("sameAs").map(blankToNone).flatMap(toOption[SameAs])
        maybePublishedDate <- extract[Option[PublishedDate]]("publishedDate")
        maybeDescription <- extract[Option[String]]("description")
                             .map(blankToNone)
                             .flatMap(toOption[Description])
      } yield NonModifiedDataset(
        identifier,
        name,
        url,
        maybeSameAs getOrElse SameAs(id),
        maybeDescription,
        DatasetPublishing(maybePublishedDate, Set.empty),
        parts    = List.empty,
        projects = List.empty
      )
    }

    _.downField("results").downField("bindings").as(decodeList(dataset))
  }

  private[rest] implicit val modifiedDecoder: Decoder[List[ModifiedDataset]] = {

    val dataset: Decoder[ModifiedDataset] = { implicit cursor =>
      for {
        identifier         <- extract[Identifier]("identifier")
        name               <- extract[Name]("name")
        url                <- extract[Url]("url")
        derivedFrom        <- extract[DerivedFrom]("derivedFrom")
        maybePublishedDate <- extract[Option[PublishedDate]]("publishedDate")
        maybeDescription <- extract[Option[String]]("description")
                             .map(blankToNone)
                             .flatMap(toOption[Description])
      } yield ModifiedDataset(
        identifier,
        name,
        url,
        derivedFrom,
        maybeDescription,
        DatasetPublishing(maybePublishedDate, Set.empty),
        parts    = List.empty,
        projects = List.empty
      )
    }

    _.downField("results").downField("bindings").as(decodeList(dataset))
  }

  private def extract[T](property: String)(implicit cursor: HCursor, decoder: Decoder[T]): Result[T] =
    cursor.downField(property).downField("value").as[T]
}
