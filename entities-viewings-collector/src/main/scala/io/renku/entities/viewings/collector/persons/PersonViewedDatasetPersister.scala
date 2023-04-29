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

package io.renku.entities.viewings.collector.persons

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.{datasets, persons}
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder, TSClient}
import org.typelevel.log4cats.Logger

private[viewings] trait PersonViewedDatasetPersister[F[_]] {
  def persist(event: GLUserViewedDataset): F[Unit]
}

private[viewings] object PersonViewedDatasetPersister {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[PersonViewedDatasetPersister[F]] =
    ProjectsConnectionConfig[F]()
      .map(TSClient[F](_))
      .map(apply[F](_))

  def apply[F[_]: MonadThrow](tsClient: TSClient[F]): PersonViewedDatasetPersister[F] =
    new PersonViewedDatasetPersisterImpl[F](tsClient,
                                            PersonFinder(tsClient),
                                            PersonViewedDatasetDeduplicator[F](tsClient)
    )
}

private class PersonViewedDatasetPersisterImpl[F[_]: MonadThrow](tsClient: TSClient[F],
                                                                 personFinder: PersonFinder[F],
                                                                 deduplicator: PersonViewedDatasetDeduplicator[F]
) extends PersonViewedDatasetPersister[F] {

  import Encoder._
  import cats.syntax.all._
  import deduplicator._
  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.graph.model.GraphClass
  import io.renku.graph.model.Schemas._
  import io.renku.jsonld.syntax._
  import io.renku.triplesstore.ResultsDecoder._
  import io.renku.triplesstore.SparqlQuery.Prefixes
  import io.renku.triplesstore.client.syntax._
  import io.renku.triplesstore.{ResultsDecoder, SparqlQuery}
  import personFinder._
  import tsClient._

  override def persist(event: GLUserViewedDataset): F[Unit] =
    findPersonId(event.userId) >>= {
      case None           => ().pure[F]
      case Some(personId) => persistIfOlderOrNone(personId, event)
    }

  private def persistIfOlderOrNone(personId: persons.ResourceId, event: GLUserViewedDataset) =
    findStoredDate(personId, event.dataset.id) >>= {
      case None =>
        insert(personId, event) >>
          deduplicate(personId, event.dataset.id)
      case Some(date) if date < event.date =>
        deleteOldViewedDate(personId, event.dataset.id) >>
          insert(personId, event) >>
          deduplicate(personId, event.dataset.id)
      case _ => ().pure[F]
    }

  private def findStoredDate(personId:  persons.ResourceId,
                             datasetId: datasets.ResourceId
  ): F[Option[datasets.DateViewed]] =
    queryExpecting {
      SparqlQuery.ofUnsafe(
        show"${GraphClass.PersonViewings}: find dataset viewed date",
        Prefixes of renku -> "renku",
        sparql"""|SELECT (MAX(?date) AS ?mostRecentDate)
                 |WHERE {
                 |  GRAPH ${GraphClass.PersonViewings.id} {
                 |    BIND (${personId.asEntityId} AS ?personId)
                 |    ?personId renku:viewedDataset ?viewingId.
                 |    ?viewingId renku:dataset ${datasetId.asEntityId};
                 |               renku:dateViewed ?date.
                 |  }
                 |}
                 |GROUP BY ?id
                 |""".stripMargin
      )
    }(dateDecoder)

  private lazy val dateDecoder: Decoder[Option[datasets.DateViewed]] = ResultsDecoder[Option, datasets.DateViewed] {
    Decoder.instance[datasets.DateViewed] { implicit cur =>
      import io.renku.tinytypes.json.TinyTypeDecoders._
      extract[datasets.DateViewed]("mostRecentDate")
    }
  }

  private def deleteOldViewedDate(personId: persons.ResourceId, datasetId: datasets.ResourceId): F[Unit] =
    updateWithNoResult(
      SparqlQuery.ofUnsafe(
        show"${GraphClass.PersonViewings}: delete",
        Prefixes of renku -> "renku",
        sparql"""|DELETE {
                 |  GRAPH ${GraphClass.PersonViewings.id} {
                 |    ?viewingId ?p ?o
                 |  }
                 |}
                 |WHERE {
                 |  GRAPH ${GraphClass.PersonViewings.id} {
                 |    ${personId.asEntityId} renku:viewedDataset ?viewingId.
                 |    ?viewingId renku:dataset ${datasetId.asEntityId};
                 |               ?p ?o.
                 |  }
                 |}
                 |""".stripMargin
      )
    )

  private def insert(personId: persons.ResourceId, event: GLUserViewedDataset): F[Unit] =
    upload(
      encode(PersonViewedDataset(personId, event.dataset, event.date))
    )
}
