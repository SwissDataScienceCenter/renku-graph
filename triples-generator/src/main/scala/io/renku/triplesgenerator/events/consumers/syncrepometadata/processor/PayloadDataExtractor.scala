/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.syncrepometadata
package processor

import cats.MonadThrow
import cats.syntax.all._
import io.renku.cli.model.CliProject
import io.renku.cli.model.Ontologies.{Schema => CliSchema}
import io.renku.eventlog.api.EventLogClient.EventPayload
import io.renku.graph.model.images.Image
import io.renku.graph.model.projects
import org.typelevel.log4cats.Logger

private trait PayloadDataExtractor[F[_]] {
  def extractPayloadData(slug: projects.Slug, payload: EventPayload): F[Option[DataExtract.Payload]]
}

private object PayloadDataExtractor {
  def apply[F[_]: MonadThrow: Logger]: PayloadDataExtractor[F] = new PayloadDataExtractorImpl[F]
}

private class PayloadDataExtractorImpl[F[_]: MonadThrow: Logger] extends PayloadDataExtractor[F] {

  import io.circe.DecodingFailure
  import io.renku.compression.Zip.unzip
  import io.renku.graph.model.projects
  import io.renku.jsonld.JsonLDDecoder._
  import io.renku.jsonld.parser._
  import io.renku.jsonld.{JsonLD, JsonLDDecoder}

  override def extractPayloadData(slug: projects.Slug, payload: EventPayload): F[Option[DataExtract.Payload]] =
    (unzip(payload.data.toArray) >>= parse)
      .fold(logError(slug), _.some.pure[F])
      .flatMap(decode(slug))

  private def decode(slug: projects.Slug): Option[JsonLD] => F[Option[DataExtract.Payload]] = {
    case None =>
      Option.empty[DataExtract.Payload].pure[F]
    case Some(jsonLD) =>
      jsonLD.cursor
        .as[List[DataExtract.Payload]]
        .map(_.headOption)
        .fold(logWarn(slug), _.pure[F])
  }

  private implicit lazy val dataExtract: JsonLDDecoder[DataExtract.Payload] =
    JsonLDDecoder.entity(CliProject.entityTypes) { cur =>
      for {
        maybeDesc <- cur.downField(CliSchema.description).as[Option[projects.Description]]
        keywords  <- cur.downField(CliSchema.keywords).as[Set[Option[projects.Keyword]]].map(_.flatten)
        images    <- cur.downField(CliSchema.image).as[List[Image]].map(_.sortBy(_.position).map(_.uri))
      } yield DataExtract.Payload(maybeDesc, keywords, images)
    }

  private def logError(slug: projects.Slug): Throwable => F[Option[JsonLD]] =
    Logger[F]
      .error(_)(show"$categoryName: cannot process data from the payload for $slug")
      .as(Option.empty[JsonLD])

  private def logWarn(slug: projects.Slug): DecodingFailure => F[Option[DataExtract.Payload]] =
    Logger[F]
      .warn(_)(show"$categoryName: cannot decode the payload for $slug")
      .as(Option.empty[DataExtract.Payload])
}
