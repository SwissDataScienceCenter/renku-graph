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

package io.renku.triplesgenerator.events.consumers.syncrepometadata
package processor

import cats.MonadThrow
import cats.syntax.all._
import io.renku.cli.model.CliProject
import io.renku.cli.model.Ontologies.{Schema => CliSchema}
import io.renku.graph.model.events.ZippedEventPayload
import io.renku.graph.model.images.Image
import io.renku.graph.model.projects
import io.renku.jsonld.{Cursor, Property}
import org.typelevel.log4cats.Logger

private trait PayloadDataExtractor[F[_]] {
  def extractPayloadData(path: projects.Path, payload: ZippedEventPayload): F[Option[DataExtract.Payload]]
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

  override def extractPayloadData(path: projects.Path, payload: ZippedEventPayload): F[Option[DataExtract.Payload]] =
    (unzip(payload.value) >>= parse)
      .fold(logError(path), _.some.pure[F])
      .flatMap(decode(path))

  private def decode(path: projects.Path): Option[JsonLD] => F[Option[DataExtract.Payload]] = {
    case None =>
      Option.empty[DataExtract.Payload].pure[F]
    case Some(jsonLD) =>
      jsonLD.cursor
        .as(decodeList(dataExtract(path)))
        .map(_.headOption)
        .fold(logWarn(path), _.pure[F])
  }

  private def dataExtract(path: projects.Path): JsonLDDecoder[DataExtract.Payload] =
    JsonLDDecoder.entity(CliProject.entityTypes) { cur =>
      for {
        name <- cur.downField(CliSchema.name).as[Option[projects.Name]] >>= {
                  case None    => decodingFailure(CliSchema.name, cur).asLeft
                  case Some(v) => v.asRight
                }
        maybeDesc <- cur.downField(CliSchema.description).as[Option[projects.Description]]
        keywords  <- cur.downField(CliSchema.keywords).as[Set[Option[projects.Keyword]]].map(_.flatten)
        images    <- cur.downField(CliSchema.image).as[List[Image]].map(_.sortBy(_.position).map(_.uri))
      } yield DataExtract.Payload(path, name, maybeDesc, keywords, images)
    }

  private def decodingFailure(propName: Property, cur: Cursor) =
    DecodingFailure(
      DecodingFailure.Reason.CustomReason(show"no '$propName' property in the payload"),
      cur.jsonLD.toJson.hcursor
    )

  private def logError(path: projects.Path): Throwable => F[Option[JsonLD]] =
    Logger[F]
      .error(_)(show"$categoryName: cannot process data from the payload for $path")
      .as(Option.empty[JsonLD])

  private def logWarn(path: projects.Path): DecodingFailure => F[Option[DataExtract.Payload]] =
    Logger[F]
      .warn(_)(show"$categoryName: cannot decode the payload for $path")
      .as(Option.empty[DataExtract.Payload])
}