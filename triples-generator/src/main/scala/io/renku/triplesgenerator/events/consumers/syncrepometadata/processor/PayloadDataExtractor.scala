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

package io.renku.triplesgenerator.events.consumers.syncrepometadata.processor

import cats.MonadThrow
import cats.syntax.all._
import io.renku.graph.model.events.ZippedEventPayload
import io.renku.graph.model.projects

private trait PayloadDataExtractor[F[_]] {
  def extractPayloadData(path: projects.Path, payload: ZippedEventPayload): F[Option[DataExtract.Payload]]
}

private object PayloadDataExtractor {
  def apply[F[_]: MonadThrow]: PayloadDataExtractor[F] = new PayloadDataExtractorImpl[F]
}

private class PayloadDataExtractorImpl[F[_]: MonadThrow] extends PayloadDataExtractor[F] {

  import io.circe.DecodingFailure
  import io.renku.compression.Zip.unzip
  import io.renku.graph.model.{entities, projects}
  import io.renku.jsonld.JsonLDDecoder._
  import io.renku.jsonld.parser._
  import io.renku.jsonld.{JsonLD, JsonLDDecoder}

  override def extractPayloadData(path: projects.Path, payload: ZippedEventPayload): F[Option[DataExtract.Payload]] =
    MonadThrow[F].fromEither {
      unzip(payload.value) >>= parse >>= decode(path)
    }

  private def decode(path: projects.Path): JsonLD => Either[DecodingFailure, Option[DataExtract.Payload]] =
    _.cursor.as(decodeList(dataExtract(path))).map(_.headOption)

  private def dataExtract(path: projects.Path): JsonLDDecoder[DataExtract.Payload] =
    JsonLDDecoder.entity(entities.Project.entityTypes) { cur =>
      for {
        name <- cur.downField(entities.Project.Ontology.nameProperty.id).as[Option[projects.Name]] >>= {
                  case None    => DecodingFailure(DecodingFailure.Reason.MissingField, cur.jsonLD.toJson.hcursor).asLeft
                  case Some(v) => v.asRight
                }
      } yield DataExtract.Payload(path, name)
    }
}
