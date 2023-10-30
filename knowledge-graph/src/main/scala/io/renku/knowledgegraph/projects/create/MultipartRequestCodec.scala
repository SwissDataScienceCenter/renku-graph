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

package io.renku.knowledgegraph.projects.create

import MultipartRequestCodec.PartName
import cats.effect.{Async, Sync}
import cats.syntax.all._
import io.renku.core.client.{Template, templates}
import io.renku.graph.model.projects
import io.renku.knowledgegraph.multipart.syntax._
import io.renku.knowledgegraph.projects.images.Image
import io.renku.knowledgegraph.projects.images.MultipartImageCodecs._
import org.http4s.multipart.{Multipart, Multiparts}

private trait MultipartRequestCodec[F[_]] extends MultipartRequestEncoder[F] with MultipartRequestDecoder[F]

private object MultipartRequestCodec {

  def apply[F[_]: Async](implicit
      encoder: MultipartRequestEncoder[F],
      decoder: MultipartRequestDecoder[F]
  ): MultipartRequestCodec[F] = new MultipartRequestCodec[F] {
    override def encode(newProject: NewProject):   F[Multipart[F]] = encoder.encode(newProject)
    override def decode(multipart:  Multipart[F]): F[NewProject]   = decoder.decode(multipart)
  }

  implicit def forAsync[F[_]](implicit F: Async[F]): MultipartRequestCodec[F] = apply[F]

  private[create] object PartName {
    val name                  = "name"
    val namespaceId           = "namespaceId"
    val description           = "description"
    val keywords              = "keywords"
    val visibility            = "visibility"
    val image                 = "image"
    val templateRepositoryUrl = "templateRepositoryUrl"
    val templateId            = "templateId"
  }
}

private trait MultipartRequestEncoder[F[_]] {
  def encode(newProject: NewProject): F[Multipart[F]]
}

private object MultipartRequestEncoder {
  def apply[F[_]: Sync]: MultipartRequestEncoder[F] = forSync(implicitly[Sync[F]])
  implicit def forSync[F[_]](implicit F: Sync[F]): MultipartRequestEncoder[F] = new MultipartRequestEncoderImpl[F]
}

private class MultipartRequestEncoderImpl[F[_]: Sync] extends MultipartRequestEncoder[F] {

  override def encode(newProject: NewProject): F[Multipart[F]] = {

    val parts =
      Vector(
        newProject.maybeDescription.asParts[F](PartName.description),
        newProject.maybeImage.asParts[F](PartName.image)
      ).flatten
        .appended(newProject.name.asPart[F](PartName.name))
        .appended(newProject.namespace.identifier.asPart[F](PartName.namespaceId))
        .appended(newProject.visibility.asPart[F](PartName.visibility))
        .appended(newProject.template.repositoryUrl.asPart[F](PartName.templateRepositoryUrl))
        .appended(newProject.template.identifier.asPart[F](PartName.templateId))
        .appendedAll(newProject.keywords.toList.asParts[F](PartName.keywords))

    Multiparts.forSync[F].flatMap(_.multipart(parts))
  }
}

private trait MultipartRequestDecoder[F[_]] {
  def decode(multipart: Multipart[F]): F[NewProject]
}

private object MultipartRequestDecoder {
  def apply[F[_]: Async]: MultipartRequestDecoder[F] = forAsync[F]
  implicit def forAsync[F[_]](implicit F: Async[F]): MultipartRequestDecoder[F] = new MultipartRequestDecoderImpl[F]
}

private class MultipartRequestDecoderImpl[F[_]: Async] extends MultipartRequestDecoder[F] {

  override def decode(multipart: Multipart[F]): F[NewProject] =
    (multipart.part(PartName.name).flatMap(_.as[projects.Name]),
     multipart.part(PartName.namespaceId).flatMap(_.as[NamespaceId]).map(Namespace(_)),
     multipart.findPart(PartName.description).map(_.as[Option[projects.Description]]).sequence.map(_.flatten),
     multipart.filterParts(PartName.keywords).map(_.as[projects.Keyword]).sequence.map(_.toSet),
     multipart.part(PartName.visibility).flatMap(_.as[projects.Visibility]),
     templateDecoder(multipart),
     multipart.findPart(PartName.image).map(_.as[Option[Image]]).sequence.map(_.flatten)
    ).mapN(NewProject.apply)

  private def templateDecoder(multipart: Multipart[F]) =
    (multipart.part(PartName.templateRepositoryUrl).flatMap(_.as[templates.RepositoryUrl]),
     multipart.part(PartName.templateId).flatMap(_.as[templates.Identifier])
    ).mapN(Template.apply)
}
