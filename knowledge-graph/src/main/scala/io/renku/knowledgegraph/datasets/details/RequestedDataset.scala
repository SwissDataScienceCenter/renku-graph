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

package io.renku.knowledgegraph.datasets.details

import cats.Show
import cats.syntax.all._
import io.renku.graph.model.{datasets, RenkuUrl}
import org.http4s.Uri.Path.{Segment, SegmentEncoder}
import scodec.bits.{Bases, ByteVector}

trait RequestedDataset {
  def fold[A](idf: datasets.Identifier => A, saf: datasets.SameAs => A): A
}

object RequestedDataset {

  private val sameAsPathEncodingPrefix: String = "sa"
  private val base64Alphabet = Bases.Alphabets.Base64Url

  def apply(identifier: datasets.Identifier): RequestedDataset = Identifier(identifier)
  def apply(sameAs:     datasets.SameAs):     RequestedDataset = SameAs(sameAs)

  def unapply(value: String)(implicit renkuUrl: RenkuUrl): Option[RequestedDataset] =
    if (value startsWith sameAsPathEncodingPrefix) {
      val encSameAs = value.replaceFirst(sameAsPathEncodingPrefix, "")
      ByteVector
        .fromBase64(encSameAs, base64Alphabet)
        .flatMap(_.decodeUtf8.toOption)
        .flatMap(datasets.SameAs.of(_).toOption)
        .map(RequestedDataset(_))
    } else
      datasets.Identifier.from(value).toOption.map(RequestedDataset(_))

  final case class Identifier(value: datasets.Identifier) extends RequestedDataset {
    override def fold[A](idf: datasets.Identifier => A, saf: datasets.SameAs => A): A = idf(value)
  }

  final case class SameAs(value: datasets.SameAs) extends RequestedDataset {
    override def fold[A](idf: datasets.Identifier => A, saf: datasets.SameAs => A): A = saf(value)
  }

  implicit val se: SegmentEncoder[RequestedDataset] = SegmentEncoder.instance {
    case Identifier(v) =>
      Segment(v.value)
    case SameAs(v) =>
      val base64 =
        ByteVector
          .encodeUtf8(v.value)
          .fold(throw _, identity)
          .toBase64(base64Alphabet)
      Segment(s"$sameAsPathEncodingPrefix$base64")
  }

  implicit val show: Show[RequestedDataset] = Show.show {
    case Identifier(v) => v.show
    case SameAs(v)     => v.show
  }
}
