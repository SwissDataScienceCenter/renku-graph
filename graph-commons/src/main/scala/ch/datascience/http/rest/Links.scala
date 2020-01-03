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

package ch.datascience.http.rest

import cats.data.NonEmptyList
import ch.datascience.http.rest.Links.{Link, Rel}
import ch.datascience.tinytypes.constraints.{NonBlank, Url, UrlOps}
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}
import io.circe._
import io.circe.literal._

final case class Links(links: NonEmptyList[Link]) extends LinkOps

trait LinkOps {
  val links: NonEmptyList[Link]

  def get(rel: Rel): Option[Link] = links.find(_.rel == rel)
}

object Links {

  def apply(link: (Rel, Href)): Links = Links(NonEmptyList.of(Link(link)))
  def of(link:    Link, other: Link*): Links = Links(NonEmptyList.of(link, other: _*))
  def of(link:    (Rel, Href), other: (Rel, Href)*): Links = Links(NonEmptyList.of(link, other: _*).map(Link.apply))

  def self(href: Href) = Links(NonEmptyList.of(Link.self(href)))

  final class Rel private (val value: String) extends AnyVal with StringTinyType
  implicit object Rel extends TinyTypeFactory[Rel](new Rel(_)) with NonBlank {
    lazy val Self: Rel = Rel("self")
  }

  final class Href private (val value: String) extends AnyVal with StringTinyType
  implicit object Href extends TinyTypeFactory[Href](new Href(_)) with Url with UrlOps[Href] {
    def apply(value: StringTinyType): Href = Href(value.value)
  }

  final case class Link(rel: Rel, href: Href)

  object Link {
    def self(href:   Href):        Link = Link(Rel.Self, href)
    def apply(tuple: (Rel, Href)): Link = Link(tuple._1, tuple._2)
  }

  implicit val linksEncoder: Encoder[Links] = Encoder.instance[Links] { links =>
    import io.circe.Json
    import io.circe.literal._

    def toJson(link: Link) = json"""{
      "rel": ${link.rel.value},
      "href": ${link.href.value}
    }"""

    Json.arr(links.links.toList.map(toJson): _*)
  }

  implicit val linksDecoder: Decoder[Links] = {
    import ch.datascience.tinytypes.json.TinyTypeDecoders._
    import io.circe.Decoder.decodeList

    val link: Decoder[Link] = (cursor: HCursor) =>
      for {
        rel  <- cursor.downField("rel").as[Rel]
        href <- cursor.downField("href").as[Href]
      } yield Link(rel, href)

    decodeList(link).emap {
      case Nil          => Left("No links found")
      case head +: tail => Right(Links(NonEmptyList.of(head, tail: _*)))
    }
  }

  def _links(link: Link, more: Link*): Json =
    _links(Links(NonEmptyList.of(link, more: _*)))

  def _links(links: Links): Json = json"""{
    "_links": $links
  }"""
}
