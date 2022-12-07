/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.http.rest

import cats.data.NonEmptyList
import cats.syntax.all._
import io.circe._
import io.circe.literal._
import io.circe.syntax._
import io.renku.http.rest.Links.{Link, Rel}
import io.renku.tinytypes.constraints.{NonBlank, Url, UrlOps}
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory, UrlTinyType}

final case class Links(links: NonEmptyList[Link]) extends LinkOps

trait LinkOps {
  val links: NonEmptyList[Link]

  def get(rel: Rel): Option[Link] = links.find(_.rel == rel)
}

object Links {

  def apply(link: (Rel, Href)): Links = Links(NonEmptyList.of(Link(link)))

  def of(link: Link, other: Link*): Links = Links(NonEmptyList.of(link, other: _*))

  def of(link: (Rel, Href), other: (Rel, Href)*): Links = Links(NonEmptyList.of(link, other: _*).map(Link.apply))

  def self(href: Href): Links = Links(NonEmptyList.of(Link.self(href)))

  final class Rel private (val value: String) extends AnyVal with StringTinyType
  implicit object Rel extends TinyTypeFactory[Rel](new Rel(_)) with NonBlank[Rel] {
    lazy val Self: Rel = Rel("self")
  }

  final class Href private (val value: String) extends AnyVal with UrlTinyType

  implicit object Href extends TinyTypeFactory[Href](new Href(_)) with Url[Href] with UrlOps[Href] {
    def apply(value: UrlTinyType): Href = Href(value.value)
  }

  sealed trait Method extends StringTinyType with Product with Serializable

  object Method extends TinyTypeFactory[Method](MethodInstantiator) {
    final case object GET extends Method {
      override val value: String = "GET"
    }

    final case object POST extends Method {
      override val value: String = "POST"
    }

    lazy val all: Set[Method] = Set(GET, POST)

    implicit val jsonDecoder: Decoder[Method] = Decoder.decodeString.emap { value =>
      Either.fromOption(
        Method.all.find(_.value == value),
        ifNone = s"'$value' unknown Link Method"
      )
    }
  }

  private object MethodInstantiator extends (String => Method) {
    override def apply(value: String): Method = Method.all.find(_.value.toLowerCase == value.toLowerCase).getOrElse {
      throw new IllegalArgumentException(s"'$value' unknown Method")
    }
  }

  final case class Link(rel: Rel, href: Href, method: Method = Method.GET)

  object Link {
    def self(href:   Href):        Link = Link(Rel.Self, href)
    def apply(tuple: (Rel, Href)): Link = Link(tuple._1, tuple._2)
  }

  implicit val linksEncoder: Encoder[Links] = Encoder.instance[Links] { links =>
    import io.circe.Json

    def toJson(link: Link) = Json.obj(
      List(
        Some("rel"                                            -> link.rel.asJson),
        Some("href"                                           -> link.href.asJson),
        Option.when(link.method != Links.Method.GET)("method" -> link.method.asJson)
      ).flatten: _*
    )

    Json.arr(links.links.toList.map(toJson): _*)
  }

  implicit val linksDecoder: Decoder[Links] = {
    import io.circe.Decoder.decodeList
    import io.renku.tinytypes.json.TinyTypeDecoders._

    val link: Decoder[Link] = (cursor: HCursor) =>
      for {
        rel    <- cursor.downField("rel").as[Rel]
        href   <- cursor.downField("href").as[Href]
        method <- cursor.downField("method").as[Option[Method]].map(_.getOrElse(Method.GET))
      } yield Link(rel, href, method)

    decodeList(link).emap {
      case Nil          => Left("No links found")
      case head :: tail => Right(Links(NonEmptyList.of(head, tail: _*)))
    }
  }

  def _links(link: (Rel, Href), other: (Rel, Href)*): Json = _links(
    Links(
      NonEmptyList.fromListUnsafe(
        (link +: other).map(Link.apply).toList
      )
    )
  )

  def _links(link: Link, more: Link*): Json =
    _links(Links(NonEmptyList.of(link, more: _*)))

  def _links(links: Links): Json = json"""{
    "_links": $links
  }"""
}
