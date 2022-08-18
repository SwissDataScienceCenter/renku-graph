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

import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.http.rest.Links.{Link, _links}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class linksSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "links" should {

    "be serializable to Json" in {
      forAll { links: Links =>
        links.asJson shouldBe Json.arr(links.links.map(_.asJson).toList: _*)
      }
    }

    "be deserializable from Json" in {
      forAll { links: Links =>
        Json.arr(links.links.map(_.asJson).toList: _*).as[Links] shouldBe Right(links)
      }
    }

    "be deserializable from Json with no 'method'" in {
      val link = linkObjects.generateOne.copy(method = Links.Method.GET)

      val encoder: Encoder[Link] = Encoder.instance[Link] { link =>
        json"""{
          "rel":  ${link.rel.value},
          "href": ${link.href.value}
        }"""
      }

      Json.arr(List(link).map(_.asJson(encoder)): _*).as[Links] shouldBe Right(Links.of(link))
    }
  }

  "_links" should {

    "create a Json object with '_links' property and the given Rel and Href tuples as the value" in {
      forAll { links: Links =>
        val relHrefTuple +: relHrefTuples = links.links.map { case Link(rel, href, _) =>
          rel -> href
        }.toList

        _links(relHrefTuple, relHrefTuples: _*) shouldBe json"""{
          "_links": ${Links(links.links.map(_.copy(method = Links.Method.GET)))}
        }"""
      }
    }

    "create a Json object with '_links' property and the given Links as the value" in {
      forAll { links: Links =>
        _links(links) shouldBe json"""{
          "_links": $links
        }"""
      }
    }

    "create a Json object with '_links' property when links given as varargs" in {
      forAll { links: Links =>
        _links(links.links.head, links.links.tail: _*) shouldBe json"""{
          "_links": $links
        }"""
      }
    }
  }

  "get" should {

    "return a link matching the given Rel" in {
      val link1 = linkObjects.generateOne
      val link2 = linkObjects.generateOne

      Links.of(link1, link2).get(link2.rel) shouldBe Some(link2)
    }

    "return None if there's no link with matching Rel" in {
      val link1 = linkObjects.generateOne

      Links.of(link1).get(rels.generateOne) shouldBe None
    }
  }

  private implicit val linkEncoder: Encoder[Link] = Encoder.instance[Link] { link =>
    link.method match {
      case Links.Method.GET =>
        json"""{
          "rel":    ${link.rel.value},
          "href":   ${link.href.value}
        }"""
      case method =>
        json"""{
          "rel":    ${link.rel.value},
          "href":   ${link.href.value},
          "method": ${method.value}
        }"""
    }
  }
}
