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

package ch.datascience.tinytypes.constraints

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{httpUrls, nonBlankStrings, relativePaths}
import ch.datascience.tinytypes.{IntTinyType, StringTinyType, TinyTypeFactory}
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class UrlSpec extends WordSpec with ScalaCheckPropertyChecks {

  import UrlTypes._

  "Url" should {

    "be instantiatable for valid urls" in {
      forAll(httpUrls()) { url =>
        UrlType(url).toString shouldBe url
      }
    }

    "throw an IllegalArgumentException for invalid urls" in {
      intercept[IllegalArgumentException](UrlType("invalid url")).getMessage shouldBe "Cannot instantiate ch.datascience.tinytypes.constraints.UrlTypes.UrlType with 'invalid url'"
    }
  }

  "/" should {

    "allow to add next path segment" in {
      val url = (httpUrls() map UrlType.apply).generateOne
      (url / "path") shouldBe UrlType(s"$url/path")
    }

    "url encode the path segment added to a url" in {
      val url = (httpUrls() map UrlType.apply).generateOne
      (url / "path to smth") shouldBe UrlType(s"$url/path+to+smth")
    }

    "use provided converted when adding the next path segment" in {
      val url       = (httpUrls() map UrlType.apply).generateOne
      val otherPath = (relativePaths(minSegments = 2) map RelativePathString.apply).generateOne
      (url / otherPath) shouldBe UrlType(s"$url/$otherPath")
    }

    "add the given TinyType path segment when it's Some" in {
      val url                         = (httpUrls() map UrlType.apply).generateOne
      val maybePath @ Some(pathValue) = (relativePaths(minSegments = 2) map RelativePathString.apply).generateSome
      (url / maybePath) shouldBe UrlType(s"$url/$pathValue")
    }

    "not add a path segment for None TinyType" in {
      val url       = (httpUrls() map UrlType.apply).generateOne
      val otherPath = Option.empty[RelativePathString]
      (url / otherPath) shouldBe url
    }

    "add the given value path segment when it's Some" in {
      val url                          = (httpUrls() map UrlType.apply).generateOne
      val maybeSegment @ Some(segment) = nonBlankStrings().generateSome.map(_.toString())
      (url / maybeSegment) shouldBe UrlType(s"$url/$segment")
    }

    "not add a path segment for None value" in {
      val url = (httpUrls() map UrlType.apply).generateOne
      (url / Option.empty[String]) shouldBe url
    }
  }

  "?" should {

    "allow to add a String query parameter if there's no such a parameter already" in {
      val url = (httpUrls() map UrlType.apply).generateOne
      (url ? ("param" -> "value a")).toString shouldBe s"$url?param=value+a"
    }

    "allow to add an Int query parameter if there's no such a parameter already" in {
      val url = (httpUrls() map UrlType.apply).generateOne
      (url ? ("param" -> 1)).toString shouldBe s"$url?param=1"
    }

    "allow to add an IntTinyType query parameter if there's no such a parameter already" in {
      val url = (httpUrls() map UrlType.apply).generateOne
      (url ? ("param" -> new IntTinyType { override val value = 1 })).toString shouldBe s"$url?param=1"
    }

    "allow to add an StringTinyType query parameter if there's no such a parameter already" in {
      val url = (httpUrls() map UrlType.apply).generateOne
      (url ? ("param" -> new StringTinyType { override val value = "value a" })).toString shouldBe s"$url?param=value+a"
    }

    "replace the value of the parameter if it already exists as the last one" in {
      val url = (httpUrls() map UrlType.apply).generateOne

      val newUrl: UrlType = url ? ("param1" -> "value a") & ("param2" -> "value C")
      newUrl.toString shouldBe s"$url?param1=value+a&param2=value+C"

      (newUrl ? ("param1" -> "value b")).toString shouldBe s"$url?param1=value+b&param2=value+C"
    }

    "replace the value of the correct parameter - case with similar param names" in {
      val url = (httpUrls() map UrlType.apply).generateOne

      val newUrl: UrlType = url ? ("page" -> "1") & ("per_page" -> "3")
      newUrl.toString shouldBe s"$url?page=1&per_page=3"

      (newUrl ? ("page" -> "2")).toString shouldBe s"$url?page=2&per_page=3"
    }

    "replace the value of the parameter if it already exists somewhere in the middle" in {
      val url = (httpUrls() map UrlType.apply).generateOne

      val newUrl: UrlType = url ? ("param1" -> "value 1") & ("param2" -> "value 2")
      newUrl.toString shouldBe s"$url?param1=value+1&param2=value+2"

      (newUrl ? ("param2" -> "value=3")).toString shouldBe s"$url?param1=value+1&param2=value%3D3"
    }

    "add an additional parameter if there's already at least one in the url" in {
      val url = (httpUrls() map UrlType.apply).generateOne

      val newUrl: UrlType = url ? ("param1" -> "value 1")
      newUrl.toString shouldBe s"$url?param1=value+1"

      (newUrl ? ("param2" -> "value 2")).toString shouldBe s"$url?param1=value+1&param2=value+2"
    }
  }

  "&" should {

    "allow to add a new query parameter" in {
      val url = (httpUrls() map UrlType.apply).generateOne
      ((url ? ("param1" -> "value 1")) & ("param2" -> "value 2")).toString shouldBe s"$url?param1=value+1&param2=value+2"
    }

    "allow to add another query parameter if there's more than one" in {
      val url = (httpUrls() map UrlType.apply).generateOne
      (url ? ("param1" -> "value 1") & ("param2" -> "value 2") & ("param3" -> "value 3")).toString shouldBe s"$url?param1=value+1&param2=value+2&param3=value+3"
    }

    "add query parameter with optional value if the value is given" in {
      val url = (httpUrls() map UrlType.apply).generateOne
      (url ? ("param1" -> "value 1") && ("param2" -> Some("value 2"))).toString shouldBe s"$url?param1=value+1&param2=value+2"
    }

    "do not add query parameter with absent value" in {
      val url = (httpUrls() map UrlType.apply).generateOne
      (url ? ("param1" -> "value 1") && ("param2" -> Option.empty[String])).toString shouldBe s"$url?param1=value+1"
    }

    "replace the value of the parameter if it already exists as the last one" in {
      val url = (httpUrls() map UrlType.apply).generateOne

      val newUrl = url ? ("param1" -> "value 1") & ("param2" -> "value 2")
      newUrl.toString shouldBe s"$url?param1=value+1&param2=value+2"

      (newUrl & ("param1" -> "value b")).toString shouldBe s"$url?param1=value+b&param2=value+2"
    }

    "replace the value of the correct parameter - case with similar param names" in {
      val url = (httpUrls() map UrlType.apply).generateOne

      val newUrl = url ? ("page" -> "1") & ("per_page" -> "3")
      newUrl.toString shouldBe s"$url?page=1&per_page=3"

      (newUrl & ("page" -> "2")).toString shouldBe s"$url?page=2&per_page=3"
    }

    "replace the value of the parameter if it already exists somewhere in the middle" in {
      val url = (httpUrls() map UrlType.apply).generateOne

      val newUrl = url ? ("param1" -> "value 1") & ("param2" -> "value 2")
      newUrl.toString shouldBe s"$url?param1=value+1&param2=value+2"

      (newUrl & ("param2" -> "value=3")).toString shouldBe s"$url?param1=value+1&param2=value%3D3"
    }
  }
}

class BaseUrlSpec extends WordSpec {

  import UrlTypes._

  "/" should {

    "add the given segment and produce a new type" in {

      val baseUrl = (httpUrls() map BaseUrlType.apply).generateOne
      val segment = relativePaths(maxSegments = 1).generateOne

      val extendedUrl = baseUrl / segment

      extendedUrl       shouldBe an[ExtendedUrlType]
      extendedUrl.value shouldBe s"$baseUrl/$segment"
    }
  }
}

private object UrlTypes {

  class UrlType private (val value: String) extends AnyVal with StringTinyType
  object UrlType extends TinyTypeFactory[UrlType](new UrlType(_)) with Url with UrlOps[UrlType]

  class BaseUrlType private (val value: String) extends AnyVal with StringTinyType
  object BaseUrlType
      extends TinyTypeFactory[BaseUrlType](new BaseUrlType(_))
      with Url
      with BaseUrl[BaseUrlType, ExtendedUrlType]

  class ExtendedUrlType private (val value: String) extends AnyVal with StringTinyType
  implicit object ExtendedUrlType
      extends TinyTypeFactory[ExtendedUrlType](new ExtendedUrlType(_))
      with Url
      with UrlOps[ExtendedUrlType]
}
