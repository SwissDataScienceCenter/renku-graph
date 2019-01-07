/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.tinytypes.json

import ch.datascience.tinytypes.{ TinyType, TinyTypeFactory }
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import play.api.libs.json.Json.toJson
import play.api.libs.json._

class TinyTypeReadsSpec extends WordSpec {

  private implicit val reads: Reads[StringTinyType] = TinyTypeReads( StringTinyType.apply )

  "apply" should {

    "create a Reads to deserialize the tiny type from JSON" in {
      JsString( "abc" ).as[StringTinyType] shouldBe StringTinyType( "abc" )
    }

    "create a Reads which fails deserialization for different JsValue type" in {
      JsBoolean( true ).validate[StringTinyType] shouldBe jsStringReads( JsBoolean( true ) )
    }

    "create a Reads which fails deserialization when TinyType instantation fails" in {
      val Left( errors ) = JsString( "abcdef" ).validate[StringTinyType].asEither

      val ( path, pathErrors ) = errors.head
      path shouldBe JsPath()
      pathErrors should have size 1
      pathErrors.head.message shouldBe "some message"
    }
  }
}

class TinyTypeWritesSpec extends WordSpec {

  private implicit val writes: Writes[StringTinyType] = TinyTypeWrites[String, StringTinyType]

  "apply" should {

    "create a Writes to serialize a tiny type to JSON" in {
      toJson( StringTinyType( "abc" ) ) shouldBe JsString( "abc" )
    }
  }
}

class TinyTypeFormatSpec extends WordSpec {

  private implicit val format: Format[StringTinyType] = TinyTypeFormat( StringTinyType.apply )

  "apply" should {

    "create a Format to allow serialization to JSON" in {
      toJson( StringTinyType( "abc" ) ) shouldBe JsString( "abc" )
    }

    "create a Format to allow deserialization from JSON" in {
      JsString( "abc" ).as[StringTinyType] shouldBe StringTinyType( "abc" )
    }
  }

}

private class StringTinyType private ( val value: String ) extends AnyVal with TinyType[String]

private object StringTinyType
  extends TinyTypeFactory[String, StringTinyType]( new StringTinyType( _ ) ) {

  addConstraint(
    check = _.length < 5,
    message = _ => "some message"
  )
}
