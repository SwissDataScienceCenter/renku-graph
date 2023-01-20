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

package io.renku.cli.model

import com.softwaremill.diffx.Diff
import com.softwaremill.diffx.scalatest.DiffShouldMatcher._
import io.renku.jsonld.{JsonLD, JsonLDDecoder, JsonLDEncoder}

trait JsonLDCodecMatchers {

  /** Asserts that encoding the given value and then decoding the result yields the same value.
   * 
   * Note that this only works for structures that don't contain itself references to other As. The
   * assumption is that the given value produces as JSON-LD that contains only one value of type A.
   */
  def assertCompatibleCodec[A <: CliModel: JsonLDDecoder: JsonLDEncoder: Diff](value: A, more: A*): Unit =
    assertCompatibleCodec((v: A) => List(v))(value, more: _*)

  /** Asserts that encoding the given value and decoding the result yields the expected value. */
  def assertCompatibleCodec[A <: CliModel: JsonLDDecoder: JsonLDEncoder: Diff](expected: A => List[A])(
      value: A,
      more:  A*
  ): Unit = {
    val jsonLD = value.asFlattenedJsonLD
    val back = jsonLD.cursor
      .as[List[A]]
      .fold(throw _, identity)

    back shouldMatchTo expected(value)

    if (more.nonEmpty) {
      val all       = value :: more.toList
      val allJsonLD = combineArrays(all.map(_.asFlattenedJsonLD))
      //println(allJsonLD.toJson.spaces2)
      //println(List.fill(80)("-").mkString)
      val allBack = allJsonLD.cursor.as[List[A]].fold(throw _, identity)

      allBack.sortBy(_.toString) shouldMatchTo all.flatMap(expected).sortBy(_.toString)
      ()
    }
  }

  private def combineArrays(jsonld: Seq[JsonLD]): JsonLD =
    jsonld.foldLeft(JsonLD.JsonLDArray(Seq.empty)) { (result, element) =>
      element match {
        case JsonLD.JsonLDArray(inner) => result.copy(jsons = inner ++ result.jsons)
        case _                         => result.copy(element +: result.jsons)
      }
    }
}
