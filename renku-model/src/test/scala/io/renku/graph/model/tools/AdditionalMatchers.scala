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

package io.renku.graph.model.tools

import com.softwaremill.diffx.Diff
import com.softwaremill.diffx.scalatest.DiffShouldMatcher
import io.renku.jsonld.{JsonLD, JsonLDDecoder}
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.{Assertion, Assertions}

import scala.reflect.{ClassTag, classTag}

trait AdditionalMatchers extends DiffShouldMatcher {

  final implicit class EitherDiffMatcher[A, B: Diff](self: Either[A, B]) {
    def shouldMatchToRight(other: B): Assertion =
      self.fold(
        {
          case ex: Throwable => throw ex
          case a => Assertions.fail(s"Unexpected left value: $a")
        },
        b => b shouldMatchTo other
      )
  }

  def decodeAndEqualTo[A: JsonLDDecoder: Diff: ClassTag](right: A, filter: A => A = (a: A) => a): Matcher[JsonLD] =
    new AdditionalMatchers.JsonLDDecodingMatcher[A](right, filter)
}

object AdditionalMatchers {

  final class JsonLDDecodingMatcher[A: JsonLDDecoder: Diff: ClassTag](value: A, filter: A => A)
      extends Matcher[JsonLD]
      with DiffShouldMatcher {
    def apply(left: JsonLD): MatchResult = {
      val decoded = left.cursor.as[A].map(filter)
      decoded match {
        case Right(dval) =>
          val matchResult = diffMatcher(value).apply(dval)
          if (matchResult.matches) matchResult
          else
            matchResult.copy(rawFailureMessage = matchResult.rawFailureMessage + s"\nJsonLD:\n${left.toJson.noSpaces}")

        case Left(err) =>
          val clazz = classTag[A].runtimeClass
          MatchResult(
            matches = false,
            s"Decoding JsonLD into a ${clazz.getSimpleName} failed: ${err.getMessage()}. JsonLD:\n  ${left.toJson.noSpaces}",
            ""
          )
      }
    }
  }

  // Note: copied from com.softwaremill.diffx.scalatest.DiffShouldMatcher, bc of private scope
  private def diffMatcher[A: Diff](right: A): Matcher[A] = { left =>
    val result = Diff[A].apply(left, right)
    if (!result.isIdentical) {
      val diff =
        result.show().split('\n').mkString(Console.RESET, s"${Console.RESET}\n${Console.RESET}", Console.RESET)
      MatchResult(matches = false, s"Matching error:\n$diff", "")
    } else {
      MatchResult(matches = true, "", "")
    }
  }
}
