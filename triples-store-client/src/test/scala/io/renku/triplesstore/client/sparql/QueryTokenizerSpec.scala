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

package io.renku.triplesstore.client.sparql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class QueryTokenizerSpec extends AnyFlatSpec with should.Matchers {
  val tokenizer = QueryTokenizer.luceneStandard

  it should "split on whitespace" in {
    val input = "  one two\tthree   four\nfive "
    tokenizer.split(input) shouldBe List("one", "two", "three", "four", "five")
  }

  it should "split on dashes" in {
    val input = "one-two-three-"
    tokenizer.split(input) shouldBe List("one", "two", "three")
  }

  it should "split on underscores" in {
    val input = "01_one_two"
    QueryTokenizer.extended.split(input) shouldBe List("01", "one", "two")
  }

  it should "return different data" in {
    val input = "one 1 two 2-3-4 \"http://hello.com\" next~"
    tokenizer.split(input) shouldBe List("one", "1", "two", "2", "3", "4", "http", "hello.com", "next")
  }
}
