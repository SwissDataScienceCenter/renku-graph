/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.tinytypes

import io.renku.generators.Generators.{jsons, localDates, nonBlankStrings, relativePaths, timestamps}
import org.scalacheck.{Arbitrary, Gen}

object Generators {

  implicit val tinyTypes: Gen[TinyType] = Gen.oneOf(
    nonBlankStrings() map (_.value) map (v => new StringTinyType { override def value = v }),
    relativePaths() map (v => new RelativePathTinyType { override def value = v }),
    Arbitrary.arbInt.arbitrary map (v => new IntTinyType { override def value = v }),
    Arbitrary.arbLong.arbitrary map (v => new LongTinyType { override def value = v }),
    jsons map (v => new JsonTinyType { override def value = v }),
    timestamps map (v => new InstantTinyType { override def value = v }),
    localDates map (v => new LocalDateTinyType { override def value = v })
  )

}
