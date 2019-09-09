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

package ch.datascience.rdfstore

import ch.datascience.tinytypes.{JsonTinyType, TinyTypeFactory}
import io.circe.Json

import scala.language.higherKinds

class JsonLDTriples private (val value: Json) extends AnyVal with JsonTinyType

object JsonLDTriples extends TinyTypeFactory[JsonLDTriples](new JsonLDTriples(_)) {
  import cats.MonadError
  import io.circe.parser

  def parse[Interpretation[_]](
      string:    String
  )(implicit ME: MonadError[Interpretation, Throwable]): Interpretation[JsonLDTriples] = ME.fromEither {
    for {
      json    <- parser parse string
      triples <- JsonLDTriples from json
    } yield triples
  }
}
