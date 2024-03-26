/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph

import cats.syntax.all._
import io.renku.CommonGenerators.messages
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.http.client.HttpClientGenerators.{clientErrorHttpStatuses, serverErrorHttpStatuses}
import org.scalacheck.Gen

private object Generators {

  val simpleFailures: Gen[Failure] =
    (Gen.oneOf(clientErrorHttpStatuses, serverErrorHttpStatuses), messages)
      .mapN(Failure(_, _))

  def withCauseFailures(causeGen: Gen[Throwable] = exceptions): Gen[Failure] =
    (Gen.oneOf(clientErrorHttpStatuses, serverErrorHttpStatuses), messages, causeGen)
      .mapN(Failure(_, _, _))

  val failures: Gen[Failure] =
    Gen.oneOf(simpleFailures, withCauseFailures())
}
