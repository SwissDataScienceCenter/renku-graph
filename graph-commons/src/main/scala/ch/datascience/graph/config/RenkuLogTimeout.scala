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

package ch.datascience.graph.config

import cats.syntax.all._
import ch.datascience.tinytypes.{TinyType, TinyTypeFactory}

import scala.concurrent.duration.FiniteDuration

final class RenkuLogTimeout private (val value: FiniteDuration) extends AnyVal with TinyType { type V = FiniteDuration }
object RenkuLogTimeout extends TinyTypeFactory[RenkuLogTimeout](new RenkuLogTimeout(_)) {

  import cats.MonadError
  import ch.datascience.config.ConfigLoader.find
  import com.typesafe.config.{Config, ConfigFactory}

  def apply[Interpretation[_]](
      config:    Config = ConfigFactory.load
  )(implicit ME: MonadError[Interpretation, Throwable]): Interpretation[RenkuLogTimeout] =
    find[Interpretation, FiniteDuration]("renku-log-timeout", config) map RenkuLogTimeout.apply

  import ch.datascience.tinytypes.TinyTypeConverter

  implicit val toJavaTimeDuration: TinyTypeConverter[RenkuLogTimeout, java.time.Duration] = {
    timeout: RenkuLogTimeout =>
      Right {
        java.time.Duration.ofMillis(timeout.value.toMillis)
      }
  }
}
