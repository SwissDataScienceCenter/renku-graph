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

package io.renku.triplesgenerator.events.categories.awaitinggeneration

import cats.MonadError
import io.renku.config.ConfigLoader
import io.renku.tinytypes.{IntTinyType, TinyTypeFactory}

private[events] final class GenerationProcessesNumber private (val value: Int) extends AnyVal with IntTinyType
private object GenerationProcessesNumber
    extends TinyTypeFactory[GenerationProcessesNumber](new GenerationProcessesNumber(_)) {

  import ConfigLoader._
  import com.typesafe.config.{Config, ConfigFactory}
  import pureconfig.ConfigReader

  private implicit val configReader: ConfigReader[GenerationProcessesNumber] = intTinyTypeReader(this)

  def apply[Interpretation[_]](
      config:    Config = ConfigFactory.load()
  )(implicit ME: MonadError[Interpretation, Throwable]): Interpretation[GenerationProcessesNumber] =
    find[Interpretation, GenerationProcessesNumber]("generation-processes-number", config)
}
