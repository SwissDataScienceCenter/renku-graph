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

package io.renku.microservices

import io.renku.tinytypes.constraints.NonBlank
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter.ofPattern
import scala.util.Random

final class MicroserviceIdentifier private (val value: String) extends AnyVal with StringTinyType
object MicroserviceIdentifier
    extends TinyTypeFactory[MicroserviceIdentifier](new MicroserviceIdentifier(_))
    with NonBlank {

  def generate: MicroserviceIdentifier = generate(LocalDateTime.now _)

  private[microservices] def generate(now: () => LocalDateTime): MicroserviceIdentifier =
    MicroserviceIdentifier(s"${now().format(ofPattern("yyyyMMddHHmmss"))}-${Random.between(1000, 9999)}")
}
