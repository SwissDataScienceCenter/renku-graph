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

package io.renku.eventlog.events.producers

import io.circe.Decoder
import io.renku.events.CategoryName
import io.renku.graph.model.projects
import io.renku.tinytypes._
import io.renku.tinytypes.constraints.{NonNegativeInt, Url}
import io.renku.tinytypes.json.TinyTypeDecoders.{intDecoder, stringDecoder}

private final case class ProjectIds(id: projects.GitLabId, path: projects.Path)

final class TotalCapacity private (val value: Int) extends AnyVal with IntTinyType {

  def -(usedCapacity: UsedCapacity): FreeCapacity = {
    val c = value - usedCapacity.value
    if (c > 0) FreeCapacity(c)
    else FreeCapacity(0)
  }

  def *(v: Double): Double = value * v
}
object TotalCapacity extends TinyTypeFactory[TotalCapacity](new TotalCapacity(_)) with NonNegativeInt[TotalCapacity] {
  implicit val decoder: Decoder[TotalCapacity] = intDecoder(TotalCapacity)
}
final class FreeCapacity private (val value: Int) extends AnyVal with IntTinyType
object FreeCapacity extends TinyTypeFactory[FreeCapacity](new FreeCapacity(_)) with NonNegativeInt[FreeCapacity] {
  implicit val decoder: Decoder[FreeCapacity] = intDecoder(FreeCapacity)
}
final class UsedCapacity private (val value: Int) extends AnyVal with IntTinyType
object UsedCapacity extends TinyTypeFactory[UsedCapacity](new UsedCapacity(_)) with NonNegativeInt[UsedCapacity] {
  val zero:             UsedCapacity          = UsedCapacity(0)
  implicit val decoder: Decoder[UsedCapacity] = intDecoder(UsedCapacity)
}

private final class SourceUrl private (val value: String) extends AnyVal with StringTinyType
private object SourceUrl extends TinyTypeFactory[SourceUrl](new SourceUrl(_)) with Url[SourceUrl] {
  implicit val decoder: Decoder[SourceUrl] = stringDecoder(SourceUrl)
}

final case class EventProducerStatus(categoryName: CategoryName, maybeCapacity: Option[EventProducerStatus.Capacity])
object EventProducerStatus {
  final case class Capacity(total: TotalCapacity, free: FreeCapacity)
}
