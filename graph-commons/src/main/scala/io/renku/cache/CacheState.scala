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

package io.renku.cache

import scala.collection.immutable.TreeSet
import scala.concurrent.duration.FiniteDuration

final class CacheState[A, B](data: Map[A, (Key[A], Option[B])], keys: TreeSet[Key[A]], ignoreEmptyValues: Boolean) {
  require(data.size == keys.size, s"sizes differ: data=${data.size} vs keys=${keys.size}")

  def get(key: A, currentTime: FiniteDuration): (CacheState[A, B], CacheResult[Option[B]]) =
    data
      .get(key)
      .map { case (curKey, result) =>
        val newKey   = curKey.withAccessedAt(currentTime)
        val nextKeys = keys - curKey
        val next = new CacheState[A, B](
          data.updated(newKey.value, newKey -> result),
          nextKeys + newKey,
          ignoreEmptyValues
        )
        println(s"hit: $newKey -> $result")

        (next, CacheResult.Hit(result))
      }
      .getOrElse(this -> CacheResult.Miss)

  def put(key: Key[A], entry: Option[B]): CacheState[A, B] =
    if (ignoreEmptyValues && entry.isEmpty) this
    else
      data.get(key.value) match {
        case Some((curKey, result)) if result != entry =>
          new CacheState[A, B](
            data.updated(key.value, key -> result),
            (keys - curKey) + key,
            ignoreEmptyValues
          )
        case Some(_) => this
        case None =>
          new CacheState[A, B](
            data.updated(key.value, key -> entry),
            keys + key,
            ignoreEmptyValues
          )
      }

  def put(key: Key[A], entry: B): CacheState[A, B] = put(key, Some(entry))

  def shrinkTo(targetSize: Int): CacheState[A, B] = {
    val currentSize = keys.size
    val diff        = currentSize - targetSize
    println(s"shrink $currentSize -> $targetSize by $diff")
    if (diff <= 0) this
    else {
      val (toDrop, rest) = keys.splitAt(diff)
      println(s"dropping: ${toDrop.toList.map(_.value)}")
      new CacheState[A, B](
        data.removedAll(toDrop.unsorted.map(_.value)),
        rest,
        ignoreEmptyValues
      )
    }
  }

  override def toString: String = s"CacheState(data=$data, keys=$keys)"
}

object CacheState {

  def create[A, B](evictStrategy: EvictStrategy, ignoreEmptyValues: Boolean): CacheState[A, B] =
    createEmpty(evictStrategy.keyOrder, ignoreEmptyValues)

  private def createEmpty[A, B](ordering: Ordering[Key[A]], ignoreEmpty: Boolean): CacheState[A, B] =
    new CacheState[A, B](Map.empty, TreeSet.empty(ordering), ignoreEmpty)
}
