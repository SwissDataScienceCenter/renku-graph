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

private[cache] final case class CacheState[A, B](
    data:   Map[A, (Key[A], Option[B])],
    keys:   TreeSet[Key[A]],
    config: CacheConfig
) {
  require(data.size == keys.size, s"sizes differ: data=${data.size} vs keys=${keys.size}")

  def get(key: A, currentTime: FiniteDuration): (CacheState[A, B], CacheResult[Option[B]]) =
    data
      .get(key)
      .map { case (k, r) => (k, r, config.isExpired(k, currentTime)) }
      .map { case (curKey, result, expired) =>
        val nextKeys = keys - curKey
        val newKey   = curKey.withAccessedAt(currentTime)

        if (expired) (new CacheState[A, B](data.removed(curKey.value), nextKeys, config), CacheResult.Miss)
        else
          (new CacheState[A, B](
             data.updated(newKey.value, newKey -> result),
             nextKeys + newKey,
             config
           ),
           CacheResult.Hit(newKey, result)
          )
      }
      .getOrElse(this -> CacheResult.Miss)

  def put(key: Key[A], entry: Option[B]): CacheState[A, B] =
    if (config.ignoreEmptyValues && entry.isEmpty) this
    else
      data.get(key.value) match {
        case Some((curKey, result)) if result != entry =>
          new CacheState[A, B](
            data.updated(key.value, key -> entry),
            (keys - curKey) + key,
            config
          )
        case Some(_) => this
        case None =>
          new CacheState[A, B](
            data.updated(key.value, key -> entry),
            keys + key,
            config
          )
      }

  def put(key: Key[A], entry: B): CacheState[A, B] = put(key, Some(entry))

  def shrink: CacheState[A, B] = {
    val currentSize = keys.size
    val diff        = currentSize - config.clearConfig.maximumSize
    if (diff <= 0) this
    else {
      val (toDrop, rest) = keys.splitAt(diff)
      new CacheState[A, B](
        data.removedAll(toDrop.unsorted.map(_.value)),
        rest,
        config
      )
    }
  }

  override def toString: String = s"CacheState(data=$data, keys=$keys)"
}

object CacheState {

  def create[A, B](config: CacheConfig): CacheState[A, B] =
    new CacheState[A, B](Map.empty, TreeSet.empty(config.evictStrategy.keyOrder), config)
}
