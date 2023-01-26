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

package io.renku.cli.model

import io.renku.jsonld.{JsonLD, JsonLDEncoder}

/** Marker trait to annotate a class that models an entity from the cli schema. */
trait CliModel extends Product

object CliModel {

  final implicit class CliModelOps[A <: CliModel](self: A) {

    /**
     * Creates a JSON-LD representation of the value that is flattened into an array.
     * It is generally safe to flatten a jsonld value that was generated from a cli model class.
     */
    def asFlattenedJsonLD(implicit encoder: JsonLDEncoder[A]): JsonLD =
      encoder(self).flatten.fold(throw _, identity)

    /** Creates a nested JSON-LD representation of the value. */
    def asNestedJsonLD(implicit encoder: JsonLDEncoder[A]): JsonLD =
      encoder(self)
  }
}
