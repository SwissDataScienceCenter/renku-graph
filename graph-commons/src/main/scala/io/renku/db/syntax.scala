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

package io.renku.db

import cats.Applicative
import cats.data.Kleisli
import skunk.Session

object syntax extends implicits {

  type QueryDef[F[_], R] = Kleisli[F, Session[F], R]
  type CommandDef[F[_]]  = Kleisli[F, Session[F], Unit]

  object CommandDef {
    def apply[F[_]](f: Session[F] => F[Unit]): CommandDef[F] =
      Kleisli(f)

    def liftF[F[_]: Applicative](result: F[Unit]): CommandDef[F] =
      Kleisli.liftF[F, Session[F], Unit](result)

    def pure[F[_]: Applicative]: CommandDef[F] =
      Kleisli.pure[F, Session[F], Unit](())
  }
}
