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

package ch.datascience.tokenrepository.repository.association

import cats.effect.{Bracket, IO}
import ch.datascience.db.DbTransactor
import ch.datascience.tokenrepository.repository.{AccessTokenCrypto, ProjectsTokensDB}

import scala.util.Try

private class TryAssociationPersister(
    transactor: DbTransactor[Try, ProjectsTokensDB]
)(implicit ME:  Bracket[Try, Throwable])
    extends AssociationPersister[Try](transactor)

private class IOTokenAssociator(
    accessTokenCrypto:    AccessTokenCrypto[IO],
    associationPersister: AssociationPersister[IO]
) extends TokenAssociator[IO](accessTokenCrypto, associationPersister)
