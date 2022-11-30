/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.tokenrepository.repository.refresh

import io.renku.tinytypes.constraints.NonNegativeInt
import io.renku.tinytypes.{IntTinyType, TinyTypeFactory}
import io.renku.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import io.renku.tokenrepository.repository.association.TokenStoringInfo.Project

private final case class TokenCloseExpiration(project: Project, encryptedToken: EncryptedAccessToken)

private final class TokenId private (val value: Int) extends AnyVal with IntTinyType
private object TokenId extends TinyTypeFactory[TokenId](new TokenId(_)) with NonNegativeInt[TokenId]

private def aboutToExpireDate = 