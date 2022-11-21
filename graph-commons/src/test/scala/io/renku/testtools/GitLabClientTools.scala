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

package io.renku.testtools

import cats.Applicative
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.circe.Json
import io.renku.generators.Generators.Implicits._
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import org.http4s.Method.{DELETE, GET, HEAD, POST}
import org.http4s.{Method, Uri}
import org.scalacheck.Gen
import org.scalamock.matchers.ArgCapture.CaptureOne
import org.scalamock.scalatest.MockFactory

trait GitLabClientTools[F[_]] {
  self: MockFactory =>

  def captureMapping[FinderType, ResultType, A](finder: FinderType, gitLabClient: GitLabClient[F])(
      findingMethod:                                    FinderType => A,
      resultGenerator:                                  Gen[ResultType],
      method:                                           Method = GET,
      expectedNumberOfCalls:                            Int = 1
  )(implicit applicative:                               Applicative[F]): ResponseMappingF[F, ResultType] = {
    val responseMapping = CaptureOne[ResponseMappingF[F, ResultType]]()

    method match {
      case GET =>
        (gitLabClient
          .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[F, ResultType])(
            _: Option[AccessToken]
          ))
          .expects(*, *, capture(responseMapping), *)
          .returning(resultGenerator.generateOne.pure[F])
          .repeat(expectedNumberOfCalls)
      case HEAD =>
        (gitLabClient
          .head(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[F, ResultType])(
            _: Option[AccessToken]
          ))
          .expects(*, *, capture(responseMapping), *)
          .returning(resultGenerator.generateOne.pure[F])
          .repeat(expectedNumberOfCalls)
      case POST =>
        (gitLabClient
          .post(_: Uri, _: String Refined NonEmpty, _: Json)(_: ResponseMappingF[F, ResultType])(
            _: Option[AccessToken]
          ))
          .expects(*, *, *, capture(responseMapping), *)
          .returning(resultGenerator.generateOne.pure[F])
          .repeat(expectedNumberOfCalls)
      case DELETE =>
        (gitLabClient
          .delete(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[F, ResultType])(
            _: Option[AccessToken]
          ))
          .expects(*, *, capture(responseMapping), *)
          .returning(resultGenerator.generateOne.pure[F])
          .repeat(expectedNumberOfCalls)
    }

    findingMethod(finder)
    responseMapping.value
  }
}
