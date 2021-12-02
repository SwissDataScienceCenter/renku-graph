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

package io.renku.triplesgenerator.events.categories.triplesgenerated.transformation

import cats.MonadThrow
import cats.data.EitherT
import eu.timepit.refined.auto._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.http.client.RestClientError
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.triplesgenerator.events.categories.triplesgenerated.TransformationStep
import io.renku.triplesgenerator.events.categories.triplesgenerated.TransformationStep.Queries
import org.scalacheck.Gen

private[triplesgenerated] object Generators {

  implicit def transformationSteps[F[_]: MonadThrow]: Gen[TransformationStep[F]] = for {
    name    <- nonBlankStrings(minLength = 5)
    queries <- queriesGen
  } yield TransformationStep[F](
    name,
    project => EitherT.rightT((project, queries))
  )

  implicit lazy val queriesGen: Gen[Queries] = for {
    pre  <- sparqlQueries.toGeneratorOfList()
    post <- sparqlQueries.toGeneratorOfList()
  } yield Queries(pre, post)

  lazy val recoverableClientErrors: Gen[RestClientError] =
    Gen.oneOf(clientExceptions, connectivityExceptions, unexpectedResponseExceptions, Gen.const(UnauthorizedException))
}
