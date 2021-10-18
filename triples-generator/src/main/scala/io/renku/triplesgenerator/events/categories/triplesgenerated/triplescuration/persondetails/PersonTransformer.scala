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

package io.renku.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.{ContextShift, Timer}
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.http.client.RestClientError._
import io.renku.rdfstore.SparqlQueryTimeRecorder
import io.renku.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.TransformationStep.{ResultData, Transformation}
import io.renku.triplesgenerator.events.categories.triplesgenerated.triplescuration.TriplesCurator.TransformationRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.{ProjectFunctions, TransformationStep}
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private[triplescuration] trait PersonTransformer[Interpretation[_]] {
  def createTransformationStep: TransformationStep[Interpretation]
}

private class PersonTransformerImpl[Interpretation[_]: MonadThrow](
    kgPersonFinder:   KGPersonFinder[Interpretation],
    personMerger:     PersonMerger,
    updatesCreator:   UpdatesCreator,
    projectFunctions: ProjectFunctions
) extends PersonTransformer[Interpretation] {

  import projectFunctions._

  override def createTransformationStep: TransformationStep[Interpretation] =
    TransformationStep("Person Details Updates", createTransformation)

  private def createTransformation: Transformation[Interpretation] = project =>
    EitherT {
      findAllPersons(project)
        .foldLeft(ResultData(project, List.empty).pure[Interpretation]) { (previousResultsF, person) =>
          for {
            previousResults   <- previousResultsF
            maybeKGPerson     <- kgPersonFinder find person
            maybeMergedPerson <- maybeKGPerson.map(personMerger.merge(person, _)).sequence
          } yield (maybeKGPerson, maybeMergedPerson)
            .mapN { (kgPerson, mergedPerson) =>
              val updatedProjectMetadata = update(person, mergedPerson)(previousResults.project)
              val queries                = updatesCreator.prepareUpdates(kgPerson, mergedPerson)
              ResultData(updatedProjectMetadata, previousResults.queries ::: queries)
            }
            .getOrElse(previousResults)
        }
        .map(_.asRight[ProcessingRecoverableError])
        .recoverWith(maybeToRecoverableError)
    }

  private lazy val maybeToRecoverableError
      : PartialFunction[Throwable, Interpretation[Either[ProcessingRecoverableError, ResultData]]] = {
    case e @ (_: UnexpectedResponseException | _: ConnectivityException | _: ClientException |
        _: UnauthorizedException) =>
      TransformationRecoverableError("Problem finding person details in KG", e)
        .asLeft[ResultData]
        .leftWiden[ProcessingRecoverableError]
        .pure[Interpretation]
  }
}

private[triplescuration] object PersonTransformer {

  import cats.effect.IO

  def apply(
      timeRecorder: SparqlQueryTimeRecorder[IO],
      logger:       Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[PersonTransformer[IO]] = for {
    kgPersonFinder <- KGPersonFinder(logger, timeRecorder)
  } yield new PersonTransformerImpl[IO](kgPersonFinder, PersonMerger, UpdatesCreator, ProjectFunctions)
}
