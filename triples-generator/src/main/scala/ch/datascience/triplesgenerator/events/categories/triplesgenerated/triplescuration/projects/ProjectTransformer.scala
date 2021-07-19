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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.projects

import cats.data.EitherT
import cats.effect._
import cats.syntax.all._
import ch.datascience.http.client.RestClientError.{ClientException, ConnectivityException, UnauthorizedException, UnexpectedResponseException}
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TransformationStep
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TransformationStep.{ResultData, Transformation}
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.TriplesCurator.TransformationRecoverableError
import eu.timepit.refined.auto._
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

trait ProjectTransformer[Interpretation[_]] {
  def createTransformationStep: TransformationStep[Interpretation]
}

class ProjectTransformerImpl[Interpretation[_]: MonadThrow](
    kGProjectFinder: KGProjectFinder[Interpretation],
    updatesCreator:  UpdatesCreator
) extends ProjectTransformer[Interpretation] {

  override def createTransformationStep: TransformationStep[Interpretation] =
    TransformationStep("Project Details Updates", createTransformation)

  private def createTransformation: Transformation[Interpretation] = projectMetadata =>
    EitherT {
      kGProjectFinder
        .find(projectMetadata.project.resourceId)
        .map {
          case None =>
            TransformationStep.ResultData(projectMetadata, Nil).asRight[ProcessingRecoverableError]
          case Some(kgProjectInfo) =>
            TransformationStep
              .ResultData(projectMetadata, updatesCreator.prepareUpdates(projectMetadata.project, kgProjectInfo))
              .asRight[ProcessingRecoverableError]
        }
        .recoverWith(maybeToRecoverableError)
    }

  private lazy val maybeToRecoverableError
      : PartialFunction[Throwable, Interpretation[Either[ProcessingRecoverableError, ResultData]]] = {
    case e @ (_: UnexpectedResponseException | _: ConnectivityException | _: ClientException |
        _: UnauthorizedException) =>
      TransformationRecoverableError("Problem finding project details in KG", e)
        .asLeft[ResultData]
        .leftWiden[ProcessingRecoverableError]
        .pure[Interpretation]
  }
}

object ProjectTransformer {
  def apply(
      timeRecorder:            SparqlQueryTimeRecorder[IO],
      logger:                  Logger[IO]
  )(implicit executionContext: ExecutionContext, cs: ContextShift[IO], timer: Timer[IO]): IO[ProjectTransformer[IO]] =
    for {
      kgProjectFinder <- KGProjectFinder(timeRecorder, logger)
    } yield new ProjectTransformerImpl[IO](kgProjectFinder, UpdatesCreator)
}
