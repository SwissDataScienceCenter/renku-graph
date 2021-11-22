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
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.entities.Project
import io.renku.http.client.RestClientError._
import io.renku.rdfstore.SparqlQueryTimeRecorder
import io.renku.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.TransformationStep.{Queries, Transformation}
import io.renku.triplesgenerator.events.categories.triplesgenerated.triplescuration.TriplesCurator.TransformationRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.{ProjectFunctions, TransformationStep}
import org.typelevel.log4cats.Logger

private[triplescuration] trait PersonTransformer[F[_]] {
  def createTransformationStep: TransformationStep[F]
}

private class PersonTransformerImpl[F[_]: MonadThrow](
    kgPersonFinder:   KGPersonFinder[F],
    personMerger:     PersonMerger,
    updatesCreator:   UpdatesCreator,
    projectFunctions: ProjectFunctions
) extends PersonTransformer[F] {

  import projectFunctions._

  override def createTransformationStep: TransformationStep[F] =
    TransformationStep("Person Details Updates", createTransformation)

  private def createTransformation: Transformation[F] = project =>
    EitherT {
      findAllPersons(project)
        .foldLeft((project, Queries.empty).pure[F]) { (previousResultsF, person) =>
          for {
            previousResults   <- previousResultsF
            maybeKGPerson     <- kgPersonFinder find person
            maybeMergedPerson <- maybeKGPerson.map(personMerger.merge(person, _)).sequence
          } yield (maybeKGPerson, maybeMergedPerson)
            .mapN { (kgPerson, mergedPerson) =>
              val updatedProject = update(person, mergedPerson)(previousResults._1)
              val queries        = updatesCreator.prepareUpdates(kgPerson, mergedPerson)
              (updatedProject, previousResults._2 |+| Queries.preDataQueriesOnly(queries))
            }
            .getOrElse(previousResults)
        }
        .map(_.asRight[ProcessingRecoverableError])
        .recoverWith(maybeToRecoverableError)
    }

  private lazy val maybeToRecoverableError
      : PartialFunction[Throwable, F[Either[ProcessingRecoverableError, (Project, Queries)]]] = {
    case e @ (_: UnexpectedResponseException | _: ConnectivityException | _: ClientException |
        _: UnauthorizedException) =>
      TransformationRecoverableError("Problem finding person details in KG", e)
        .asLeft[(Project, Queries)]
        .leftWiden[ProcessingRecoverableError]
        .pure[F]
  }
}

private[triplescuration] object PersonTransformer {

  def apply[F[_]: Async: Logger](timeRecorder: SparqlQueryTimeRecorder[F]): F[PersonTransformer[F]] = for {
    kgPersonFinder <- KGPersonFinder(timeRecorder)
  } yield new PersonTransformerImpl[F](kgPersonFinder, PersonMerger, UpdatesCreator, ProjectFunctions)
}
