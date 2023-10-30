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

package io.renku.triplesgenerator.tsprovisioning

import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.RenkuUrl
import io.renku.graph.model.entities.Project
import io.renku.triplesstore.{ProjectSparqlClient, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger
import transformation.TransformationStepsCreator
import triplesuploading.{TransformationStepsRunner, TriplesUploadResult}

trait TSProvisioner[F[_]] {
  def provisionTS(project: Project): F[TriplesUploadResult]
}

object TSProvisioner {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      projectSparqlClient: ProjectSparqlClient[F]
  )(implicit renkuUrl: RenkuUrl): F[TSProvisioner[F]] =
    (TransformationStepsCreator[F], TransformationStepsRunner[F](projectSparqlClient))
      .mapN(new TSProvisionerImpl[F](_, _))
}

private class TSProvisionerImpl[F[_]](stepsCreator: TransformationStepsCreator[F],
                                      stepsRunner:  TransformationStepsRunner[F]
) extends TSProvisioner[F] {

  override def provisionTS(project: Project): F[TriplesUploadResult] =
    stepsRunner.run(stepsCreator.createSteps, project)
}
