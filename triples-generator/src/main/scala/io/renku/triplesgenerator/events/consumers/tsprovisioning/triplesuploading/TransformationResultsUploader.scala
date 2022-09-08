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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.triplesuploading

import TriplesUploadResult.NonRecoverableFailure
import cats.MonadThrow
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.config.{GitLabUrlLoader, RenkuUrlLoader}
import io.renku.graph.model.entities.Project
import io.renku.graph.model.{GitLabApiUrl, RenkuUrl}
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._
import io.renku.triplesgenerator.events.consumers.{ProcessingRecoverableError, TSVersion}
import io.renku.triplesstore.{RenkuConnectionConfig, SparqlQuery, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

private trait TransformationResultsUploader[F[_], TS <: TSVersion] {
  def execute(query:  SparqlQuery): EitherT[F, ProcessingRecoverableError, Unit]
  def upload(project: Project):     EitherT[F, ProcessingRecoverableError, Unit]
}

private object TransformationResultsUploader {

  trait Locator[F[_]] {
    def apply[TS <: TSVersion](implicit ev: TS): TransformationResultsUploader[F, TS]
  }

  private class LocatorImpl[F[_]](defaultGraphResultsUploader: TransformationResultsUploader[F, TSVersion.DefaultGraph])
      extends Locator[F] {

    override def apply[TS <: TSVersion](implicit ev: TS) = ev match {
      case TSVersion.DefaultGraph => defaultGraphResultsUploader.asInstanceOf[TransformationResultsUploader[F, TS]]
      case TSVersion.NamedGraphs  => ???
    }
  }

  def apply[F[_]: Async: SparqlQueryTimeRecorder: Logger]: F[TransformationResultsUploader.Locator[F]] = for {
    implicit0(renkuUrl: RenkuUrl)     <- RenkuUrlLoader[F]()
    implicit0(glApiUrl: GitLabApiUrl) <- GitLabUrlLoader[F]().map(_.apiV4)
    renkuConnectionConfig             <- RenkuConnectionConfig[F]()
  } yield new LocatorImpl(
    new DefaultGraphResultsUploader[F](new ProjectUploaderImpl[F](renkuConnectionConfig),
                                       new UpdateQueryRunnerImpl(renkuConnectionConfig)
    )
  )
}

private class DefaultGraphResultsUploader[F[_]: MonadThrow](projectUploader: ProjectUploader[F],
                                                            updateQueryRunner: UpdateQueryRunner[F]
)(implicit renkuUrl:                                                           RenkuUrl, gitLabUrl: GitLabApiUrl)
    extends TransformationResultsUploader[F, TSVersion.DefaultGraph] {

  override def execute(query: SparqlQuery) = updateQueryRunner run query

  override def upload(project: Project) = encode(project) >>= projectUploader.uploadProject

  private def encode(project: Project): EitherT[F, ProcessingRecoverableError, JsonLD] = EitherT
    .fromEither[F](project.asJsonLD.flatten)
    .leftSemiflatMap(error =>
      NonRecoverableFailure(s"Metadata for project ${project.path} failed: ${error.getMessage}", error)
        .raiseError[F, ProcessingRecoverableError]
    )
}
