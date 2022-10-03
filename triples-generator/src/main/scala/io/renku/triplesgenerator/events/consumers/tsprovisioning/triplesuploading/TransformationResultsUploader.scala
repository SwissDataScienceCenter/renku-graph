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
import io.renku.graph.model._
import io.renku.graph.model.entities.{EntityFunctions, Person, Project}
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._
import io.renku.triplesgenerator.events.consumers.ProcessingRecoverableError
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger

private trait TransformationResultsUploader[F[_]] {
  def execute(query:  SparqlQuery): EitherT[F, ProcessingRecoverableError, Unit]
  def upload(project: Project):     EitherT[F, ProcessingRecoverableError, Unit]
}

private object TransformationResultsUploader {

  trait Locator[F[_]] {
    def apply(tsVersion: TSVersion): TransformationResultsUploader[F]
  }

  private class LocatorImpl[F[_]](defaultGraphResultsUploader: TransformationResultsUploader[F],
                                  namedGraphsResultsUploader:  TransformationResultsUploader[F]
  ) extends Locator[F] {

    override def apply(tsVersion: TSVersion): TransformationResultsUploader[F] = tsVersion match {
      case TSVersion.DefaultGraph => defaultGraphResultsUploader
      case TSVersion.NamedGraphs  => namedGraphsResultsUploader
    }
  }

  def apply[F[_]: Async: SparqlQueryTimeRecorder: Logger]: F[TransformationResultsUploader.Locator[F]] = for {
    implicit0(renkuUrl: RenkuUrl)     <- RenkuUrlLoader[F]()
    implicit0(glApiUrl: GitLabApiUrl) <- GitLabUrlLoader[F]().map(_.apiV4)
    renkuConnectionConfig             <- RenkuConnectionConfig[F]()
    projectsConnectionConfig          <- ProjectsConnectionConfig[F]()
  } yield new LocatorImpl(
    new DefaultGraphResultsUploader[F](new JsonLDUploaderImpl[F](renkuConnectionConfig),
                                       new UpdateQueryRunnerImpl(renkuConnectionConfig)
    ),
    new NamedGraphsResultsUploader[F](new JsonLDUploaderImpl[F](projectsConnectionConfig),
                                      new UpdateQueryRunnerImpl(projectsConnectionConfig)
    )
  )
}

private class DefaultGraphResultsUploader[F[_]: MonadThrow](jsonLDUploader: JsonLDUploader[F],
                                                            updateQueryRunner: UpdateQueryRunner[F]
)(implicit renkuUrl:                                                           RenkuUrl, gitLabUrl: GitLabApiUrl)
    extends TransformationResultsUploader[F] {

  private implicit val graph: GraphClass = GraphClass.Default
  import jsonLDUploader._

  override def execute(query: SparqlQuery) = updateQueryRunner run query

  override def upload(project: Project) = encode(project) >>= uploadJsonLD

  private def encode(project: Project): EitherT[F, ProcessingRecoverableError, JsonLD] = EitherT
    .fromEither[F](project.asJsonLD.flatten)
    .leftSemiflatMap(error =>
      NonRecoverableFailure(s"Encoding '${project.path}' failed", error).raiseError[F, ProcessingRecoverableError]
    )
}

private class NamedGraphsResultsUploader[F[_]: MonadThrow](jsonLDUploader: JsonLDUploader[F],
                                                           updateQueryRunner: UpdateQueryRunner[F]
)(implicit renkuUrl:                                                          RenkuUrl, gitLabUrl: GitLabApiUrl)
    extends TransformationResultsUploader[F] {

  import Schemas.schema
  import io.renku.jsonld.{JsonLDEncoder, NamedGraph}
  import jsonLDUploader._

  override def execute(query: SparqlQuery) = updateQueryRunner run query

  override def upload(project: Project): EitherT[F, ProcessingRecoverableError, Unit] =
    encode(project).flatMap(_.map(uploadJsonLD).sequence.void)

  private def encode(project: Project): EitherT[F, ProcessingRecoverableError, List[NamedGraph]] = EitherT.right {
    (projectGraph(project) -> maybePersonsGraph(project))
      .mapN(_ :: _.toList)
      .flatMap(_.map(_.flatten).sequence)
      .fold(
        error => NonRecoverableFailure(s"Encoding '${project.path}' failed", error).raiseError[F, List[NamedGraph]],
        _.pure[F]
      )
  }

  private def projectGraph(project: Project) = {
    implicit val encoder: JsonLDEncoder[Project] = EntityFunctions[Project].encoder(GraphClass.Project)
    NamedGraph.from(project.resourceId.asEntityId, project.asJsonLD)
  }

  private def maybePersonsGraph(project: Project) =
    EntityFunctions[Project].findAllPersons(project).toList match {
      case Nil => Option.empty[NamedGraph].asRight
      case h :: t =>
        implicit val encoder: JsonLDEncoder[Person] = EntityFunctions[Person].encoder(GraphClass.Persons)
        NamedGraph.from(schema / "Person", h.asJsonLD, t.map(_.asJsonLD): _*).map(Option.apply)
    }
}
