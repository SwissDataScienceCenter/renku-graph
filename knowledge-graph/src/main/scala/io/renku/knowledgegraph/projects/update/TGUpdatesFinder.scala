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

package io.renku.knowledgegraph.projects.update

import cats.MonadThrow
import cats.syntax.all._
import io.renku.graph.model.images.ImageUri
import io.renku.triplesgenerator.api.{ProjectUpdates => TGProjectUpdates}

private trait TGUpdatesFinder[F[_]] {
  def findTGProjectUpdates(updates:               ProjectUpdates,
                           maybeGLUpdatedProject: Option[GLUpdatedProject]
  ): F[TGProjectUpdates]
}

private object TGUpdatesFinder {
  def apply[F[_]: MonadThrow]: TGUpdatesFinder[F] = new TGUpdatesFinderImpl[F]

}
private class TGUpdatesFinderImpl[F[_]: MonadThrow] extends TGUpdatesFinder[F] {

  override def findTGProjectUpdates(updates:               ProjectUpdates,
                                    maybeGLUpdatedProject: Option[GLUpdatedProject]
  ): F[TGProjectUpdates] =
    findNewImages(updates, maybeGLUpdatedProject).map(maybeNewImages =>
      TGProjectUpdates(
        newDescription = updates.newDescription,
        newImages = maybeNewImages,
        newKeywords = updates.newKeywords,
        newVisibility = updates.newVisibility
      )
    )

  private def findNewImages(updates:               ProjectUpdates,
                            maybeGLUpdatedProject: Option[GLUpdatedProject]
  ): F[Option[List[ImageUri]]] =
    updates.newImage match {
      case None =>
        Option.empty[List[ImageUri]].pure[F]
      case Some(_) if maybeGLUpdatedProject.isEmpty =>
        new Exception("No info about updated values in GL").raiseError
      case Some(None) if maybeGLUpdatedProject.flatMap(_.image).nonEmpty =>
        new Exception("Image not deleted in GL").raiseError
      case Some(Some(_)) if maybeGLUpdatedProject.flatMap(_.image).isEmpty =>
        new Exception("Image not updated in GL").raiseError
      case _ =>
        maybeGLUpdatedProject.map(_.image.toList).pure[F]
    }
}
