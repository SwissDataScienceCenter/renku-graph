/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import io.renku.core.client.Branch
import io.renku.graph.model.images.ImageUri
import io.renku.triplesgenerator.api.{ProjectUpdates => TGProjectUpdates}

private trait TGUpdatesFinder[F[_]] {
  def findTGProjectUpdates(updates:               ProjectUpdates,
                           maybeGLUpdatedProject: Option[GLUpdatedProject]
  ): F[TGProjectUpdates]
  def findTGProjectUpdates(updates:               ProjectUpdates,
                           maybeGLUpdatedProject: Option[GLUpdatedProject],
                           maybeDefaultBranch:    Option[DefaultBranch],
                           corePushBranch:        Branch
  ): F[TGProjectUpdates]
}

private object TGUpdatesFinder {
  def apply[F[_]: MonadThrow]: TGUpdatesFinder[F] = new TGUpdatesFinderImpl[F]

}
private class TGUpdatesFinderImpl[F[_]: MonadThrow] extends TGUpdatesFinder[F] {

  def findTGProjectUpdates(updates:               ProjectUpdates,
                           maybeGLUpdatedProject: Option[GLUpdatedProject]
  ): F[TGProjectUpdates] =
    maybeGLUpdatedProject match {
      case None                   => new Exception("No info about values updated in GL").raiseError
      case Some(glUpdatedProject) => tgUpdates(updates, glUpdatedProject).map(removeCoreUpdatables)
    }

  override def findTGProjectUpdates(updates:               ProjectUpdates,
                                    maybeGLUpdatedProject: Option[GLUpdatedProject],
                                    maybeDefaultBranch:    Option[DefaultBranch],
                                    corePushBranch:        Branch
  ): F[TGProjectUpdates] = {
    val corePushedToDefaultBranch = maybeDefaultBranch contains DefaultBranch.Unprotected(corePushBranch)

    (updates.glUpdateNeeded, maybeGLUpdatedProject, corePushedToDefaultBranch) match {
      case (true, None, _)                       => new Exception("No info about values updated in GL").raiseError
      case (true, Some(glUpdatedProject), true)  => tgUpdates(updates, glUpdatedProject)
      case (true, Some(glUpdatedProject), false) => tgUpdates(updates, glUpdatedProject).map(removeCoreUpdatables)
      case (false, _, true)                      => tgUpdates(updates).map(removeGLUpdatables)
      case (false, _, false)                     => TGProjectUpdates.empty.pure[F]
    }
  }

  private def findNewImages(updates: ProjectUpdates, glUpdatedProject: GLUpdatedProject): F[Option[List[ImageUri]]] =
    updates.newImage match {
      case None                                            => Option.empty[List[ImageUri]].pure[F]
      case Some(None) if glUpdatedProject.image.nonEmpty   => new Exception("Image not deleted in GL").raiseError
      case Some(Some(_)) if glUpdatedProject.image.isEmpty => new Exception("Image not updated in GL").raiseError
      case _                                               => glUpdatedProject.image.toList.some.pure[F]
    }

  private def tgUpdates(updates: ProjectUpdates, glUpdatedProject: GLUpdatedProject) =
    findNewImages(updates, glUpdatedProject).map { maybeNewImages =>
      TGProjectUpdates(
        newDescription = updates.newDescription,
        newImages = maybeNewImages,
        newKeywords = updates.newKeywords,
        newVisibility = updates.newVisibility
      )
    }

  private def tgUpdates(updates: ProjectUpdates) =
    TGProjectUpdates(
      newDescription = updates.newDescription,
      newImages = None,
      newKeywords = updates.newKeywords,
      newVisibility = None
    ).pure[F]

  private lazy val removeCoreUpdatables: TGProjectUpdates => TGProjectUpdates =
    _.copy(newDescription = None, newKeywords = None)

  private lazy val removeGLUpdatables: TGProjectUpdates => TGProjectUpdates =
    _.copy(newImages = None, newVisibility = None)
}
