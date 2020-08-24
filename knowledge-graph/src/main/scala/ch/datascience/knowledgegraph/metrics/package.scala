/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.knowledgegraph

import cats.implicits._
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}
import io.circe.Decoder
import io.circe.Decoder.decodeString

package object metrics {

  sealed trait KGEntityType extends StringTinyType {
    def rdfType: String
  }

  object KGEntityType extends TinyTypeFactory[KGEntityType](EventStatusInstantiator) {

    val all: Set[KGEntityType] =
      Set(Dataset, Project, ProcessRun, Activity, WorkflowRun)

    final case object Dataset extends KGEntityType {
      override val value: String = "Dataset"
      val rdfType = "http://schema.org/Dataset"
    }
    final case object Project extends KGEntityType {
      override val value: String = "Project"
      val rdfType = "http://schema.org/Project"
    }

    final case object ProcessRun extends KGEntityType {
      override val value: String = "Process run"
      val rdfType = "http://purl.org/wf4ever/wfprov#ProcessRun"
    }

    final case object Activity extends KGEntityType {
      override val value: String = "Activity"
      val rdfType = "http://www.w3.org/ns/prov#Activity"
    }

    final case object WorkflowRun extends KGEntityType {
      override val value: String = "WorkflowRun"
      val rdfType = "http://purl.org/wf4ever/wfprov#WorkflowRun"
    }

    implicit val kgEntityTypeDecoder: Decoder[KGEntityType] = decodeString.emap { value =>
      Either.fromOption(
        KGEntityType.all.find(_.rdfType == value),
        ifNone = s"'$value' unknown KGEntityType"
      )
    }
  }

  private object EventStatusInstantiator extends (String => KGEntityType) {
    override def apply(value: String): KGEntityType = KGEntityType.all.find(_.value == value).getOrElse {
      throw new IllegalArgumentException(s"'$value' unknown KGEntityType")
    }
  }

}
