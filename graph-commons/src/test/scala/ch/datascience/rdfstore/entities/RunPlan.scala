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

package ch.datascience.rdfstore.entities

import ch.datascience.rdfstore.entities.CommandParameterBase.{CommandInput, CommandOutput, CommandParameter, Position}
import ch.datascience.rdfstore.entities.RunPlan._
import ch.datascience.tinytypes._
import ch.datascience.tinytypes.constraints._

final case class RunPlan(id:                        Id,
                         name:                      Name,
                         maybeDescription:          Option[Description],
                         command:                   Command,
                         maybeProgrammingLanguage:  Option[ProgrammingLanguage],
                         keywords:                  List[Keyword],
                         commandParameterFactories: List[RunPlan => CommandParameterBase],
                         successCodes:              List[SuccessCode],
                         maybeInvalidationTime:     Option[InvalidationTime]
) {
  private lazy val commandParameters: List[CommandParameterBase] = commandParameterFactories.map(_.apply(this))
  lazy val parameters:                List[CommandParameter]     = commandParameters.collect { case param: CommandParameter => param }
  lazy val inputs:                    List[CommandInput]         = commandParameters.collect { case in: CommandInput => in }
  lazy val outputs:                   List[CommandOutput]        = commandParameters.collect { case out: CommandOutput => out }
}

object RunPlan {

  import ch.datascience.graph.config.RenkuBaseUrl
  import io.renku.jsonld._
  import JsonLDEncoder._
  import io.renku.jsonld.syntax._

  def apply(
      name:                      Name,
      command:                   Command,
      commandParameterFactories: List[Position => RunPlan => CommandParameterBase]
  ): RunPlan = RunPlan(
    Id.generate,
    name,
    maybeDescription = None,
    command,
    maybeProgrammingLanguage = None,
    keywords = Nil,
    commandParameterFactories = commandParameterFactories.zipWithIndex.map { case (factory, idx) =>
      factory(Position(idx + 1))
    },
    successCodes = Nil,
    maybeInvalidationTime = None
  )

  object CommandParameters {
    def of(
        parameters: (Position => RunPlan => CommandParameterBase)*
    ): List[Position => RunPlan => CommandParameterBase] =
      parameters.toList
  }

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[RunPlan] = JsonLDEncoder.instance { plan =>
    JsonLD.entity(
      plan.asEntityId,
      EntityTypes.of(prov / "Plan", renku / "Run"),
      schema / "name"                -> plan.name.asJsonLD,
      schema / "description"         -> plan.maybeDescription.asJsonLD,
      renku / "command"              -> plan.command.asJsonLD,
      schema / "programmingLanguage" -> plan.maybeProgrammingLanguage.asJsonLD,
      schema / "keywords"            -> plan.keywords.asJsonLD,
      renku / "hasArguments"         -> plan.parameters.asJsonLD,
      renku / "hasInputs"            -> plan.inputs.asJsonLD,
      renku / "hasOutputs"           -> plan.outputs.asJsonLD,
      renku / "successCodes"         -> plan.successCodes.asJsonLD,
      prov / "invalidatedAtTime"     -> plan.maybeInvalidationTime.asJsonLD
    )
  }

  implicit def entityIdEncoder(implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[RunPlan] =
    EntityIdEncoder.instance(plan => EntityId of renkuBaseUrl / "plans" / plan.id)

  final class Id private (val value: String) extends AnyVal with StringTinyType
  implicit object Id extends TinyTypeFactory[Id](new Id(_)) with UUID {

    def generate: Id = Id {
      java.util.UUID.randomUUID.toString
    }
  }

  final class Name private (val value: String) extends AnyVal with StringTinyType
  implicit object Name extends TinyTypeFactory[Name](new Name(_)) with NonBlank

  final class Description private (val value: String) extends AnyVal with StringTinyType
  implicit object Description extends TinyTypeFactory[Description](new Description(_)) with NonBlank

  final class Command private (val value: String) extends AnyVal with StringTinyType
  implicit object Command extends TinyTypeFactory[Command](new Command(_)) with NonBlank

  final class Keyword private (val value: String) extends AnyVal with StringTinyType
  implicit object Keyword extends TinyTypeFactory[Keyword](new Keyword(_)) with NonBlank

  final class ProgrammingLanguage private (val value: String) extends AnyVal with StringTinyType
  implicit object ProgrammingLanguage
      extends TinyTypeFactory[ProgrammingLanguage](new ProgrammingLanguage(_))
      with NonBlank

  final class SuccessCode private (val value: Int) extends AnyVal with IntTinyType
  implicit object SuccessCode extends TinyTypeFactory[SuccessCode](new SuccessCode(_)) with NonNegativeInt
}
