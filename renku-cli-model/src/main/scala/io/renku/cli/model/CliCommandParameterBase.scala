package io.renku.cli.model

import io.renku.graph.model.commandParameters._

trait CliCommandParameterBase {

  def resourceId: ResourceId

  def name: Name

  def description: Option[Description]

  def prefix: Option[Prefix]

  def position: Option[Position]

  def defaultValue: ParameterDefaultValue

}
