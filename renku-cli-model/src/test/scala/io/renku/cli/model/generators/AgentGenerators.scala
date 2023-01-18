package io.renku.cli.model.generators

import io.renku.cli.model.CliAgent
import io.renku.graph.model.RenkuTinyTypeGenerators
import org.scalacheck.Gen

trait AgentGenerators {

  def agentGen: Gen[CliAgent] =
    for {
      id   <- RenkuTinyTypeGenerators.agentResourceIdGen
      name <- RenkuTinyTypeGenerators.agentNameGen
    } yield CliAgent(id, name)
}

object AgentGenerators extends AgentGenerators
