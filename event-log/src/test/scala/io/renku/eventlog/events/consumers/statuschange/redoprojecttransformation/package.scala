package io.renku.eventlog.events.consumers.statuschange

import io.renku.graph.model.GraphModelGenerators.projectPaths

package object redoprojecttransformation {
  lazy val redoProjectTransformationEvents = projectPaths.map(RedoProjectTransformation(_))
}
