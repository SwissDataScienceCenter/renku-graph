package io.renku.triplesgenerator.events.categories.cleanup

import io.renku.events.consumers.Project

case class CleanUpEvent(project: Project)
