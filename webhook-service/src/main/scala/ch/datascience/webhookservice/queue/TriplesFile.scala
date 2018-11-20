package ch.datascience.webhookservice.queue

import java.nio.file.Path

import ch.datascience.tinytypes.TinyType

case class TriplesFile(value: Path) extends TinyType[Path]
