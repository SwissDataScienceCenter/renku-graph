package ch.datascience.triplesgenerator.eventprocessing

import cats.effect.ExitCode

import scala.language.higherKinds

private class EventsSource[Interpretation[_]](
    newRunner: (String => Interpretation[Unit]) => EventProcessorRunner[Interpretation]
) {

  def withEventsProcessor(eventProcessor: String => Interpretation[Unit]): EventProcessorRunner[Interpretation] =
    newRunner(eventProcessor)

}

private abstract class EventProcessorRunner[Interpretation[_]](eventProcessor: String => Interpretation[Unit]) {
  def run: Interpretation[ExitCode]
}
