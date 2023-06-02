package io.renku.microservices

import cats.effect.unsafe.{IORuntime, IORuntimeConfig}

import java.nio.channels.ClosedChannelException
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext

object SuppressChannelClosedExceptionRuntime {
  // this code is copied from IOApp, only to change createDefaultBlockingExecutionContext to pass the error reporter
  def createRuntime(
      workerThreadCount:             Int,
      blockedThreadDetectionEnabled: Boolean,
      reportFailure:                 Throwable => Unit,
      runtimeConfig:                 IORuntimeConfig
  ) = {
    val (compute, compDown) =
      IORuntime.createWorkStealingComputeThreadPool(
        threads = workerThreadCount,
        reportFailure = muteClosedChannelException0(reportFailure),
        blockedThreadDetectionEnabled = blockedThreadDetectionEnabled
      )

    val (blocking, blockDown) =
      createDefaultBlockingExecutionContext(reportFailure) // <- line changed to our method

    IORuntime(
      compute,
      blocking,
      compute,
      { () =>
        compDown()
        blockDown()
      // IORuntime.resetGlobal()
      },
      runtimeConfig
    )
  }

  // this is a copy from IORuntimeCompanionPlatform#createDefaultBlockingExecutionContext that additionally passes the reportError function to the execution context
  private def createDefaultBlockingExecutionContext(
      reportFailure: Throwable => Unit,
      threadPrefix:  String = "io-blocking"
  ): (ExecutionContext, () => Unit) = {
    val threadCount = new AtomicInteger(0)
    val executor = Executors.newCachedThreadPool { (r: Runnable) =>
      val t = new Thread(r)
      t.setName(s"${threadPrefix}-${threadCount.getAndIncrement()}")
      t.setDaemon(true)
      t
    }
    (
      ExecutionContext.fromExecutor(executor, muteClosedChannelException0(reportFailure)),
      { () => executor.shutdown() }
    )
  }

  private def muteClosedChannelException0(fallback: Throwable => Unit)(err: Throwable): Unit =
    err match {
      case _: ClosedChannelException => () // ignore this here
      case _ => fallback(err)
    }

  val runtime: IORuntime =
    createRuntime(
      workerThreadCount = Math.max(2, Runtime.getRuntime.availableProcessors()),
      blockedThreadDetectionEnabled = false,
      reportFailure = _.printStackTrace(),
      IORuntimeConfig()
    )
}
