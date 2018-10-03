package org.novelfs.streaming.kafka

import cats.effect.Effect
import fs2._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

package object ops {
  implicit class ExtraStreamOps[F[_] : Effect, O](s : Stream[F, O]) {

//    def takeElementsEvery(d : FiniteDuration)(implicit ec : ExecutionContext) =
//      Scheduler[F](corePoolSize = 2).flatMap(scheduler =>
//        s.either(scheduler.awakeEvery(d))
//          .zipWithPrevious
//          .filter {
//            case  (Some(Right(_)), Left(_)) => true
//            case _ => false
//          }
//          .map{case (_, x) => x}
//          .collect{case Left(x) => x}
//      )


  }
}