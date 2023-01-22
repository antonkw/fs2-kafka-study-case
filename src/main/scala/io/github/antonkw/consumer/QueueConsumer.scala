package io.github.antonkw.consumer

//import java.time.Instant
//
//
//
//
//trait QueueConsumer[F[_], I, O] {
//  def process(f: (EventMeta, I) => F[Either[Error, O]]): F[RunningProcess[F]]
//}