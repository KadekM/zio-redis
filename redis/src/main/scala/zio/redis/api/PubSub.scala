package zio.redis.api

import zio.ZIO
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.stream.ZStream

trait PubSub {
  import PubSub._
  final def publish(channel: String, msg: String): ZIO[RedisExecutor, RedisError, Long] = Publish.run((channel, msg))

  final def subscribe(channel: String, channels: String*): ZStream[RedisExecutor, RedisError, String] =
    Subscribe.stream((channel, channels.toList))

}

object PubSub {
  final val Publish        = RedisCommand("PUBLISH", Tuple2(StringInput, StringInput), LongOutput)
  final val Subscribe      = RedisCommand("SUBSCRIBE", NonEmptyList(StringInput), StringOutput)
}