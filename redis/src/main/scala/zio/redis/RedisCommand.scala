package zio.redis

import zio.ZIO
import zio.redis.Output.StringOutput
import zio.stream.ZStream

sealed trait RedisCommandLike[-In] {
  def name: String
  def input: Input[In]
}

final class RedisCommand[-In, +Out] private (val name: String, val input: Input[In], val output: Output[Out]) extends RedisCommandLike[In] {
  private[redis] def run(in: In): ZIO[RedisExecutor, RedisError, Out] =
    ZIO
      .accessM[RedisExecutor](_.get.execute(Input.StringInput.encode(name) ++ input.encode(in)))
      .flatMap(out => ZIO.effect(output.unsafeDecode(out)))
      .refineToOrDie[RedisError]
}

object RedisCommand {
  private[redis] def apply[In, Out](name: String, input: Input[In], output: Output[Out]): RedisCommand[In, Out] =
    new RedisCommand(name, input, output)
}

final class RedisSubCommand[-In] private (val name: String, val input: Input[In]) extends RedisCommandLike[In] {
  val output = StringOutput
  private[redis] def stream(in: In): ZStream[RedisExecutor, RedisError, String] = { // todo
    ZStream.accessStream[RedisExecutor](_.get.subscribe(Input.StringInput.encode(name), input.encode(in)))
      .flatMap(out => ZStream.fromEffect(ZIO.effect(output.unsafeDecode(out))))
      .refineToOrDie[RedisError]
  }
}

object RedisSubCommand {
  private[redis] def apply[In, Out](name: String, input: Input[In]): RedisSubCommand[In] =
    new RedisSubCommand(name, input)
}
