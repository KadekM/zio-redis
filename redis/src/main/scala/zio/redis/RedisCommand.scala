package zio.redis

import zio.ZIO
import zio.stream.ZStream

final class RedisCommand[-In, +Out] private (val name: String, val input: Input[In], val output: Output[Out]) {
  private[redis] def run(in: In): ZIO[RedisExecutor, RedisError, Out] =
    ZIO
      .accessM[RedisExecutor](_.get.execute(Input.StringInput.encode(name) ++ input.encode(in)))
      .flatMap(out => ZIO.effect(output.unsafeDecode(out)))
      .refineToOrDie[RedisError]

  private[redis] def stream(in: In): ZStream[RedisExecutor, RedisError, Out] = {
    ZStream.accessStream[RedisExecutor](_.get.stream(Input.StringInput.encode(name) ++ input.encode(in)))
      .flatMap(out => ZStream.fromEffect(ZIO.effect(output.unsafeDecode(out))))
      .refineToOrDie[RedisError]
  }
}

object RedisCommand {
  private[redis] def apply[In, Out](name: String, input: Input[In], output: Output[Out]): RedisCommand[In, Out] =
    new RedisCommand(name, input, output)
}
