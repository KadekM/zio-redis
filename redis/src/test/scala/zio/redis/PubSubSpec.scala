package zio.redis

import zio.ZIO
import zio.test._

trait PubSubSpec extends BaseSpec {

  val pubSubSuite: Spec[RedisExecutor, TestFailure[RedisError], TestSuccess] =
    suite("pubSub")(
      suite("subscribe")(
        testM("subscribe follow by multiple values") {
          val channel   = "channel_1"
          val msg       = RespValue.SimpleString("msg_1")

          for {
            _ <- subscribe(channel).tap(x => ZIO.effectTotal(println(x))).runDrain.fork
            _ = println("publishing")
            _   <- publish(channel, msg)
            _ = println("published")
          } yield assertCompletes
        }
      )
    )
}
