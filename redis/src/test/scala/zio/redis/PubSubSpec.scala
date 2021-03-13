package zio.redis

import zio.ZIO
import zio.test._

trait PubSubSpec extends BaseSpec {

  val pubSubSuite: Spec[RedisExecutor, TestFailure[RedisError], TestSuccess] =
    suite("pubSub")(
      suite("subscribe")(
        testM("subscribe follow by multiple values") {
          val channel   = "channel"
          //val msg       = "msg"

          for {
            _ <- subscribe(channel)
              .tap(x => ZIO.effectTotal(println(x)))
              .runDrain
              .fork
            _ = println("ready")
            //_   <- publish(channel, msg)
            //_   <- publish(channel, msg)
            //_   <- publish(channel, msg)
            //_ = println("published")
            _ <- ZIO.effectTotal(Thread.sleep(20000))
          } yield assertCompletes
        }
      )
    )
}
