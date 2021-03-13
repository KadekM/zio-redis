package zio.redis

import zio._
import zio.logging._
import zio.stm.{STM, TRef, TSet}
import zio.stream.{Stream, ZStream}

object RedisExecutor {
  trait Service {
    def execute(command: Chunk[RespValue.BulkString]): IO[RedisError, RespValue]

    def subscribe(command: Chunk[RespValue.BulkString], channels: Chunk[RespValue.BulkString]): ZStream[Any, RedisError, RespValue]
  }

  lazy val live: ZLayer[Logging with Has[RedisConfig], RedisError.IOError, RedisExecutor] =
    ZLayer.identity[Logging] ++ ByteStream.live >>> StreamedExecutor

  lazy val local: ZLayer[Logging, RedisError.IOError, RedisExecutor] =
    ZLayer.identity[Logging] ++ ByteStream.default >>> StreamedExecutor

  lazy val test: URLayer[zio.random.Random, RedisExecutor] = TestExecutor.live

  private[this] final case class Request(command: Chunk[RespValue.BulkString], promise: Promise[RedisError, RespValue])

  private[this] final val True: Any => Boolean = _ => true

  private[this] final val RequestQueueSize = 16

  sealed trait Mode
  object Mode {
    case object ReqResp extends Mode
    case object Streaming extends Mode
  }

  private[this] final val StreamedExecutor =
    ZLayer.fromServicesManaged[ByteStream.Service, Logger[String], Any, RedisError.IOError, RedisExecutor.Service] {
      (byteStream: ByteStream.Service, logging: Logger[String]) =>
        for {
          reqQueue <- Queue.bounded[Request](RequestQueueSize).toManaged_
          resQueue <- Queue.unbounded[Promise[RedisError, RespValue]].toManaged_
          mode <- TRef.makeCommit[Mode](Mode.ReqResp).toManaged_
          subs     <- TSet.empty[RespValue.BulkString].commit.toManaged_
          subsRes <- Queue.unbounded[RespValue].toManaged_
          live      = new Live(reqQueue, resQueue, mode, subs, subsRes, byteStream, logging)
          _        <- live.run.forkManaged
        } yield live
    }

  private[this] final class Live(
    reqQueue: Queue[Request],
    resQueue: Queue[Promise[RedisError, RespValue]],
    mode: TRef[Mode],
    subs: TSet[RespValue.BulkString],
    subsResQueue: Queue[RespValue],
    byteStream: ByteStream.Service,
    logger: Logger[String]
  ) extends Service {

    def subscribe(command: Chunk[RespValue.BulkString], channels: Chunk[RespValue.BulkString]): Stream[RedisError, RespValue] = {
      for {
        promise <- ZStream.fromEffect(Promise.make[RedisError, RespValue])
        _       <- ZStream.fromEffect(reqQueue.offer(Request(command ++ channels, promise)))
        _       <- ZStream.fromEffect(promise.await)

        _       <- ZStream.fromEffect(STM.foreach(channels)(subs.put).commit)
        _ = println("waiting")
        res     <- ZStream.fromQueue(subsResQueue)
          .tap(x => ZIO.effectTotal(println(x)))
          .takeUntilM(_ => subs.isEmpty.commit)
      } yield res
    }

    def execute(command: Chunk[RespValue.BulkString]): IO[RedisError, RespValue] =
      Promise
        .make[RedisError, RespValue]
        .flatMap(promise => reqQueue.offer(Request(command, promise)) *> promise.await)

    /**
     * Opens a connection to the server and launches send and receive operations.
     * All failures are retried by opening a new connection.
     * Only exits by interruption or defect.
     */
    val run: IO[RedisError, Unit] =
      (send.forever race receive)
        .tapError(e => logger.warn(s"Reconnecting due to error: $e") *> drainWith(e))
        .retryWhile(True)
        .tapError(e => logger.error(s"Executor exiting: $e"))

    private def drainWith(e: RedisError): UIO[Unit] = resQueue.takeAll.flatMap(IO.foreach_(_)(_.fail(e)))

    private def send: IO[RedisError, Unit] =
      reqQueue.takeBetween(1, Int.MaxValue).flatMap { reqs =>
        val buffer = ChunkBuilder.make[Byte]()
        val it     = reqs.iterator

        while (it.hasNext) {
          val req = it.next()
          buffer ++= RespValue.Array(req.command).serialize
        }

        val bytes = buffer.result()

        byteStream
          .write(bytes)
          .mapError(RedisError.IOError)
          .tapBoth(
            e => IO.foreach_(reqs)(_.promise.fail(e)),
            _ => IO.foreach_(reqs)(req => resQueue.offer(req.promise))
          )
      }

    private def receive: IO[RedisError, Unit] =
      byteStream.read
        .mapError(RedisError.IOError)
        .transduce(RespValue.Decoder)
        .tap(x => ZIO.effectTotal(println(s"received! $x")))
        .foreach{response =>
          mode.get.commit.flatMap {
            case Mode.ReqResp => resQueue.take.flatMap(_.succeed(response))
            case Mode.Streaming => subsResQueue.offer(response)
          }
        }

  }
}
