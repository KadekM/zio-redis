package zio.redis

import scala.collection.compat.immutable.LazyList
import zio._
import zio.redis.RedisError.ProtocolError
import zio.redis.RespValue.BulkString
import zio.stm.{STM, TMap, TRef, USTM}
import zio.stream.ZStream

private[redis] final class TestExecutor private (
  sets: TMap[String, Set[String]],
  strings: TMap[String, String],
  randomPick: Int => USTM[Int],
  hyperLogLogs: TMap[String, Seq[String]]
) extends RedisExecutor.Service {

  def execute(command: Chunk[RespValue.BulkString]): IO[RedisError, RespValue] =
    for {
      name   <- ZIO.fromOption(command.headOption).orElseFail(ProtocolError("Malformed command."))
      result <- runCommand(name.asString, command.tail).commit
    } yield result

  private def errResponse(cmd: String): RespValue.BulkString =
    RespValue.bulkString(s"(error) ERR wrong number of arguments for '$cmd' command")

  private def onConnection(command: String, input: Chunk[RespValue.BulkString])(
    res: => RespValue.BulkString
  ): USTM[BulkString] = STM.succeedNow(if (input.isEmpty) errResponse(command) else res)

  private[this] def runCommand(name: String, input: Chunk[RespValue.BulkString]): STM[RedisError, RespValue] = {
    name match {
      case api.Connection.Ping.name =>
        STM.succeedNow {
          if (input.isEmpty)
            RespValue.bulkString("PONG")
          else
            input.head
        }
      case api.Connection.Auth.name   => onConnection(name, input)(RespValue.bulkString("OK"))
      case api.Connection.Echo.name   => onConnection(name, input)(input.head)
      case api.Connection.Select.name => onConnection(name, input)(RespValue.bulkString("OK"))
      case api.Sets.SAdd.name =>
        val key = input.head.asString
        STM.ifM(isSet(key))(
          {
            val values = input.tail.map(_.asString)
            for {
              oldSet <- sets.getOrElse(key, Set.empty)
              newSet  = oldSet ++ values
              added   = newSet.size - oldSet.size
              _      <- sets.put(key, newSet)
            } yield RespValue.Integer(added.toLong)
          },
          STM.succeedNow(Replies.WrongType)
        )

      case api.Sets.SCard.name =>
        val key = input.head.asString
        STM.ifM(isSet(key))(
          sets.get(key).map(_.fold(RespValue.Integer(0))(s => RespValue.Integer(s.size.toLong))),
          STM.succeedNow(Replies.WrongType)
        )

      case api.Sets.SDiff.name =>
        val allkeys = input.map(_.asString)
        val mainKey = allkeys.head
        val others  = allkeys.tail
        STM.ifM(forAll(allkeys)(isSet))(
          for {
            main   <- sets.getOrElse(mainKey, Set.empty)
            result <- STM.foldLeft(others)(main) { case (acc, k) => sets.get(k).map(_.fold(acc)(acc -- _)) }
          } yield RespValue.array(result.map(RespValue.bulkString(_)).toList: _*),
          STM.succeedNow(Replies.WrongType)
        )

      case api.Sets.SDiffStore.name =>
        val allkeys = input.map(_.asString)
        val distkey = allkeys.head
        val mainKey = allkeys(1)
        val others  = allkeys.drop(2)
        STM.ifM(forAll(allkeys)(isSet))(
          for {
            main   <- sets.getOrElse(mainKey, Set.empty)
            result <- STM.foldLeft(others)(main) { case (acc, k) => sets.get(k).map(_.fold(acc)(acc -- _)) }
            _      <- sets.put(distkey, result)
          } yield RespValue.Integer(result.size.toLong),
          STM.succeedNow(Replies.WrongType)
        )

      case api.Sets.SInter.name =>
        val keys      = input.map(_.asString)
        val mainKey   = keys.head
        val otherKeys = keys.tail
        sInter(mainKey, otherKeys).fold(_ => Replies.WrongType, Replies.array)

      case api.Sets.SInterStore.name =>
        val keys        = input.map(_.asString)
        val destination = keys.head
        val mainKey     = keys(1)
        val otherKeys   = keys.tail
        (STM.fail(()).unlessM(isSet(destination)) *> sInter(mainKey, otherKeys)).foldM(
          _ => STM.succeedNow(Replies.WrongType),
          s =>
            for {
              _ <- sets.put(destination, s)
            } yield RespValue.Integer(s.size.toLong)
        )

      case api.Sets.SIsMember.name =>
        val key    = input.head.asString
        val member = input(1).asString
        STM.ifM(isSet(key))(
          for {
            set   <- sets.getOrElse(key, Set.empty)
            result = if (set.contains(member)) RespValue.Integer(1) else RespValue.Integer(0)
          } yield result,
          STM.succeedNow(Replies.WrongType)
        )

      case api.Sets.SMove.name =>
        val sourceKey      = input.head.asString
        val destinationKey = input(1).asString
        val member         = input(2).asString
        STM.ifM(isSet(sourceKey))(
          sets.getOrElse(sourceKey, Set.empty).flatMap { source =>
            if (source.contains(member))
              STM.ifM(isSet(destinationKey))(
                for {
                  dest <- sets.getOrElse(destinationKey, Set.empty)
                  _    <- sets.put(sourceKey, source - member)
                  _    <- sets.put(destinationKey, dest + member)
                } yield RespValue.Integer(1),
                STM.succeedNow(Replies.WrongType)
              )
            else STM.succeedNow(RespValue.Integer(0))
          },
          STM.succeedNow(Replies.WrongType)
        )

      case api.Sets.SPop.name =>
        val key   = input.head.asString
        val count = if (input.size == 1) 1 else input(1).asString.toInt
        STM.ifM(isSet(key))(
          for {
            set   <- sets.getOrElse(key, Set.empty)
            result = set.take(count)
            _     <- sets.put(key, set -- result)
          } yield Replies.array(result),
          STM.succeedNow(Replies.WrongType)
        )

      case api.Sets.SMembers.name =>
        val key = input.head.asString
        STM.ifM(isSet(key))(
          sets.get(key).map(_.fold(Replies.EmptyArray)(Replies.array(_))),
          STM.succeedNow(Replies.WrongType)
        )

      case api.Sets.SRandMember.name =>
        val key = input.head.asString
        STM.ifM(isSet(key))(
          {
            val maybeCount = input.tail.headOption.map(b => b.asString.toLong)
            for {
              set     <- sets.getOrElse(key, Set.empty[String])
              asVector = set.toVector
              res <- maybeCount match {
                       case None =>
                         selectOne[String](asVector, randomPick).map(maybeValue =>
                           maybeValue.map(RespValue.bulkString).getOrElse(RespValue.NullBulkString)
                         )
                       case Some(n) if n > 0 => selectN(asVector, n, randomPick).map(Replies.array)
                       case Some(n) if n < 0 =>
                         selectNWithReplacement(asVector, -1 * n, randomPick).map(Replies.array)
                       case Some(0) => STM.succeedNow(RespValue.NullBulkString)
                     }
            } yield res
          },
          STM.succeedNow(Replies.WrongType)
        )

      case api.Sets.SRem.name =>
        val key = input.head.asString
        STM.ifM(isSet(key))(
          {
            val values = input.tail.map(_.asString)
            for {
              oldSet <- sets.getOrElse(key, Set.empty)
              newSet  = oldSet -- values
              removed = oldSet.size - newSet.size
              _      <- sets.put(key, newSet)
            } yield RespValue.Integer(removed.toLong)
          },
          STM.succeedNow(Replies.WrongType)
        )

      case api.Sets.SUnion.name =>
        val keys = input.map(_.asString)
        STM.ifM(forAll(keys)(isSet))(
          STM
            .foldLeft(keys)(Set.empty[String]) { (unionSoFar, nextKey) =>
              sets.getOrElse(nextKey, Set.empty[String]).map { currentSet =>
                unionSoFar ++ currentSet
              }
            }
            .map(unionSet => Replies.array(unionSet)),
          STM.succeedNow(Replies.WrongType)
        )

      case api.Sets.SUnionStore.name =>
        val destination = input.head.asString
        val keys        = input.tail.map(_.asString)
        STM.ifM(forAll(keys)(isSet))(
          for {
            union <- STM
                       .foldLeft(keys)(Set.empty[String]) { (unionSoFar, nextKey) =>
                         sets.getOrElse(nextKey, Set.empty[String]).map { currentSet =>
                           unionSoFar ++ currentSet
                         }
                       }
            _ <- sets.put(destination, union)
          } yield RespValue.Integer(union.size.toLong),
          STM.succeedNow(Replies.WrongType)
        )

      case api.Sets.SScan.name =>
        def maybeGetCount(key: RespValue.BulkString, value: RespValue.BulkString): Option[Int] =
          key.asString match {
            case "COUNT" => Some(value.asString.toInt)
            case _       => None
          }

        val key = input.head.asString
        STM.ifM(isSet(key))(
          {
            val start = input(1).asString.toInt
            val maybeRegex = if (input.size > 2) input(2).asString match {
              case "MATCH" => Some(input(3).asString.r)
              case _       => None
            }
            else None
            val maybeCount =
              if (input.size > 4) maybeGetCount(input(4), input(5))
              else if (input.size > 2) maybeGetCount(input(2), input(3))
              else None
            val end = start + maybeCount.getOrElse(10)
            for {
              set      <- sets.getOrElse(key, Set.empty)
              filtered  = maybeRegex.map(regex => set.filter(s => regex.pattern.matcher(s).matches)).getOrElse(set)
              resultSet = filtered.slice(start, end)
              nextIndex = if (filtered.size <= end) 0 else end
              results   = Replies.array(resultSet)
            } yield RespValue.array(RespValue.bulkString(nextIndex.toString), results)
          },
          STM.succeedNow(Replies.WrongType)
        )

      case api.Strings.Set.name =>
        // not a full implementation. Just enough to make set tests work
        val key   = input.head.asString
        val value = input(1).asString
        strings.put(key, value).as(Replies.Ok)
      case api.HyperLogLog.PfAdd.name =>
        val key    = input.head.asString
        val values = input.tail.map(_.asString)
        STM.ifM(forAll(Chunk.single(key))(isSet))(
          for {
            oldValues <- hyperLogLogs.getOrElse(key, Chunk.empty)
            ret        = if (oldValues == values) 0L else 1L
            _         <- hyperLogLogs.put(key, values)
          } yield {
            RespValue.Integer(ret)
          },
          STM.succeedNow(Replies.WrongType)
        )

      case api.HyperLogLog.PfCount.name =>
        val keys = input.map(_.asString)
        STM.ifM(forAll(keys)(isSet))(
          STM
            .foldLeft(keys)(Seq.empty[String]) { (bHyperLogLogs, nextKey) =>
              hyperLogLogs.getOrElse(nextKey, Seq.empty[String]).map { currentSet =>
                bHyperLogLogs ++ currentSet
              }
            }
            .map(vs => RespValue.Integer(vs.size.toLong)),
          STM.succeedNow(Replies.WrongType)
        )
      case api.HyperLogLog.PfMerge.name =>
        val key    = input.head.asString
        val values = input.tail.map(_.asString)
        STM.ifM(forAll(values ++ Chunk.single(key))(isSet))(
          for {
            sourceValues <- STM.foldLeft(values)(Chunk.empty: Chunk[String]) { (bHyperLogLogs, nextKey) =>
                              hyperLogLogs.getOrElse(nextKey, Chunk.empty).map { currentSet =>
                                bHyperLogLogs ++ currentSet
                              }
                            }
            destValues <- hyperLogLogs.getOrElse(key, Chunk.empty)
            putValues   = sourceValues ++ destValues
            _          <- hyperLogLogs.put(key, putValues)
          } yield Replies.Ok,
          STM.succeedNow(Replies.WrongType)
        )
      case _ => STM.fail(RedisError.ProtocolError(s"Command not supported by test executor: $name"))
    }
  }

  // check whether the key is a set or unused.
  private[this] def isSet(name: String): STM[Nothing, Boolean] =
    for {
      isString <- strings.contains(name)
    } yield !isString

  private[this] def selectN[A](values: Vector[A], n: Long, pickRandom: Int => USTM[Int]): USTM[List[A]] = {
    def go(remaining: Vector[A], toPick: Long, acc: List[A]): USTM[List[A]] =
      (remaining, toPick) match {
        case (Vector(), _) | (_, 0) => STM.succeed(acc)
        case _ =>
          pickRandom(remaining.size).flatMap { index =>
            val x  = remaining(index)
            val xs = remaining.patch(index, Nil, 1)
            go(xs, toPick - 1, x :: acc)
          }
      }

    go(values, Math.max(n, 0), Nil)
  }

  private[this] def selectOne[A](values: Vector[A], pickRandom: Int => USTM[Int]): USTM[Option[A]] =
    if (values.isEmpty) STM.succeedNow(None)
    else pickRandom(values.size).map(index => Some(values(index)))

  private[this] def selectNWithReplacement[A](
    values: Vector[A],
    n: Long,
    pickRandom: Int => USTM[Int]
  ): USTM[List[A]] =
    STM
      .loop(Math.max(n, 0))(_ > 0, _ - 1)(_ => selectOne(values, pickRandom))
      .map(_.flatten)

  private[this] def sInter(mainKey: String, otherKeys: Chunk[String]): STM[Unit, Set[String]] = {
    sealed trait State
    object State {

      case object WrongType extends State

      case object Empty extends State

      final case class Continue(values: Set[String]) extends State

    }
    def get(key: String): STM[Nothing, State] =
      STM.ifM(isSet(key))(
        sets.get(key).map(_.fold[State](State.Empty)(State.Continue)),
        STM.succeedNow(State.WrongType)
      )

    def step(state: State, next: String): STM[Nothing, State] =
      state match {
        case State.Empty     => STM.succeedNow(State.Empty)
        case State.WrongType => STM.succeedNow(State.WrongType)
        case State.Continue(values) =>
          get(next).map {
            case State.Continue(otherValues) =>
              val intersection = values.intersect(otherValues)
              if (intersection.isEmpty) State.Empty else State.Continue(intersection)
            case s => s
          }
      }

    for {
      init  <- get(mainKey)
      state <- STM.foldLeft(otherKeys)(init)(step)
      result <- state match {
                  case State.Continue(values) => STM.succeedNow(values)
                  case State.Empty            => STM.succeedNow(Set.empty[String])
                  case State.WrongType        => STM.fail(())
                }
    } yield result
  }

  private[this] def forAll[A](chunk: Chunk[A])(f: A => STM[Nothing, Boolean]) =
    STM.foldLeft(chunk)(true) { case (b, a) => f(a).map(b && _) }

  private[this] object Replies {
    val Ok: RespValue.SimpleString = RespValue.SimpleString("OK")
    val WrongType: RespValue.Error = RespValue.Error("WRONGTYPE")

    def array(values: Iterable[String]): RespValue.Array =
      RespValue.array(values.map(RespValue.bulkString).toList: _*)

    val EmptyArray: RespValue.Array = RespValue.array()
  }

  override def subscribe(command: Chunk[BulkString], channels: Chunk[BulkString]): ZStream[Any, RedisError, RespValue] = ???
}

private[redis] object TestExecutor {
  lazy val live: URLayer[zio.random.Random, RedisExecutor] = {
    val executor = for {
      seed         <- random.nextInt
      sRandom       = new scala.util.Random(seed)
      ref          <- TRef.make(LazyList.continually((i: Int) => sRandom.nextInt(i))).commit
      randomPick    = (i: Int) => ref.modify(s => (s.head(i), s.tail))
      sets         <- TMap.empty[String, Set[String]].commit
      strings      <- TMap.empty[String, String].commit
      hyperLogLogs <- TMap.empty[String, Seq[String]].commit
    } yield new TestExecutor(sets, strings, randomPick, hyperLogLogs)

    executor.toLayer
  }
}
