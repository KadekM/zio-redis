package zio.redis

import java.time.Instant

import zio.duration._
import zio.redis.Input._
import zio.test.Assertion._
import zio.test._
import zio.{ Chunk, Task }

object InputSpec extends BaseSpec {
  import BitFieldCommand._
  import BitFieldType._
  import BitOperation._
  import Order._
  import RadiusUnit._

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Input encoders")(
      suite("AbsTtl")(
        testM("valid value") {
          for {
            result <- Task(AbsTtlInput.encode(AbsTtl))
          } yield assert(result)(equalTo(respArgs("ABSTTL")))
        }
      ),
      suite("Aggregate")(
        testM("max") {
          for {
            result <- Task(AggregateInput.encode(Aggregate.Max))
          } yield assert(result)(equalTo(respArgs("AGGREGATE", "MAX")))
        },
        testM("min") {
          for {
            result <- Task(AggregateInput.encode(Aggregate.Min))
          } yield assert(result)(equalTo(respArgs("AGGREGATE", "MIN")))
        },
        testM("sum") {
          for {
            result <- Task(AggregateInput.encode(Aggregate.Sum))
          } yield assert(result)(equalTo(respArgs("AGGREGATE", "SUM")))
        }
      ),
      suite("Alpha")(
        testM("alpha") {
          for {
            result <- Task(AlphaInput.encode(Alpha))
          } yield assert(result)(equalTo(respArgs("ALPHA")))
        }
      ),
      suite("Auth")(
        testM("with empty password") {
          for {
            result <- Task(AuthInput.encode(Auth("")))
          } yield assert(result)(equalTo(respArgs("AUTH", "")))
        },
        testM("with non-empty password") {
          for {
            result <- Task(AuthInput.encode(Auth("pass")))
          } yield assert(result)(equalTo(respArgs("AUTH", "pass")))
        }
      ),
      suite("Bool")(
        testM("true") {
          for {
            result <- Task(BoolInput.encode(true))
          } yield assert(result)(equalTo(respArgs("1")))
        },
        testM("false") {
          for {
            result <- Task(BoolInput.encode(false))
          } yield assert(result)(equalTo(respArgs("0")))
        }
      ),
      suite("BitFieldCommand")(
        testM("get with unsigned type and positive offset") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldGet(UnsignedInt(3), 2)))
          } yield assert(result)(equalTo(respArgs("GET", "u3", "2")))
        },
        testM("get with signed type and negative offset") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldGet(SignedInt(3), -2)))
          } yield assert(result)(equalTo(respArgs("GET", "i3", "-2")))
        },
        testM("get with unsigned type and zero offset") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldGet(UnsignedInt(3), 0)))
          } yield assert(result)(equalTo(respArgs("GET", "u3", "0")))
        },
        testM("set with unsigned type, positive offset and positive value") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldSet(UnsignedInt(3), 2, 100L)))
          } yield assert(result)(equalTo(respArgs("SET", "u3", "2", "100")))
        },
        testM("set with signed type, negative offset and negative value") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldSet(SignedInt(3), -2, -100L)))
          } yield assert(result)(equalTo(respArgs("SET", "i3", "-2", "-100")))
        },
        testM("set with unsigned type, zero offset and zero value") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldSet(UnsignedInt(3), 0, 0L)))
          } yield assert(result)(equalTo(respArgs("SET", "u3", "0", "0")))
        },
        testM("incr with unsigned type, positive offset and positive value") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldIncr(UnsignedInt(3), 2, 100L)))
          } yield assert(result)(equalTo(respArgs("INCRBY", "u3", "2", "100")))
        },
        testM("incr with signed type, negative offset and negative value") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldIncr(SignedInt(3), -2, -100L)))
          } yield assert(result)(equalTo(respArgs("INCRBY", "i3", "-2", "-100")))
        },
        testM("incr with unsigned type, zero offset and zero value") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldIncr(UnsignedInt(3), 0, 0L)))
          } yield assert(result)(equalTo(respArgs("INCRBY", "u3", "0", "0")))
        },
        testM("overflow sat") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldOverflow.Sat))
          } yield assert(result)(equalTo(respArgs("OVERFLOW", "SAT")))
        },
        testM("overflow fail") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldOverflow.Fail))
          } yield assert(result)(equalTo(respArgs("OVERFLOW", "FAIL")))
        },
        testM("overflow warp") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldOverflow.Wrap))
          } yield assert(result)(equalTo(respArgs("OVERFLOW", "WRAP")))
        }
      ),
      suite("BitOperation")(
        testM("and") {
          for {
            result <- Task(BitOperationInput.encode(AND))
          } yield assert(result)(equalTo(respArgs("AND")))
        },
        testM("or") {
          for {
            result <- Task(BitOperationInput.encode(OR))
          } yield assert(result)(equalTo(respArgs("OR")))
        },
        testM("xor") {
          for {
            result <- Task(BitOperationInput.encode(XOR))
          } yield assert(result)(equalTo(respArgs("XOR")))
        },
        testM("not") {
          for {
            result <- Task(BitOperationInput.encode(NOT))
          } yield assert(result)(equalTo(respArgs("NOT")))
        }
      ),
      suite("BitPosRange")(
        testM("with only start") {
          for {
            result <- Task(BitPosRangeInput.encode(BitPosRange(1.second.toMillis, None)))
          } yield assert(result)(equalTo(respArgs("1000")))
        },
        testM("with start and the end") {
          for {
            result <- Task(BitPosRangeInput.encode(BitPosRange(0.second.toMillis, Some(1.second.toMillis))))
          } yield assert(result)(equalTo(respArgs("0", "1000")))
        }
      ),
      suite("By")(
        testM("with a pattern") {
          for {
            result <- Task(ByInput.encode("mykey_*"))
          } yield assert(result)(equalTo(respArgs("BY", "mykey_*")))
        }
      ),
      suite("Changed")(
        testM("valid value") {
          for {
            result <- Task(ChangedInput.encode(Changed))
          } yield assert(result)(equalTo(respArgs("CH")))
        }
      ),
      suite("Copy")(
        testM("valid value") {
          for {
            result <- Task(CopyInput.encode(Copy))
          } yield assert(result)(equalTo(respArgs("COPY")))
        }
      ),
      suite("Count")(
        testM("positive value") {
          for {
            result <- Task(CountInput.encode(Count(3L)))
          } yield assert(result)(equalTo(respArgs("COUNT", "3")))
        },
        testM("negative value") {
          for {
            result <- Task(CountInput.encode(Count(-3L)))
          } yield assert(result)(equalTo(respArgs("COUNT", "-3")))
        },
        testM("zero value") {
          for {
            result <- Task(CountInput.encode(Count(0L)))
          } yield assert(result)(equalTo(respArgs("COUNT", "0")))
        }
      ),
      suite("Position")(
        testM("before") {
          for {
            result <- Task(PositionInput.encode(Position.Before))
          } yield assert(result)(equalTo(respArgs("BEFORE")))
        },
        testM("after") {
          for {
            result <- Task(PositionInput.encode(Position.After))
          } yield assert(result)(equalTo(respArgs("AFTER")))
        }
      ),
      suite("Double")(
        testM("positive value") {
          for {
            result <- Task(DoubleInput.encode(4.2d))
          } yield assert(result)(equalTo(respArgs("4.2")))
        },
        testM("negative value") {
          for {
            result <- Task(DoubleInput.encode(-4.2d))
          } yield assert(result)(equalTo(respArgs("-4.2")))
        },
        testM("zero value") {
          for {
            result <- Task(DoubleInput.encode(0d))
          } yield assert(result)(equalTo(respArgs("0.0")))
        }
      ),
      suite("DurationMilliseconds")(
        testM("1 second") {
          for {
            result <- Task(DurationMillisecondsInput.encode(1.second))
          } yield assert(result)(equalTo(respArgs("1000")))
        },
        testM("100 milliseconds") {
          for {
            result <- Task(DurationMillisecondsInput.encode(100.millis))
          } yield assert(result)(equalTo(respArgs("100")))
        }
      ),
      suite("DurationSeconds")(
        testM("1 minute") {
          for {
            result <- Task(DurationSecondsInput.encode(1.minute))
          } yield assert(result)(equalTo(respArgs("60")))
        },
        testM("1 second") {
          for {
            result <- Task(DurationSecondsInput.encode(1.second))
          } yield assert(result)(equalTo(respArgs("1")))
        },
        testM("100 milliseconds") {
          for {
            result <- Task(DurationSecondsInput.encode(100.millis))
          } yield assert(result)(equalTo(respArgs("0")))
        }
      ),
      suite("DurationTtl")(
        testM("1 second") {
          for {
            result <- Task(DurationTtlInput.encode(1.second))
          } yield assert(result)(equalTo(respArgs("PX", "1000")))
        },
        testM("100 milliseconds") {
          for {
            result <- Task(DurationTtlInput.encode(100.millis))
          } yield assert(result)(equalTo(respArgs("PX", "100")))
        }
      ),
      suite("Freq")(
        testM("empty string") {
          for {
            result <- Task(FreqInput.encode(Freq("")))
          } yield assert(result)(equalTo(respArgs("FREQ", "")))
        },
        testM("non-empty string") {
          for {
            result <- Task(FreqInput.encode(Freq("frequency")))
          } yield assert(result)(equalTo(respArgs("FREQ", "frequency")))
        }
      ),
      suite("Get")(
        testM("with a pattern") {
          for {
            result <- Task(GetInput.encode("mypattern_*"))
          } yield assert(result)(equalTo(respArgs("GET", "mypattern_*")))
        }
      ),
      suite("IdleTime")(
        testM("0 seconds") {
          for {
            result <- Task(IdleTimeInput.encode(IdleTime(0)))
          } yield assert(result)(equalTo(respArgs("IDLETIME", "0")))
        },
        testM("5 seconds") {
          for {
            result <- Task(IdleTimeInput.encode(IdleTime(5)))
          } yield assert(result)(equalTo(respArgs("IDLETIME", "5")))
        }
      ),
      suite("Increment")(
        testM("valid value") {
          for {
            result <- Task(IncrementInput.encode(Increment))
          } yield assert(result)(equalTo(respArgs("INCR")))
        }
      ),
      suite("KeepTtl")(
        testM("valid value") {
          for {
            result <- Task(KeepTtlInput.encode(KeepTtl))
          } yield assert(result)(equalTo(respArgs("KEEPTTL")))
        }
      ),
      suite("LexRange")(
        testM("with unbound min and unbound max") {
          for {
            result <- Task(LexRangeInput.encode(LexRange(LexMinimum.Unbounded, LexMaximum.Unbounded)))
          } yield assert(result)(equalTo(respArgs("-", "+")))
        },
        testM("with open min and unbound max") {
          for {
            result <- Task(LexRangeInput.encode(LexRange(LexMinimum.Open("a"), LexMaximum.Unbounded)))
          } yield assert(result)(equalTo(respArgs("(a", "+")))
        },
        testM("with closed min and unbound max") {
          for {
            result <- Task(LexRangeInput.encode(LexRange(LexMinimum.Closed("a"), LexMaximum.Unbounded)))
          } yield assert(result)(equalTo(respArgs("[a", "+")))
        },
        testM("with unbound min and open max") {
          for {
            result <- Task(LexRangeInput.encode(LexRange(LexMinimum.Unbounded, LexMaximum.Open("z"))))
          } yield assert(result)(equalTo(respArgs("-", "(z")))
        },
        testM("with open min and open max") {
          for {
            result <- Task(LexRangeInput.encode(LexRange(LexMinimum.Open("a"), LexMaximum.Open("z"))))
          } yield assert(result)(equalTo(respArgs("(a", "(z")))
        },
        testM("with closed min and open max") {
          for {
            result <- Task(LexRangeInput.encode(LexRange(LexMinimum.Closed("a"), LexMaximum.Open("z"))))
          } yield assert(result)(equalTo(respArgs("[a", "(z")))
        },
        testM("with unbound min and closed max") {
          for {
            result <- Task(LexRangeInput.encode(LexRange(LexMinimum.Unbounded, LexMaximum.Closed("z"))))
          } yield assert(result)(equalTo(respArgs("-", "[z")))
        },
        testM("with open min and closed max") {
          for {
            result <- Task(LexRangeInput.encode(LexRange(LexMinimum.Open("a"), LexMaximum.Closed("z"))))
          } yield assert(result)(equalTo(respArgs("(a", "[z")))
        },
        testM("with closed min and closed max") {
          for {
            result <- Task(LexRangeInput.encode(LexRange(LexMinimum.Closed("a"), LexMaximum.Closed("z"))))
          } yield assert(result)(equalTo(respArgs("[a", "[z")))
        }
      ),
      suite("Limit")(
        testM("with positive offset and positive count") {
          for {
            result <- Task(LimitInput.encode(Limit(4L, 5L)))
          } yield assert(result)(equalTo(respArgs("LIMIT", "4", "5")))
        },
        testM("with negative offset and negative count") {
          for {
            result <- Task(LimitInput.encode(Limit(-4L, -5L)))
          } yield assert(result)(equalTo(respArgs("LIMIT", "-4", "-5")))
        },
        testM("with zero offset and zero count") {
          for {
            result <- Task(LimitInput.encode(Limit(0L, 0L)))
          } yield assert(result)(equalTo(respArgs("LIMIT", "0", "0")))
        }
      ),
      suite("Long")(
        testM("positive value") {
          for {
            result <- Task(LongInput.encode(4L))
          } yield assert(result)(equalTo(respArgs("4")))
        },
        testM("negative value") {
          for {
            result <- Task(LongInput.encode(-4L))
          } yield assert(result)(equalTo(respArgs("-4")))
        },
        testM("zero value") {
          for {
            result <- Task(LongInput.encode(0L))
          } yield assert(result)(equalTo(respArgs("0")))
        }
      ),
      suite("LongLat")(
        testM("positive longitude and latitude") {
          for {
            result <- Task(LongLatInput.encode(LongLat(4.2d, 5.2d)))
          } yield assert(result)(equalTo(respArgs("4.2", "5.2")))
        },
        testM("negative longitude and latitude") {
          for {
            result <- Task(LongLatInput.encode(LongLat(-4.2d, -5.2d)))
          } yield assert(result)(equalTo(respArgs("-4.2", "-5.2")))
        },
        testM("zero longitude and latitude") {
          for {
            result <- Task(LongLatInput.encode(LongLat(0d, 0d)))
          } yield assert(result)(equalTo(respArgs("0.0", "0.0")))
        }
      ),
      suite("MemberScore")(
        testM("with positive score and empty member") {
          for {
            result <- Task(MemberScoreInput.encode(MemberScore(4.2d, "")))
          } yield assert(result)(equalTo(respArgs("4.2", "")))
        },
        testM("with negative score and empty member") {
          for {
            result <- Task(MemberScoreInput.encode(MemberScore(-4.2d, "")))
          } yield assert(result)(equalTo(respArgs("-4.2", "")))
        },
        testM("with zero score and empty member") {
          for {
            result <- Task(MemberScoreInput.encode(MemberScore(0d, "")))
          } yield assert(result)(equalTo(respArgs("0.0", "")))
        },
        testM("with positive score and non-empty member") {
          for {
            result <- Task(MemberScoreInput.encode(MemberScore(4.2d, "member")))
          } yield assert(result)(equalTo(respArgs("4.2", "member")))
        },
        testM("with negative score and non-empty member") {
          for {
            result <- Task(MemberScoreInput.encode(MemberScore(-4.2d, "member")))
          } yield assert(result)(equalTo(respArgs("-4.2", "member")))
        },
        testM("with zero score and non-empty member") {
          for {
            result <- Task(MemberScoreInput.encode(MemberScore(0d, "member")))
          } yield assert(result)(equalTo(respArgs("0.0", "member")))
        }
      ),
      suite("NoInput")(
        testM("valid value") {
          for {
            result <- Task(NoInput.encode(()))
          } yield assert(result)(isEmpty)
        }
      ),
      suite("NonEmptyList")(
        testM("with multiple elements") {
          for {
            result <- Task(NonEmptyList(StringInput).encode(("a", List("b", "c"))))
          } yield assert(result)(equalTo(respArgs("a", "b", "c")))
        },
        testM("with one element") {
          for {
            result <- Task(NonEmptyList(StringInput).encode(("a", List.empty)))
          } yield assert(result)(equalTo(respArgs("a")))
        }
      ),
      suite("Order")(
        testM("ascending") {
          for {
            result <- Task(OrderInput.encode(Ascending))
          } yield assert(result)(equalTo(respArgs("ASC")))
        },
        testM("descending") {
          for {
            result <- Task(OrderInput.encode(Descending))
          } yield assert(result)(equalTo(respArgs("DESC")))
        }
      ),
      suite("RadiusUnit")(
        testM("meters") {
          for {
            result <- Task(RadiusUnitInput.encode(Meters))
          } yield assert(result)(equalTo(respArgs("m")))
        },
        testM("kilometers") {
          for {
            result <- Task(RadiusUnitInput.encode(Kilometers))
          } yield assert(result)(equalTo(respArgs("km")))
        },
        testM("feet") {
          for {
            result <- Task(RadiusUnitInput.encode(Feet))
          } yield assert(result)(equalTo(respArgs("ft")))
        },
        testM("miles") {
          for {
            result <- Task(RadiusUnitInput.encode(Miles))
          } yield assert(result)(equalTo(respArgs("mi")))
        }
      ),
      suite("Range")(
        testM("with positive start and positive end") {
          for {
            result <- Task(RangeInput.encode(Range(1, 5)))
          } yield assert(result)(equalTo(respArgs("1", "5")))
        },
        testM("with negative start and positive end") {
          for {
            result <- Task(RangeInput.encode(Range(-1, 5)))
          } yield assert(result)(equalTo(respArgs("-1", "5")))
        },
        testM("with positive start and negative end") {
          for {
            result <- Task(RangeInput.encode(Range(1, -5)))
          } yield assert(result)(equalTo(respArgs("1", "-5")))
        },
        testM("with negative start and negative end") {
          for {
            result <- Task(RangeInput.encode(Range(-1, -5)))
          } yield assert(result)(equalTo(respArgs("-1", "-5")))
        }
      ),
      suite("Regex")(
        testM("with valid pattern") {
          for {
            result <- Task(RegexInput.encode("\\d{3}".r))
          } yield assert(result)(equalTo(respArgs("MATCH", "\\d{3}")))
        },
        testM("with empty pattern") {
          for {
            result <- Task(RegexInput.encode("".r))
          } yield assert(result)(equalTo(respArgs("MATCH", "")))
        }
      ),
      suite("Replace")(
        testM("valid value") {
          for {
            result <- Task(ReplaceInput.encode(Replace))
          } yield assert(result)(equalTo(respArgs("REPLACE")))
        }
      ),
      suite("StoreDist")(
        testM("with non-empty string") {
          for {
            result <- Task(StoreDistInput.encode(StoreDist("key")))
          } yield assert(result)(equalTo(respArgs("STOREDIST", "key")))
        },
        testM("with empty string") {
          for {
            result <- Task(StoreDistInput.encode(StoreDist("")))
          } yield assert(result)(equalTo(respArgs("STOREDIST", "")))
        }
      ),
      suite("Store")(
        testM("with non-empty string") {
          for {
            result <- Task(StoreInput.encode(Store("key")))
          } yield assert(result)(equalTo(respArgs("STORE", "key")))
        },
        testM("with empty string") {
          for {
            result <- Task(StoreInput.encode(Store("")))
          } yield assert(result)(equalTo(respArgs("STORE", "")))
        }
      ),
      suite("ScoreRange")(
        testM("with infinite min and infinite max") {
          for {
            result <- Task(ScoreRangeInput.encode(ScoreRange(ScoreMinimum.Infinity, ScoreMaximum.Infinity)))
          } yield assert(result)(equalTo(respArgs("-inf", "+inf")))
        },
        testM("with open min and infinite max") {
          for {
            result <- Task(ScoreRangeInput.encode(ScoreRange(ScoreMinimum.Open(4.2d), ScoreMaximum.Infinity)))
          } yield assert(result)(equalTo(respArgs("(4.2", "+inf")))
        },
        testM("with closed min and infinite max") {
          for {
            result <- Task(ScoreRangeInput.encode(ScoreRange(ScoreMinimum.Closed(4.2d), ScoreMaximum.Infinity)))
          } yield assert(result)(equalTo(respArgs("4.2", "+inf")))
        },
        testM("with infinite min and open max") {
          for {
            result <- Task(ScoreRangeInput.encode(ScoreRange(ScoreMinimum.Infinity, ScoreMaximum.Open(5.2d))))
          } yield assert(result)(equalTo(respArgs("-inf", "(5.2")))
        },
        testM("with open min and open max") {
          for {
            result <- Task(ScoreRangeInput.encode(ScoreRange(ScoreMinimum.Open(4.2d), ScoreMaximum.Open(5.2d))))
          } yield assert(result)(equalTo(respArgs("(4.2", "(5.2")))
        },
        testM("with closed min and open max") {
          for {
            result <- Task(ScoreRangeInput.encode(ScoreRange(ScoreMinimum.Closed(4.2d), ScoreMaximum.Open(5.2d))))
          } yield assert(result)(equalTo(respArgs("4.2", "(5.2")))
        },
        testM("with infinite min and closed max") {
          for {
            result <- Task(ScoreRangeInput.encode(ScoreRange(ScoreMinimum.Infinity, ScoreMaximum.Closed(5.2d))))
          } yield assert(result)(equalTo(respArgs("-inf", "5.2")))
        },
        testM("with open min and closed max") {
          for {
            result <- Task(ScoreRangeInput.encode(ScoreRange(ScoreMinimum.Open(4.2d), ScoreMaximum.Closed(5.2d))))
          } yield assert(result)(equalTo(respArgs("(4.2", "5.2")))
        },
        testM("with closed min and closed max") {
          for {
            result <- Task(ScoreRangeInput.encode(ScoreRange(ScoreMinimum.Closed(4.2d), ScoreMaximum.Closed(5.2d))))
          } yield assert(result)(equalTo(respArgs("4.2", "5.2")))
        }
      ),
      suite("String")(
        testM("non-empty value") {
          for {
            result <- Task(StringInput.encode("non-empty"))
          } yield assert(result)(equalTo(respArgs("non-empty")))
        },
        testM("empty value") {
          for {
            result <- Task(StringInput.encode(""))
          } yield assert(result)(equalTo(respArgs("")))
        }
      ),
      suite("Optional")(
        testM("none") {
          for {
            result <- Task(OptionalInput(LongInput).encode(None))
          } yield assert(result)(isEmpty)
        },
        testM("some") {
          for {
            result <- Task(OptionalInput(LongInput).encode(Some(2L)))
          } yield assert(result)(equalTo(respArgs("2")))
        }
      ),
      suite("TimeSeconds")(
        testM("positiv value") {
          for {
            result <- Task(TimeSecondsInput.encode(Instant.ofEpochSecond(3L)))
          } yield assert(result)(equalTo(respArgs("3")))
        },
        testM("zero value") {
          for {
            result <- Task(TimeSecondsInput.encode(Instant.ofEpochSecond(0L)))
          } yield assert(result)(equalTo(respArgs("0")))
        },
        testM("negative value") {
          for {
            result <- Task(TimeSecondsInput.encode(Instant.ofEpochSecond(-3L)))
          } yield assert(result)(equalTo(respArgs("-3")))
        }
      ),
      suite("TimeMilliseconds")(
        testM("positiv value") {
          for {
            result <- Task(TimeMillisecondsInput.encode(Instant.ofEpochSecond(3L)))
          } yield assert(result)(equalTo(respArgs("3000")))
        },
        testM("zero value") {
          for {
            result <- Task(TimeMillisecondsInput.encode(Instant.ofEpochSecond(0L)))
          } yield assert(result)(equalTo(respArgs("0")))
        },
        testM("negative value") {
          for {
            result <- Task(TimeMillisecondsInput.encode(Instant.ofEpochSecond(-3L)))
          } yield assert(result)(equalTo(respArgs("-3000")))
        }
      ),
      suite("Tuple2")(
        testM("valid value") {
          for {
            result <- Task(Tuple2(StringInput, LongInput).encode(("one", 2L)))
          } yield assert(result)(equalTo(respArgs("one", "2")))
        }
      ),
      suite("Tuple3")(
        testM("valid value") {
          for {
            result <- Task(Tuple3(StringInput, LongInput, StringInput).encode(("one", 2, "three")))
          } yield assert(result)(equalTo(respArgs("one", "2", "three")))
        }
      ),
      suite("Tuple4")(
        testM("valid value") {
          for {
            result <- Task(Tuple4(StringInput, LongInput, StringInput, LongInput).encode(("one", 2, "three", 4)))
          } yield assert(result)(equalTo(respArgs("one", "2", "three", "4")))
        }
      ),
      suite("Tuple5")(
        testM("valid value") {
          for {
            result <- Task(
                        Tuple5(StringInput, LongInput, StringInput, LongInput, StringInput)
                          .encode(("one", 2, "three", 4, "five"))
                      )
          } yield assert(result)(
            equalTo(respArgs("one", "2", "three", "4", "five"))
          )
        }
      ),
      suite("Tuple7")(
        testM("valid value") {
          for {
            result <- Task(
                        Tuple7(StringInput, LongInput, StringInput, LongInput, StringInput, LongInput, StringInput)
                          .encode(("one", 2, "three", 4, "five", 6, "seven"))
                      )
          } yield assert(result)(
            equalTo(
              respArgs(
                "one",
                "2",
                "three",
                "4",
                "five",
                "6",
                "seven"
              )
            )
          )
        }
      ),
      suite("Tuple9")(
        testM("valid value") {
          for {
            result <- Task(
                        Tuple9(
                          StringInput,
                          LongInput,
                          StringInput,
                          LongInput,
                          StringInput,
                          LongInput,
                          StringInput,
                          LongInput,
                          StringInput
                        ).encode(("one", 2, "three", 4, "five", 6, "seven", 8, "nine"))
                      )
          } yield assert(result)(
            equalTo(
              respArgs(
                "one",
                "2",
                "three",
                "4",
                "five",
                "6",
                "seven",
                "8",
                "nine"
              )
            )
          )
        }
      ),
      suite("Tuple11")(
        testM("valid value") {
          for {
            result <- Task(
                        Tuple11(
                          StringInput,
                          LongInput,
                          StringInput,
                          LongInput,
                          StringInput,
                          LongInput,
                          StringInput,
                          LongInput,
                          StringInput,
                          LongInput,
                          StringInput
                        ).encode(("one", 2, "three", 4, "five", 6, "seven", 8, "nine", 10, "eleven"))
                      )
          } yield assert(result)(
            equalTo(
              respArgs(
                "one",
                "2",
                "three",
                "4",
                "five",
                "6",
                "seven",
                "8",
                "nine",
                "10",
                "eleven"
              )
            )
          )
        }
      ),
      suite("Update")(
        testM("set existing") {
          for {
            result <- Task(UpdateInput.encode(Update.SetExisting))
          } yield assert(result)(equalTo(respArgs("XX")))
        },
        testM("set new") {
          for {
            result <- Task(UpdateInput.encode(Update.SetNew))
          } yield assert(result)(equalTo(respArgs("NX")))
        }
      ),
      suite("Varargs")(
        testM("with multiple elements") {
          for {
            result <- Task(Varargs(LongInput).encode(List(1, 2, 3)))
          } yield assert(result)(equalTo(respArgs("1", "2", "3")))
        },
        testM("with no elements") {
          for {
            result <- Task(Varargs(LongInput).encode(List.empty))
          } yield assert(result)(isEmpty)
        }
      ),
      suite("WithScores")(
        testM("valid value") {
          for {
            result <- Task(WithScoresInput.encode(WithScores))
          } yield assert(result)(equalTo(respArgs("WITHSCORES")))
        }
      ),
      suite("WithCoord")(
        testM("valid value") {
          for {
            result <- Task(WithCoordInput.encode(WithCoord))
          } yield assert(result)(equalTo(respArgs("WITHCOORD")))
        }
      ),
      suite("WithDist")(
        testM("valid value") {
          for {
            result <- Task(WithDistInput.encode(WithDist))
          } yield assert(result)(equalTo(respArgs("WITHDIST")))
        }
      ),
      suite("WithHash")(
        testM("valid value") {
          for {
            result <- Task(WithHashInput.encode(WithHash))
          } yield assert(result)(equalTo(respArgs("WITHHASH")))
        }
      ),
      suite("Idle")(
        testM("with 1 second") {
          Task(IdleInput.encode(1.second))
            .map(assert(_)(equalTo(respArgs("IDLE", "1000"))))
        },
        testM("with 100 milliseconds") {
          Task(IdleInput.encode(100.millis))
            .map(assert(_)(equalTo(respArgs("IDLE", "100"))))
        },
        testM("with negative duration") {
          Task(IdleInput.encode((-1).second))
            .map(assert(_)(equalTo(respArgs("IDLE", "0"))))
        }
      ),
      suite("Time")(
        testM("with 1 second") {
          Task(TimeInput.encode(1.second))
            .map(assert(_)(equalTo(respArgs("TIME", "1000"))))
        },
        testM("with 100 milliseconds") {
          Task(TimeInput.encode(100.millis))
            .map(assert(_)(equalTo(respArgs("TIME", "100"))))
        },
        testM("with negative duration") {
          Task(TimeInput.encode((-1).second))
            .map(assert(_)(equalTo(respArgs("TIME", "0"))))
        }
      ),
      suite("RetryCount")(
        testM("with positive count") {
          Task(RetryCountInput.encode(100))
            .map(assert(_)(equalTo(respArgs("RETRYCOUNT", "100"))))
        },
        testM("with negative count") {
          Task(RetryCountInput.encode(-100))
            .map(assert(_)(equalTo(respArgs("RETRYCOUNT", "-100"))))
        }
      ),
      suite("XGroupCreate")(
        testM("without mkStream") {
          Task(XGroupCreateInput.encode(XGroupCommand.Create("key", "group", "id", mkStream = false)))
            .map(assert(_)(equalTo(respArgs("CREATE", "key", "group", "id"))))
        },
        testM("with mkStream") {
          Task(XGroupCreateInput.encode(XGroupCommand.Create("key", "group", "id", mkStream = true)))
            .map(assert(_)(equalTo(respArgs("CREATE", "key", "group", "id", "MKSTREAM"))))
        }
      ),
      suite("XGroupSetId")(
        testM("valid value") {
          Task(XGroupSetIdInput.encode(XGroupCommand.SetId("key", "group", "id")))
            .map(assert(_)(equalTo(respArgs("SETID", "key", "group", "id"))))
        }
      ),
      suite("XGroupDestroy")(
        testM("valid value") {
          Task(XGroupDestroyInput.encode(XGroupCommand.Destroy("key", "group")))
            .map(assert(_)(equalTo(respArgs("DESTROY", "key", "group"))))
        }
      ),
      suite("XGroupCreateConsumer")(
        testM("valid value") {
          Task(XGroupCreateConsumerInput.encode(XGroupCommand.CreateConsumer("key", "group", "consumer")))
            .map(assert(_)(equalTo(respArgs("CREATECONSUMER", "key", "group", "consumer"))))
        }
      ),
      suite("XGroupDelConsumer")(
        testM("valid value") {
          Task(XGroupDelConsumerInput.encode(XGroupCommand.DelConsumer("key", "group", "consumer")))
            .map(assert(_)(equalTo(respArgs("DELCONSUMER", "key", "group", "consumer"))))
        }
      ),
      suite("Block")(
        testM("with 1 second") {
          Task(BlockInput.encode(1.second))
            .map(assert(_)(equalTo(respArgs("BLOCK", "1000"))))
        },
        testM("with 100 milliseconds") {
          Task(BlockInput.encode(100.millis))
            .map(assert(_)(equalTo(respArgs("BLOCK", "100"))))
        },
        testM("with negative duration") {
          Task(BlockInput.encode((-1).second))
            .map(assert(_)(equalTo(respArgs("BLOCK", "0"))))
        }
      ),
      suite("Streams")(
        testM("with one pair") {
          Task(StreamsInput.encode(("a" -> "b", Chunk.empty)))
            .map(assert(_)(equalTo(respArgs("STREAMS", "a", "b"))))
        },
        testM("with multiple pairs") {
          Task(StreamsInput.encode(("a" -> "b", Chunk.single("c" -> "d"))))
            .map(assert(_)(equalTo(respArgs("STREAMS", "a", "c", "b", "d"))))
        }
      ),
      suite("Group")(
        testM("valid value") {
          Task(GroupInput.encode(Group("group", "consumer")))
            .map(assert(_)(equalTo(respArgs("GROUP", "group", "consumer"))))
        }
      ),
      suite("NoAck")(
        testM("valid value") {
          Task(NoAckInput.encode(NoAck)).map(assert(_)(equalTo(respArgs("NOACK"))))
        }
      ),
      suite("MaxLen")(
        testM("with approximate") {
          Task(MaxLenInput.encode(MaxLen(approximate = true, 10)))
            .map(assert(_)(equalTo(respArgs("MAXLEN", "~", "10"))))
        },
        testM("without approximate") {
          Task(MaxLenInput.encode(MaxLen(approximate = false, 10)))
            .map(assert(_)(equalTo(respArgs("MAXLEN", "10"))))
        }
      ),
      suite("WithForce")(
        testM("valid value") {
          Task(WithForceInput.encode(WithForce)).map(assert(_)(equalTo(respArgs("FORCE"))))
        }
      ),
      suite("WithJustId")(
        testM("valid value") {
          Task(WithJustIdInput.encode(WithJustId)).map(assert(_)(equalTo(respArgs("JUSTID"))))
        }
      )
    )

  private def respArgs(xs: String*): Chunk[RespValue] = Chunk.fromIterable(xs.map(RespValue.bulkString))
}
