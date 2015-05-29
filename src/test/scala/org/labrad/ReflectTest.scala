package org.labrad

import java.util.Date
import org.labrad.annotations.{Accept, Return, Setting}
import org.labrad.data._
import org.labrad.manager.{ClientActor, Hub, ManagerImpl, ServerActor}
import org.labrad.types.Pattern
import org.labrad.util.Logging
import org.scalatest.FunSuite
import scala.reflect.ClassTag
import scala.reflect.runtime.{ currentMirror => cm, universe => ru }

abstract class Testable(val name: String) {
  def settings: Seq[(Long, String, String, String)]
}

class NoInput extends Testable("methods with no input params") {
  @Setting(id=1, name="noInput", doc="")
  def noInput(): Data = Data.NONE

  @Setting(id=2, name="noInput2", doc="")
  def noInput2(r: RequestContext): Data = Data.NONE

  val settings = Seq(
    (1L, "noInput", "", "?"),
    (2L, "noInput2", "", "?")
  )
}

class NoOutput extends Testable("methods with no output params") {
  @Setting(id=1, name="noOutput", doc="")
  def noOutput(r: RequestContext): Unit = {}

  @Setting(id=2, name="noOutput2", doc="")
  def noOutput2(r: RequestContext, arg: Data): Unit = {}

  @Setting(id=3, name="noOutput3", doc="")
  def noOutput3(r: RequestContext) {}

  @Setting(id=4, name="noOutput4", doc="")
  def noOutput4(r: RequestContext, arg: Data) {}

  val settings = Seq(
    (1L, "noOutput", "", ""),
    (2L, "noOutput2", "?", ""),
    (3L, "noOutput3", "", ""),
    (4L, "noOutput4", "?", "")
  )
}

class Primitives extends Testable("simple primitive input/output types") {
  @Setting(id=1, name="a", doc="")
  def a(r: RequestContext, b: Boolean): Boolean = !b

  @Setting(id=2, name="b", doc="")
  def b(r: RequestContext, i: Int): Int = i + 1

  @Setting(id=3, name="c", doc="")
  def c(r: RequestContext, l: Long): Long = l * 2

  @Setting(id=4, name="d", doc="")
  def d(r: RequestContext, v: Double): Double = math.sqrt(v)

  @Setting(id=5, name="e", doc="")
  def e(r: RequestContext, t: Date): Date = t

  @Setting(id=6, name="f", doc="")
  def f(r: RequestContext, s: String): String = s.reverse

  @Setting(id=7, name="g", doc="")
  def g(r: RequestContext, s: Array[Byte]): Array[Byte] = s ++ s

  val settings = Seq(
    (1L, "a", "b", "b"),
    (2L, "b", "i", "i"),
    (3L, "c", "w", "w"),
    (4L, "d", "v", "v"),
    (5L, "e", "t", "t"),
    (6L, "f", "s", "s"),
    (7L, "g", "s", "s")
  )
}

class Overloads extends Testable("overloaded methods") {
  @Setting(id=1, name="overloaded", doc="")
  def a(r: RequestContext): Data = a(r, "")
  def a(r: RequestContext, s: String): Data = a(r, s, false)
  def a(r: RequestContext, s: String, b: Boolean): Data = a(r, s, b, Data.NONE)
  def a(r: RequestContext, s: String, b: Boolean, d: Data): Data = Data.NONE

  val settings = Seq(
    (1L, "overloaded", "_|s|sb|sb?", "?")
  )
}

class Tuples extends Testable("translate tuples into clusters") {
  @Setting(id=1, name="a", doc="")
  def a(r: RequestContext,
        t1: Tuple1[Int],
        t2: (Int, String),
        t3: (Int, String, Int),
        t4: (Int, String, Int, Long),
        t5: (Int, String, Int, Long, Int),
        t6: (Int, String, Int, Long, Int, Date)): Data = Data.NONE

  val settings = Seq(
    (1L, "a", "(i)(is)(isi)(isiw)(isiwi)(isiwit)", "?")
  )
}

class Eithers extends Testable("translate Either into type alternatives") {
  @Setting(id=1, name="a", doc="")
  def a(r: RequestContext, id: Either[Long, String]): Data = Data.NONE

  @Setting(id=2, name="b", doc="")
  def b(r: RequestContext, id: Either[Either[Int, Long], String]): Data = Data.NONE

  val settings = Seq(
    (1L, "a", "w|s", "?"),
    (2L, "b", "i|w|s", "?")
  )
}

class Options extends Testable("translate Option into type or None") {
  @Setting(id=1, name="a", doc="")
  def a(r: RequestContext, name: Option[String]): Data = Data.NONE

  @Setting(id=2, name="b", doc="")
  def b(r: RequestContext, name: String, create: Option[Boolean]): Data = Data.NONE

  @Setting(id=3, name="c", doc="")
  def c(r: RequestContext, name: String, create: Option[Either[Long, String]]): Data = Data.NONE

  val settings = Seq(
    (1L, "a", "s|_", "?"),
    (2L, "b", "s|s<b|_>", "?"),
    (3L, "c", "s|s<w|s|_>", "?")
  )
}

class TypeAlias extends Testable("type aliases do not affect inference") {

  type Foo = String
  type Bar = Either[Long, Foo]

  @Setting(id=1, name="a", doc="")
  def a(name: Foo): Data = Data.NONE

  @Setting(id=2, name="b", doc="")
  def b(name: Bar): Data = Data.NONE

  @Setting(id=3, name="c", doc="")
  def c(): Bar = Right("blah")

  val settings = Seq(
    (1L, "a", "s", "?"),
    (2L, "b", "w|s", "?"),
    (3L, "c", "", "w|s")
  )
}

object Container {
  class Nested extends Testable("handle nested classes") {
    @Setting(id=1, name="a", doc="")
    def a(r: RequestContext, name: Option[String]): Data = Data.NONE

    @Setting(id=2, name="b", doc="")
    def b(r: RequestContext, name: String, create: Option[Boolean]): Data = Data.NONE

    @Setting(id=3, name="c", doc="")
    def c(r: RequestContext, name: String, create: Option[Either[Long, String]]): Data = Data.NONE

    val settings = Seq(
      (1L, "a", "s|_", "?"),
      (2L, "b", "s|s<b|_>", "?"),
      (3L, "c", "s|s<w|s|_>", "?")
    )
  }
}

class DefaultArgs extends Testable("allow parameters with default values to be omitted") {
  @Setting(id=1, name="a", doc="")
  def a(r: RequestContext, required: String, name: String = "test", value: Long = 5): Data = Data.NONE

  @Setting(id=2, name="b", doc="")
  def b(r: RequestContext, required: String, name: String = "test", value: Long): Data = Data.NONE

  @Setting(id=3, name="c", doc="")
  def c(r: RequestContext, name: Option[String] = None): Data = Data.NONE

  @Setting(id=4, name="d", doc="")
  def d(path: Seq[String] = Nil, create: Boolean = false): Seq[String] = path

  val settings = Seq(
    (1L, "a", "s | s<s|_> | s<s|_><w|_>", "?"),
    (2L, "b", "s<s|_>w", "?"),
    (3L, "c", "s | _", "?"),
    (4L, "d", "_ | *s | <*s|_><b|_>", "*s")
  )
}

class AcceptAnnotations extends Testable("allow parameters to be annotated with accepted types") {
  @Setting(id=1, name="a", doc="")
  def a(r: RequestContext, @Accept("v[s]") timeout: Double): Data = Data.NONE

  val settings = Seq(
    (1L, "a", "v[s]", "?")
  )
}

class ReturnAnnotations extends Testable("allow return types to be annotated") {
  @Setting(id=1, name="a", doc="")
  @Return("v[s]")
  def a(r: RequestContext, data: Data): Double = 0

  val settings = Seq(
    (1L, "a", "?", "v[s]")
  )
}

//class VarArgs extends Testable("handle varargs") {
//  @Setting(id=1, name="a", doc="")
//  def a(names: String*): Data = Data.NONE
//
//  @Setting(id=2, name="b", doc="")
//  def b(name: String, children: String*): Data = Data.NONE
//
//  @Setting(id=3, name="c", doc="")
//  def c(name: String, children: Data*): Data = Data.NONE
//
//  val settings = Seq(
//    (1L, "a", "*s", "?"),
//    (2L, "b", "s*s", "?"),
//    (3L, "c", "s*?", "?")
//  )
//}

class ReflectTests extends FunSuite with Logging {

  def same(pa: Pattern, pb: Pattern): Boolean = (pa accepts pb) && (pb accepts pa)

  def testSettings[A <: Testable : ClassTag : ru.TypeTag] {
    val inst = implicitly[ClassTag[A]].runtimeClass.newInstance.asInstanceOf[A]
    test(inst.name) {
      val settings = inst.settings
      val (found, _) = try {
        Reflect.makeHandler[A]
      } catch {
        case e: Throwable => log.error(s"error while making handler for $inst.name", e); throw e
      }
      assert(found.size == settings.size, "not all settings checked")
      val handlerMap = found.map(s => s.id -> s).toMap
      for ((id, name, acceptPat, returnPat) <- settings) {
        assert(found.exists(_.id == id), "expecting setting with id " + id)
        val setting = found.find(_.id == id).get
        assert(setting.name == name, s"incorrect name: expected $name, got ${setting.name}")
        assert(same(setting.accepts.pat, Pattern(acceptPat)), s"incorrect pattern: expected $acceptPat, got ${setting.accepts}")
        assert(same(setting.returns.pat, Pattern(returnPat)), s"incorrect pattern: expected $returnPat, got ${setting.returns}")
      }
    }
  }

  testSettings[NoInput]
  testSettings[NoOutput]
  testSettings[Primitives]
  testSettings[Overloads]
  testSettings[Tuples]
  testSettings[Eithers]
  testSettings[Options]
  testSettings[TypeAlias]
  testSettings[Container.Nested]
  testSettings[DefaultArgs]
  testSettings[AcceptAnnotations]
  testSettings[ReturnAnnotations]
  //testSettings[VarArgs]

  test("method mirror") {
    val inst = new Adder
    val tpe = ru.typeOf[Adder]
    val method = tpe.member(ru.TermName("add")).asMethod
    val add = cm.reflect(inst).reflectMethod(method)
    val result = add(1, 2).asInstanceOf[Int]
    assert(result == 3)
  }

  test("method mirror on ManagerImpl") {
    import scala.concurrent.Future
    import scala.concurrent.duration._

    val hub = new Hub {
      def allocateClientId(name: String): Long = 0
      def allocateServerId(name: String): Long = 0

      def connectClient(id: Long, name: String, handler: ClientActor): Unit = {}
      def connectServer(id: Long, name: String, handler: ServerActor): Unit = {}

      def disconnect(id: Long): Unit = {}
      def close(id: Long): Unit = {}

      def message(id: Long, packet: Packet): Unit = {}
      def request(id: Long, packet: Packet)(implicit timeout: Duration): Future[Packet] = null

      def expireContext(ctx: Context)(implicit timeout: Duration): Future[Long] = null
      def expireContext(id: Long, ctx: Context)(implicit timeout: Duration): Future[Long] = null
      def expireAll(id: Long, high: Long)(implicit timeout: Duration): Future[Long] = null

      def setServerInfo(info: ServerInfo): Unit = {}
      def serversInfo: Seq[ServerInfo] = Seq(
        // note, Manager gets added automatically by ManagerImpl
        ServerInfo(2L, "Registry", "the registry", Seq())
      )
      def serverInfo(id: Either[Long, String]): Option[ServerInfo] = None
    }

    val inst = new ManagerImpl(1L, "Manager", hub, null, null, null)
    val tpe = ru.typeOf[ManagerImpl]
    val method = tpe.member(ru.TermName("servers")).asMethod
    val getServers = cm.reflect(inst).reflectMethod(method)
    val reqCtx = RequestContext(1, Context(1, 1), 1, Data.NONE)
    val result = getServers(reqCtx).asInstanceOf[Seq[(Long, String)]]
    assert(result == Seq((1L, "Manager"), (2L, "Registry")))
  }
}

class Adder {
  def add(a: Int, b: Int) = a + b
}

class TypeTests extends FunSuite with Logging {

  def testInference[T: ru.TypeTag](expected: Pattern, values: T*): Unit = {
    import SettingHandler._

    val tpe = ru.typeOf[T]
    test(tpe.toString) {
      val (p, pack, unpack) = (patternFor(tpe), packer(tpe), unpacker(tpe))
      assert(p == expected)
      for (x <- values) {
        assert(unpack(pack(x)) == x)
      }
    }
  }

  def testInferenceArray[T <: Array[_] : ru.TypeTag](expected: Pattern, values: T*): Unit = {
    import SettingHandler._

    val tpe = ru.typeOf[T]
    test(tpe.toString) {
      val (p, pack, unpack) = (patternFor(tpe), packer(tpe), unpacker(tpe))
      assert(p == expected)
      for (x <- values) {
        val unpacked = unpack(pack(x)).asInstanceOf[T]
        assert(unpacked.length == x.length)
        for (i <- 0 until x.length) {
          assert(unpacked(i) == x(i))
        }
      }
    }
  }

  testInference[Unit](Pattern("_"), ())
  testInference[Boolean](Pattern("b"), true, false)
  testInference[Int](Pattern("i"), 0, 1, Int.MaxValue, Int.MinValue)
  testInference[String](Pattern("s"), "", "a", "\n\t")
  testInference[Seq[Byte]](Pattern("s"), Seq.tabulate[Byte](256)(_.toByte))
  testInference[Seq[Data]](Pattern("*?"), Seq(Str("abc")))
  testInference[Seq[String]](Pattern("*s"), Seq("abc"))
  testInference[Seq[Int]](Pattern("*i"), Seq(-2, -1, 0, 1, 2))
  testInference[Option[Boolean]](Pattern("b|_"), Some(true), Some(false), None)
  testInference[Either[Int, String]](Pattern("i|s"), Left(-1), Right("test"))
  testInference[Tuple1[Int]](Pattern("(i)"), Tuple1(1))
  testInference[(Int, String)](Pattern("is"), (1, ""), (-1, "a"))
  testInference[(Int, String, Option[Boolean])](Pattern("is<b|_>"), (1, "", None), (-1, "a", Some(true)))

  testInferenceArray[Array[Byte]](Pattern("s"), Array.tabulate[Byte](256)(_.toByte))
  testInferenceArray[Array[Data]](Pattern("*?"), Array(Str("abc")), Array())
  testInferenceArray[Array[String]](Pattern("*s"), Array("abc"), Array())
}

class InvokeTests extends FunSuite with Logging {
  val (_, bindDefaultArgs) = Reflect.makeHandler[DefaultArgs]

  test("can invoke method with some args having default values") {
    val defaultArgs = new DefaultArgs
    val handler = bindDefaultArgs(defaultArgs)
    val result = handler(RequestContext(source = 3, context = Context(3, 1), id = 4, data = Arr(Str("test"))))
    assert(result.get[Seq[String]] == Seq("test"))
  }

  test("can invoke method with None when all args have default values") {
    val defaultArgs = new DefaultArgs
    val handler = bindDefaultArgs(defaultArgs)
    val result = handler(RequestContext(source = 3, context = Context(3, 1), id = 4, data = Data.NONE))
    assert(result.get[Seq[String]] == Nil)
  }
}
