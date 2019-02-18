package io.surfkit.typebus.cli

import java.io.File

object CommandParser {

  sealed trait Cmd
  object Cmd {
    object None extends Cmd
    object CodeGen extends Cmd
  }

  final case class CodeGenOptions(
         out: Option[File] = None,
         target: Option[File] = None,
         service: String = "",
         push: Boolean = false)

  case class CmdConfig(command: Cmd = Cmd.None,
                       file: File = new File("."),
                       gen: CodeGenOptions = CodeGenOptions(),
                       out: File = new File("."),
                       verbose: Boolean = false,
                       debug: Boolean = false,
                       files: Seq[File] = Seq(),
                       jars: Seq[File] = Seq(), kwargs: Map[String,String] = Map())

  private[this] val parser = new scopt.OptionParser[CmdConfig]("cli") {
    head("typebus cli", "0.0.1")

    opt[File]('f', "file").valueName("<file>")
      .action( (x, c) => c.copy(file = x) )
      .text("input file")

    opt[Seq[File]]('j', "jars").valueName("<jar1>,<jar2>...").action( (x,c) =>
      c.copy(jars = x) ).text("jars to include")

    opt[Map[String,String]]("kwargs").valueName("k1=v1,k2=v2...").action( (x, c) =>
      c.copy(kwargs = x) ).text("other arguments")

    opt[Unit]("verbose").action( (_, c) =>
      c.copy(verbose = true) ).text("verbose is a flag")

    opt[Unit]("debug").hidden().action( (_, c) =>
      c.copy(debug = true) ).text("this option is hidden in the usage text")

    help("help").text("prints this usage text")

    note("typebus helper cli.. Feed me !!!\n")

    cmd("gen").required().action( (_, c) => c.copy(command = Cmd.CodeGen) ).
      text("perform code generation.").
      children(
        opt[File]("target").abbr("t").action( (f, c) =>
          c.copy(gen = c.gen.copy(target = Some(f) )) ).text("Target resource/typebus directory to perform code gen on"),
        opt[File]("out").abbr("o").action( (o, c) =>
          c.copy(gen = c.gen.copy(out = Some(o) )) ).text("output to a directory (defaults to this projects source directory)"),
        opt[String]("service").abbr("s").action( (s, c) =>
          c.copy(gen = c.gen.copy(service = s )) ).text("contact service over typebus to code gen from"),
        opt[Unit]("push").abbr("p").action( (s, c) =>
          c.copy(gen = c.gen.copy(push = true )) ).text("push codegen to services defined in application.conf")
      )
  }


  def parse(args: Array[String]):Option[CmdConfig] =
    parser.parse(args, CmdConfig())

}
