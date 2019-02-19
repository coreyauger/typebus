package io.surfkit.typebus.cli

import java.io.File
import java.nio.file.Files
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._
import scala.util.Try
import io.surfkit.typebus._

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


  def runCli(busType: String = "Kafka") = {

    val config = ConfigFactory.load()

    @inline def defined(line: String) = {
      line != null && line.nonEmpty
    }
    print("typebus> ")
    Iterator.continually(scala.io.StdIn.readLine).takeWhile(defined(_)).foreach { line =>
      println("cli read: " + line)
      CommandParser.parse(line.split(' ')).foreach{
        case cmd if cmd.command == Cmd.CodeGen =>
          val genCmd = cmd.gen
          if(genCmd.service != ""){
            // TODO: how do we do this one?
            //ZookeeperClusterSeed(system).join()
            //new GeneratorService
          }else if(genCmd.push){
            // get the project to push to
            println("push code gen...")
            val generated = gen.selfCodeGen
            genCmd.out.foreach { outFile =>
              gen.ScalaCodeWriter.writeCodeToFiles(busType, generated, outFile.getAbsolutePath.split('/').toList )
            }
            val pushPaths = Try(config.getStringList("bus.code-gen.push").toList).toOption.getOrElse(List.empty[String])
            val projectDir = new java.io.File(".").toPath
            println(s"pushPaths: ${projectDir.toAbsolutePath} ${pushPaths}")
            pushPaths.foreach{ part =>
              val scalaSrcPath = projectDir.resolve(part + "/src/main/scala")
              if(Files.isDirectory(scalaSrcPath)){
                println(s"pushing code to: ${scalaSrcPath}")
                gen.ScalaCodeWriter.writeCodeToFiles(busType, generated, scalaSrcPath.toAbsolutePath.toString.split('/').toList )
              }
            }
          }else if(!genCmd.target.isEmpty){
            val target = genCmd.target.get
            val generated = gen.codeGen(target.toPath)
            genCmd.out.foreach { outFile =>
              gen.ScalaCodeWriter.writeCodeToFiles(busType, generated, outFile.getAbsolutePath.split('/').toList )
            }
          }


        case cmd =>
          println(s"Unknown or unsupported command. ${cmd}")
      }
      print("typebus> ")
    }
  }
}
