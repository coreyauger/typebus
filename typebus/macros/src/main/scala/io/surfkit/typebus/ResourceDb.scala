package io.surfkit.typebus

import java.nio.file.{Files, Path, Paths}

trait ResourceDb{

  def databaseTablePath(key: String): Path = {
    val root = Paths.get( this.getClass.getResource("/").getPath )
    // We want the resource path before compile
    val db = Paths.get( root.toString + "/../../../src/main/resources/typebus" )
    if(Files.notExists(db))
      Files.createDirectory(db)
    Paths.get(db + s"/${key}")
  }

}