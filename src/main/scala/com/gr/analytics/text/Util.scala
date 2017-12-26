package com.gr.analytics.text

import java.io.FileOutputStream
import java.nio.file.{Files, Path}
import java.util.zip.ZipInputStream

import scala.sys.process._

object Util {

  def loadAndUnzip(uri: String, zipFile: Path, destDir: Path): Path = {
    loadResource(uri, zipFile)
    unzip(zipFile, destDir)
    destDir
  }

  def loadResource(url: String, path: Path) = {
    // use 'wget' because it handles resource redirect codes
    val process: Process = s"wget -O ${path.toString} $url".run()
    // wait until download is finished
    process.exitValue()
    path
  }

  def unzip(path: Path, destDir: Path): Unit = {
    val zis = new ZipInputStream(Files.newInputStream(path))

    Stream.continually(zis.getNextEntry).takeWhile(_ != null).foreach { file =>
      if (!file.isDirectory) {
        val outPath = destDir.resolve(file.getName)
        val outPathParent = outPath.getParent
        if (!outPathParent.toFile.exists()) {
          outPathParent.toFile.mkdirs()
        }

        val outFile = outPath.toFile
        val out = new FileOutputStream(outFile)
        val buffer = new Array[Byte](4096)
        Stream.continually(zis.read(buffer)).takeWhile(_ != -1).foreach(out.write(buffer, 0, _))
      }
    }
  }
}

