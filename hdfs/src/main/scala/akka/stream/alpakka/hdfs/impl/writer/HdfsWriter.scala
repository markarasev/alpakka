/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs.impl.writer

import akka.annotation.InternalApi
import akka.stream.alpakka.hdfs.FilePathGenerator
import akka.stream.alpakka.hdfs.impl.writer.HdfsWriter._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileContext, FileSystem, Options, Path}

/**
 * Internal API
 */
@InternalApi
private[hdfs] trait HdfsWriter[W, I] {

  protected lazy val output: W = create(fc, temp)

  protected lazy val temp: Path = tempFromTarget(pathGenerator, target)

  def moveToTarget(): Unit = {
    fc.mkdir(target.getParent, FsPermission.getDirDefault, true)
    fc.rename(temp, target, Options.Rename.OVERWRITE)
  }

  def sync(): Unit

  def targetPath: String = target.toString

  def write(input: I, separator: Option[Array[Byte]]): Long

  def rotate(rotationCount: Long): HdfsWriter[W, I]

  protected def target: Path

  // FIXME remove when finished
  @deprecated
  protected def fs: FileSystem

  protected def fc: FileContext

  protected def overwrite: Boolean

  protected def pathGenerator: FilePathGenerator

  protected def create(fc: FileContext, file: Path): W

}

/**
 * Internal API
 */
@InternalApi
private[writer] object HdfsWriter {

  def createTargetPath(generator: FilePathGenerator, c: Long): Path =
    generator(c, System.currentTimeMillis / 1000)

  def tempFromTarget(generator: FilePathGenerator, target: Path): Path =
    new Path(generator.tempDirectory, target.getName)

  def getOrCreatePath(maybePath: Option[Path], default: => Path): Path =
    maybePath.getOrElse(default)

}
