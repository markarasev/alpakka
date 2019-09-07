/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs.impl.writer

import java.util

import akka.annotation.InternalApi
import akka.stream.alpakka.hdfs.FilePathGenerator
import akka.stream.alpakka.hdfs.impl.writer.HdfsWriter._
import akka.util.ByteString
import org.apache.hadoop.fs.{CreateFlag, FSDataOutputStream, FileContext, Options, Path}

/**
 * Internal API
 */
@InternalApi
private[writer] final case class DataWriter(
    override val fc: FileContext,
    override val pathGenerator: FilePathGenerator,
    maybeTargetPath: Option[Path],
    override val overwrite: Boolean
) extends HdfsWriter[FSDataOutputStream, ByteString] {

  override protected lazy val target: Path =
    getOrCreatePath(maybeTargetPath, createTargetPath(pathGenerator, 0))

  override def sync(): Unit = output.hsync()

  override def write(input: ByteString, separator: Option[Array[Byte]]): Long = {
    val bytes = input.toArray
    output.write(bytes)
    separator.foreach(output.write)
    output.size()
  }

  override def rotate(rotationCount: Long): DataWriter = {
    output.close()
    copy(maybeTargetPath = Some(createTargetPath(pathGenerator, rotationCount)))
  }

  override protected def create(fc: FileContext, file: Path): FSDataOutputStream = {
    val createFlag = util.EnumSet.of(CreateFlag.CREATE)
    if (overwrite) createFlag.add(CreateFlag.OVERWRITE)
    // FIXME defaults seems to be the same except for block size which transitions
    //  from 32MB to 64MB
    fc.create(file, createFlag, Options.CreateOpts.createParent())
  }

}

private[hdfs] object DataWriter {
  def apply(
      fc: FileContext,
      pathGenerator: FilePathGenerator,
      overwrite: Boolean
  ): DataWriter =
    new DataWriter(fc, pathGenerator, None, overwrite)
}
