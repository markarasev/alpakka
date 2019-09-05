/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs.impl.writer

import akka.annotation.InternalApi
import akka.stream.alpakka.hdfs.FilePathGenerator
import akka.stream.alpakka.hdfs.impl.writer.HdfsWriter._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{CompressionType, Writer}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.{SequenceFile, Writable}

/**
 * Internal API
 */
@InternalApi
private[writer] final case class SequenceWriter[K <: Writable, V <: Writable](
    override val fs: FileSystem,
    override val fc: FileContext,
    conf: Configuration, // FIXME deduplicate with fc
    writerOptions: Seq[Writer.Option],
    override val pathGenerator: FilePathGenerator,
    override val overwrite: Boolean,
    maybeTargetPath: Option[Path]
) extends HdfsWriter[SequenceFile.Writer, (K, V)] {

  override protected lazy val target: Path =
    getOrCreatePath(maybeTargetPath, createTargetPath(pathGenerator, 0))

  override def sync(): Unit = output.hsync()

  override def write(input: (K, V), separator: Option[Array[Byte]]): Long = {
    output.append(input._1, input._2)
    output.getLength
  }

  override def rotate(rotationCount: Long): SequenceWriter[K, V] = {
    output.close()
    copy(maybeTargetPath = Some(createTargetPath(pathGenerator, rotationCount)))
  }

  override protected def create(fc: FileContext, file: Path): Writer = {
    val ops = SequenceFile.Writer.file(file) +: writerOptions
    SequenceFile.createWriter(conf, ops: _*)
  }

}

/**
 * Internal API
 */
@InternalApi
private[hdfs] object SequenceWriter {
  def apply[K <: Writable, V <: Writable](
      fs: FileSystem,
      fc: FileContext,
      conf: Configuration,
      classK: Class[K],
      classV: Class[V],
      pathGenerator: FilePathGenerator,
      overwrite: Boolean
  ): SequenceWriter[K, V] =
    new SequenceWriter[K, V](
      fs,
      fc,
      conf,
      options(classK, classV),
      pathGenerator,
      overwrite,
      None
    )

  def apply[K <: Writable, V <: Writable](
      fs: FileSystem,
      fc: FileContext,
      conf: Configuration,
      compressionType: CompressionType,
      compressionCodec: CompressionCodec,
      classK: Class[K],
      classV: Class[V],
      pathGenerator: FilePathGenerator,
      overwrite: Boolean
  ): SequenceWriter[K, V] =
    new SequenceWriter[K, V](fs,
                             fc,
                             conf,
                             options(compressionType, compressionCodec, classK, classV),
                             pathGenerator,
                             overwrite,
                             None)

  private def options[K <: Writable, V <: Writable](
      classK: Class[K],
      classV: Class[V]
  ): Seq[Writer.Option] = Seq(
    SequenceFile.Writer.keyClass(classK),
    SequenceFile.Writer.valueClass(classV)
  )

  private def options[K <: Writable, V <: Writable](
      compressionType: CompressionType,
      compressionCodec: CompressionCodec,
      classK: Class[K],
      classV: Class[V]
  ): Seq[Writer.Option] = SequenceFile.Writer.compression(compressionType, compressionCodec) +: options(classK, classV)
}
