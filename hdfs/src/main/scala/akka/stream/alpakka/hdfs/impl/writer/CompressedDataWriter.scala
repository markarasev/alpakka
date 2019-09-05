/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs.impl.writer

import java.util

import akka.annotation.InternalApi
import akka.stream.alpakka.hdfs.FilePathGenerator
import akka.stream.alpakka.hdfs.impl.writer.HdfsWriter._
import akka.util.ByteString
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.{CreateFlag, FSDataOutputStream, FileContext, FileSystem, Options, Path}
import org.apache.hadoop.io.compress.{CodecPool, CompressionCodec, CompressionOutputStream, Compressor}

/**
 * Internal API
 */
@InternalApi
private[writer] final case class CompressedDataWriter(
    override val fs: FileSystem,
    override val fc: FileContext,
    compressionCodec: CompressionCodec,
    override val pathGenerator: FilePathGenerator,
    maybeTargetPath: Option[Path],
    override val overwrite: Boolean
) extends HdfsWriter[FSDataOutputStream, ByteString] {

  override protected lazy val target: Path = getOrCreatePath(maybeTargetPath, outputFileWithExtension(0))

  private val compressor: Compressor = CodecPool.getCompressor(compressionCodec, fs.getConf)
  private val cmpOutput: CompressionOutputStream = compressionCodec.createOutputStream(output, compressor)

  require(compressor ne null, "Compressor cannot be null")

  override def sync(): Unit = output.hsync()

  override def write(input: ByteString, separator: Option[Array[Byte]]): Long = {
    val bytes = input.toArray
    cmpOutput.write(bytes)
    separator.foreach(output.write)
    compressor.getBytesWritten
  }

  override def rotate(rotationCount: Long): CompressedDataWriter = {
    cmpOutput.finish()
    output.close()
    copy(maybeTargetPath = Some(outputFileWithExtension(rotationCount)))
  }

  override protected def create(fc: FileContext, file: Path): FSDataOutputStream = {
    val createFlag = util.EnumSet.of(CreateFlag.CREATE)
    if (overwrite) createFlag.add(CreateFlag.OVERWRITE)
    // FIXME defaults seems to be the same except for block size which transitions
    //  from 32MB to 64MB
    fc.create(file, createFlag, Options.CreateOpts.createParent())
  }

  private def outputFileWithExtension(rotationCount: Long): Path = {
    val candidatePath = createTargetPath(pathGenerator, rotationCount)
    val candidateExtension = s".${FilenameUtils.getExtension(candidatePath.getName)}"
    val codecExtension = compressionCodec.getDefaultExtension
    if (codecExtension != candidateExtension)
      candidatePath.suffix(codecExtension)
    else candidatePath
  }

}

/**
 * Internal API
 */
@InternalApi
private[hdfs] object CompressedDataWriter {
  def apply(
      fs: FileSystem,
      fc: FileContext,
      compressionCodec: CompressionCodec,
      pathGenerator: FilePathGenerator,
      overwrite: Boolean
  ): CompressedDataWriter =
    new CompressedDataWriter(fs, fc, compressionCodec, pathGenerator, None, overwrite)
}
