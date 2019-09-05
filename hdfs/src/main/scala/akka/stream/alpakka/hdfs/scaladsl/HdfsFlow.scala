/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs.scaladsl

import akka.NotUsed
import akka.stream.alpakka.hdfs._
import akka.stream.alpakka.hdfs.impl.HdfsFlowStage
import akka.stream.alpakka.hdfs.impl.writer.{CompressedDataWriter, DataWriter, SequenceWriter}
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import org.apache.hadoop.fs.{FSDataOutputStream, FileContext, FileSystem}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.{SequenceFile, Writable}

object HdfsFlow {

  private[hdfs] val OnlyRotationMessage: PartialFunction[OutgoingMessage[_], RotationMessage] = {
    case m: RotationMessage => m
  }

  /**
   * Scala API: creates a Flow for [[org.apache.hadoop.fs.FSDataOutputStream]]
   *
   * @param fs Hadoop file system
   * @param fc Hadoop file context
   * @param syncStrategy sync strategy
   * @param rotationStrategy rotation strategy
   * @param settings hdfs writing settings
   */
  def data(
      fs: FileSystem,
      fc: FileContext,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      settings: HdfsWritingSettings
  ): Flow[HdfsWriteMessage[ByteString, NotUsed], RotationMessage, NotUsed] =
    dataWithPassThrough[NotUsed](fs, fc, syncStrategy, rotationStrategy, settings)
      .collect(OnlyRotationMessage)

  /**
   * Scala API: creates a Flow for [[org.apache.hadoop.fs.FSDataOutputStream]]
   * with `passThrough` of type `C`
   *
   * @param fs Hadoop file system
   * @param fc Hadoop file context
   * @param syncStrategy sync strategy
   * @param rotationStrategy rotation strategy
   * @param settings hdfs writing settings
   */
  def dataWithPassThrough[P](
      fs: FileSystem,
      fc: FileContext,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      settings: HdfsWritingSettings
  ): Flow[HdfsWriteMessage[ByteString, P], OutgoingMessage[P], NotUsed] =
    Flow
      .fromGraph(
        new HdfsFlowStage[FSDataOutputStream, ByteString, P](
          syncStrategy,
          rotationStrategy,
          settings,
          DataWriter(fs, fc, settings.pathGenerator, settings.overwrite)
        )
      )

  /**
   * Scala API: creates a Flow for [[org.apache.hadoop.io.compress.CompressionOutputStream]]
   *
   * @param fs Hadoop file system
   * @param fc Hadoop file context
   * @param syncStrategy sync strategy
   * @param rotationStrategy rotation strategy
   * @param compressionCodec a streaming compression/decompression pair
   * @param settings hdfs writing settings
   */
  def compressed(
      fs: FileSystem,
      fc: FileContext,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      compressionCodec: CompressionCodec,
      settings: HdfsWritingSettings
  ): Flow[HdfsWriteMessage[ByteString, NotUsed], RotationMessage, NotUsed] =
    compressedWithPassThrough[NotUsed](
      fs,
      fc,
      syncStrategy,
      rotationStrategy,
      compressionCodec,
      settings
    ).collect(OnlyRotationMessage)

  /**
   * Scala API: creates a Flow for [[org.apache.hadoop.io.compress.CompressionOutputStream]]
   * with `passThrough` of type `C`
   *
   * @param fs Hadoop file system
   * @param fc Hadoop file context
   * @param syncStrategy sync strategy
   * @param rotationStrategy rotation strategy
   * @param compressionCodec a streaming compression/decompression pair
   * @param settings hdfs writing settings
   */
  def compressedWithPassThrough[P](
      fs: FileSystem,
      fc: FileContext,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      compressionCodec: CompressionCodec,
      settings: HdfsWritingSettings
  ): Flow[HdfsWriteMessage[ByteString, P], OutgoingMessage[P], NotUsed] =
    Flow
      .fromGraph(
        new HdfsFlowStage[FSDataOutputStream, ByteString, P](
          syncStrategy,
          rotationStrategy,
          settings,
          CompressedDataWriter(
            fs,
            fc,
            compressionCodec,
            settings.pathGenerator,
            settings.overwrite
          )
        )
      )

  /**
   * Scala API: creates a Flow for [[org.apache.hadoop.io.SequenceFile.Writer]]
   * without a compression
   *
   * @param fs Hadoop file system
   * @param fc Hadoop file context
   * @param syncStrategy sync strategy
   * @param rotationStrategy rotation strategy
   * @param settings hdfs writing settings
   * @param classK a key class
   * @param classV a value class
   */
  def sequence[K <: Writable, V <: Writable](
      fs: FileSystem,
      fc: FileContext,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      settings: HdfsWritingSettings,
      classK: Class[K],
      classV: Class[V]
  ): Flow[HdfsWriteMessage[(K, V), NotUsed], RotationMessage, NotUsed] =
    sequenceWithPassThrough[K, V, NotUsed](
      fs,
      fc,
      syncStrategy,
      rotationStrategy,
      settings,
      classK,
      classV
    ).collect(OnlyRotationMessage)

  /**
   * Scala API: creates a Flow for [[org.apache.hadoop.io.SequenceFile.Writer]]
   * with a compression
   *
   * @param fs Hadoop file system
   * @param fc Hadoop file context
   * @param syncStrategy sync strategy
   * @param rotationStrategy rotation strategy
   * @param compressionType a compression type used to compress key/value pairs in the SequenceFile
   * @param compressionCodec a streaming compression/decompression pair
   * @param settings hdfs writing settings
   * @param classK a key class
   * @param classV a value class
   */
  def sequence[K <: Writable, V <: Writable](
      fs: FileSystem,
      fc: FileContext,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      compressionType: CompressionType,
      compressionCodec: CompressionCodec,
      settings: HdfsWritingSettings,
      classK: Class[K],
      classV: Class[V]
  ): Flow[HdfsWriteMessage[(K, V), NotUsed], RotationMessage, NotUsed] =
    sequenceWithPassThrough[K, V, NotUsed](
      fs,
      fc,
      syncStrategy,
      rotationStrategy,
      compressionType,
      compressionCodec,
      settings,
      classK,
      classV
    ).collect(OnlyRotationMessage)

  /**
   * Scala API: creates a Flow for [[org.apache.hadoop.io.SequenceFile.Writer]]
   * with `passThrough` of type `C` and without a compression
   *
   * @param fs sync strategy
   * @param fc Hadoop file context
   * @param syncStrategy sync strategy
   * @param rotationStrategy rotation strategy
   * @param settings Hdfs writing settings
   * @param classK a key class
   * @param classV a value class
   */
  def sequenceWithPassThrough[K <: Writable, V <: Writable, P](
      fs: FileSystem,
      fc: FileContext,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      settings: HdfsWritingSettings,
      classK: Class[K],
      classV: Class[V]
  ): Flow[HdfsWriteMessage[(K, V), P], OutgoingMessage[P], NotUsed] =
    Flow
      .fromGraph(
        new HdfsFlowStage[SequenceFile.Writer, (K, V), P](
          syncStrategy,
          rotationStrategy,
          settings,
          SequenceWriter(fs, fc, fs.getConf, classK, classV, settings.pathGenerator, settings.overwrite)
        )
      )

  /**
   * Scala API: creates a Flow for [[org.apache.hadoop.io.SequenceFile.Writer]]
   * with `passThrough` of type `C` and a compression
   *
   * @param fs Hadoop file system
   * @param fc Hadoop file context
   * @param syncStrategy sync strategy
   * @param rotationStrategy rotation strategy
   * @param compressionType a compression type used to compress key/value pairs in the SequenceFile
   * @param compressionCodec a streaming compression/decompression pair
   * @param settings hdfs writing settings
   * @param classK a key class
   * @param classV a value class
   */
  def sequenceWithPassThrough[K <: Writable, V <: Writable, P](
      fs: FileSystem,
      fc: FileContext,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      compressionType: CompressionType,
      compressionCodec: CompressionCodec,
      settings: HdfsWritingSettings,
      classK: Class[K],
      classV: Class[V]
  ): Flow[HdfsWriteMessage[(K, V), P], OutgoingMessage[P], NotUsed] =
    Flow
      .fromGraph(
        new HdfsFlowStage[SequenceFile.Writer, (K, V), P](
          syncStrategy,
          rotationStrategy,
          settings,
          SequenceWriter(fs,
                         fc,
                         fs.getConf,
                         compressionType,
                         compressionCodec,
                         classK,
                         classV,
                         settings.pathGenerator,
                         settings.overwrite)
        )
      )
}
