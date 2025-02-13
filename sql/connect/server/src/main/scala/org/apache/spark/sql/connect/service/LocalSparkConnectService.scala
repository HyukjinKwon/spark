/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.connect.service

import java.io.File
import java.util.concurrent.TimeUnit

import scala.language.existentials

import io.grpc.Server
import io.grpc.netty.NettyServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import io.netty.channel.{EventLoopGroup, ServerChannel}
import io.netty.channel.epoll.{Epoll, EpollEventLoopGroup, EpollServerDomainSocketChannel}
import io.netty.channel.kqueue.{KQueue, KQueueEventLoopGroup, KQueueServerDomainSocketChannel}
import io.netty.channel.unix.DomainSocketAddress

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connect.common.config.ConnectCommon
import org.apache.spark.sql.connect.config.Connect.CONNECT_GRPC_MAX_INBOUND_MESSAGE_SIZE
import org.apache.spark.util.ThreadUtils

/**
 * Static instance of the LocalSparkConnectService.
 *
 * Used to start the local SparkConnect service with unix domain socket connection.
 */
object LocalSparkConnectService extends Logging {

  @volatile private var server: Server = _

  private var group: EventLoopGroup = _
  private val threadpool = ThreadUtils.newDaemonCachedThreadPool("local-spark-connect")

  private def getServerDomainSocketParams(): (EventLoopGroup, Class[_ <: ServerChannel]) = {
    // Determine which server domain socket channel to use based on the availability
    // of Epoll (on Linux) and KQueue (on MacOS) since Spark Connect local connection
    // needs to support both OS.
    if (Epoll.isAvailable) {
      (new EpollEventLoopGroup(), classOf[EpollServerDomainSocketChannel])
    } else if (KQueue.isAvailable) {
      (new KQueueEventLoopGroup(), classOf[KQueueServerDomainSocketChannel])
    } else {
      throw new UnsupportedOperationException("Unsupported OS for domain socket.")
    }
  }

  /**
   * Starts the GRPC Service.
   */
  private def startGRPCService(): Unit = {
    val debugMode = SparkEnv.get.conf.getBoolean("spark.connect.grpc.debug.enabled", true)
    val socketPath = ConnectCommon.getConnectSocketPath.get
    val sparkConnectSocketFile = new File(socketPath)

    val sparkConnectService = new SparkConnectService(debugMode)
    val protoReflectionService =
      if (debugMode) Some(ProtoReflectionService.newInstance()) else None
    val configuredInterceptors = SparkConnectInterceptorRegistry.createConfiguredInterceptors()
    val (eventLoopGroup, channelType) = getServerDomainSocketParams()
    group = eventLoopGroup

    val sb = NettyServerBuilder
      .forAddress(new DomainSocketAddress(sparkConnectSocketFile.getPath))
      .executor(threadpool)
      .channelType(channelType)
      .workerEventLoopGroup(group)
      .bossEventLoopGroup(group)
      .maxInboundMessageSize(SparkEnv.get.conf.get(CONNECT_GRPC_MAX_INBOUND_MESSAGE_SIZE).toInt)

    sb.addService(new SparkConnectBindableService(sparkConnectService))

    // Add all registered interceptors to the server builder.
    SparkConnectInterceptorRegistry.chainInterceptors(sb, configuredInterceptors)

    // If debug mode is configured, load the ProtoReflection service so that tools like
    // grpcurl can introspect the API for debugging.
    protoReflectionService.foreach(service => sb.addService(service))

    server = sb.build
    server.start()
  }

  // Starts the service
  def start(): Unit = {
    try {
      startGRPCService()
    } catch {
      case e: Exception =>
        logError("Could not start Spark Connect GRPC service", e)
        server = null
    }
  }

  def stop(timeout: Option[Long] = None, unit: Option[TimeUnit] = None): Unit = {
    if (server != null) {
      if (timeout.isDefined && unit.isDefined) {
        server.shutdown()
        server.awaitTermination(timeout.get, unit.get)
      } else {
        server.shutdownNow()
      }
    }
    if (group != null) {
      group.shutdownGracefully().sync()
    }
  }
}
