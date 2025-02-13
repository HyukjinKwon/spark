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
package org.apache.spark.sql.connect.client

import scala.language.existentials

import io.grpc.{ChannelCredentials, ManagedChannelBuilder}
import io.grpc.netty.NettyChannelBuilder
import io.netty.channel.{Channel, EventLoopGroup}
import io.netty.channel.epoll.{Epoll, EpollDomainSocketChannel, EpollEventLoopGroup}
import io.netty.channel.kqueue.{KQueue, KQueueDomainSocketChannel, KQueueEventLoopGroup}
import io.netty.channel.unix.DomainSocketAddress

/**
 * To build SparkConnectClient with local connect.
 */

object LocalClientBuilder {
  private def getClientDomainSocketParams(): (EventLoopGroup, Class[_ <: Channel]) = {
    // Determine which domain socket channel to use based on the availability
    // of Epoll (on Linux) and KQueue (on MacOS) since Spark Connect local connection
    // needs to support both OS.
    if (Epoll.isAvailable) {
      (new EpollEventLoopGroup(), classOf[EpollDomainSocketChannel])
    } else if (KQueue.isAvailable) {
      (new KQueueEventLoopGroup(), classOf[KQueueDomainSocketChannel])
    } else {
      throw new UnsupportedOperationException("Unsupported OS for domain socket.")
    }
  }

  def newChannelBuilderForDomainSocket(
      socketPath: String,
      creds: ChannelCredentials): ManagedChannelBuilder[_] = {
    val (eventLoopGroup, channelType) = getClientDomainSocketParams()
    val channelBuilder = NettyChannelBuilder
      .forAddress(new DomainSocketAddress(socketPath), creds)
      .channelType(channelType)
      .eventLoopGroup(eventLoopGroup)
    channelBuilder
  }
}
