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

package org.apache.spark.rpc.netty

import java.nio.ByteBuffer
import java.util.concurrent.Callable
import javax.annotation.concurrent.GuardedBy

import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.rpc.{RpcAddress, RpcEnvStoppedException}

private[netty] sealed trait OutboxMessage {

  def sendWith(client: TransportClient): Unit

  def onFailure(e: Throwable): Unit

}

/**
  * 不返回结果的消息
  * @param content
  */
private[netty] case class OneWayOutboxMessage(content: ByteBuffer) extends OutboxMessage
  with Logging {

  /**
    * 调用TransportClient的send方法发送消息
    * @param client
    */
  override def sendWith(client: TransportClient): Unit = {
    client.send(content)
  }

  override def onFailure(e: Throwable): Unit = {
    e match {
      case e1: RpcEnvStoppedException => logWarning(e1.getMessage)
      case e1: Throwable => logWarning(s"Failed to send one-way RPC.", e1)
    }
  }

}

/**
  * 需要返回结果的消息
  * @param content
  * @param _onFailure
  * @param _onSuccess
  */
private[netty] case class RpcOutboxMessage(
    content: ByteBuffer,
    _onFailure: (Throwable) => Unit,
    _onSuccess: (TransportClient, ByteBuffer) => Unit)
  extends OutboxMessage with RpcResponseCallback {

  private var client: TransportClient = _
  private var requestId: Long = _

  /**
    * 调用TransportClient的sendRpc方法发送消息
    * @param client
    */
  override def sendWith(client: TransportClient): Unit = {
    this.client = client
    this.requestId = client.sendRpc(content, this)
  }

  def onTimeout(): Unit = {
    require(client != null, "TransportClient has not yet been set.")
    client.removeRpcRequest(requestId)
  }

  override def onFailure(e: Throwable): Unit = {
    _onFailure(e)
  }

  override def onSuccess(response: ByteBuffer): Unit = {
    _onSuccess(client, response)
  }

}

/**
  * 发件箱对象
  * @param nettyEnv
  * @param address
  */
private[netty] class Outbox(nettyEnv: NettyRpcEnv, val address: RpcAddress) {

  outbox => // Give this an alias so we can use it more clearly in closures.

  // 保存消息的队列
  @GuardedBy("this")
  private val messages = new java.util.LinkedList[OutboxMessage]

  // TransportClient实例，它作为Netty的客户端，异步发送消息
  @GuardedBy("this")
  private var client: TransportClient = null

  /**
    * 表示新建server连接的异步结果
   * connectFuture points to the connect task. If there is no connect task, connectFuture will be
   * null.
   */
  @GuardedBy("this")
  private var connectFuture: java.util.concurrent.Future[Unit] = null

  @GuardedBy("this")
  private var stopped = false

  /**
    * 表示是否有线程正在发送消息。Outbox允许同时只有一个线程发送消息，所以在发送消息之前，都会判断draining的值
   * If there is any thread draining the message queue
   */
  @GuardedBy("this")
  private var draining = false

  /**
   * Send a message. If there is no active connection, cache it and launch a new connection. If
   * [[Outbox]] is stopped, the sender will be notified with a [[SparkException]].
   */
  def send(message: OutboxMessage): Unit = {
    // 如果rpc服务停止，则表示此条消息不能发送，需要丢弃
    val dropped = synchronized {
      if (stopped) {
        true
      } else {
        messages.add(message)
        false
      }
    }
    // 如果此条消息被舍弃，调用message的onFailure回调函数
    if (dropped) {
      message.onFailure(new SparkException("Message is dropped because Outbox is stopped"))
    } else {
      // drainOutbox定义了如何发送队列里的消息
      drainOutbox()
    }
  }

  /**
   * Drain the message queue. If there is other draining thread, just exit. If the connection has
   * not been established, launch a task in the `nettyEnv.clientConnectionExecutor` to setup the
   * connection.
   */
  private def drainOutbox(): Unit = {
    var message: OutboxMessage = null
    // 锁，防止发送消息的主线程，和创建连接的线程有冲突
    synchronized {
      if (stopped) {
        return
      }
      // connectFuture不为null，表示正在创建连接中，但未完成
      if (connectFuture != null) {
        // We are connecting to the remote address, so just exit
        return
      }
      // 如果connectFuture为null，client也为null，表示没有连接
      // 所以这儿提交创建新连接的任务
      if (client == null) {
        // There is no connect task but client is null, so we need to launch the connect task.
        launchConnectTask()
        return
      }
      // draining为true，表示有别的线程正在发送消息
      if (draining) {
        // There is some thread draining, so just exit
        return
      }
      // messages.poll返回null，表示消息队列都已经发送完成
      message = messages.poll()
      if (message == null) {
        return
      }
      // 更新draining为true， 表示消费消息的权利
      draining = true
    }
    while (true) {
      try {
        val _client = synchronized { client }
        if (_client != null) {
          // 调用消息的sendWith方法，发送消息
          message.sendWith(_client)
        } else {
          assert(stopped == true)
        }
      } catch {
        case NonFatal(e) =>
          handleNetworkFailure(e)
          return
      }
      synchronized {
        if (stopped) {
          return
        }
        // 循环从队列获取消息， 直到所有的消息发送完成
        message = messages.poll()
        if (message == null) {
          draining = false
          return
        }
      }
    }
  }

  private def launchConnectTask(): Unit = {
    // 提交连接任务给，nettyEnv的的线程池
    connectFuture = nettyEnv.clientConnectionExecutor.submit(new Callable[Unit] {

      override def call(): Unit = {
        try {
          // 实例化TransportClient
          val _client = nettyEnv.createClient(address)
          outbox.synchronized {
            client = _client
            if (stopped) {
              closeClient()
            }
          }
        } catch {
          case ie: InterruptedException =>
            // exit
            return
          case NonFatal(e) =>
            outbox.synchronized { connectFuture = null }
            handleNetworkFailure(e)
            return
        }
        outbox.synchronized { connectFuture = null }
        // It's possible that no thread is draining now. If we don't drain here, we cannot send the
        // messages until the next message arrives.
        // 连接完成后，处理堆积的消息
        drainOutbox()
      }
    })
  }

  /**
   * Stop [[Inbox]] and notify the waiting messages with the cause.
   */
  private def handleNetworkFailure(e: Throwable): Unit = {
    synchronized {
      assert(connectFuture == null)
      if (stopped) {
        return
      }
      stopped = true
      closeClient()
    }
    // Remove this Outbox from nettyEnv so that the further messages will create a new Outbox along
    // with a new connection
    nettyEnv.removeOutbox(address)

    // Notify the connection failure for the remaining messages
    //
    // We always check `stopped` before updating messages, so here we can make sure no thread will
    // update messages and it's safe to just drain the queue.
    var message = messages.poll()
    while (message != null) {
      message.onFailure(e)
      message = messages.poll()
    }
    assert(messages.isEmpty)
  }

  private def closeClient(): Unit = synchronized {
    // Just set client to null. Don't close it in order to reuse the connection.
    client = null
  }

  /**
   * Stop [[Outbox]]. The remaining messages in the [[Outbox]] will be notified with a
   * [[SparkException]].
   */
  def stop(): Unit = {
    synchronized {
      if (stopped) {
        return
      }
      stopped = true
      if (connectFuture != null) {
        connectFuture.cancel(true)
      }
      closeClient()
    }

    // We always check `stopped` before updating messages, so here we can make sure no thread will
    // update messages and it's safe to just drain the queue.
    var message = messages.poll()
    while (message != null) {
      message.onFailure(new SparkException("Message is dropped because Outbox is stopped"))
      message = messages.poll()
    }
  }
}
