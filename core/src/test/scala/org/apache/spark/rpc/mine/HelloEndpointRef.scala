package org.apache.spark.rpc.mine

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  *
  *
  * @author pengwang
  * @date 2020/07/13
  */
object HelloEndpointRef {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val manager = new SecurityManager(conf)
    // 创建client模式的RpcEnv
    val rpcEnv: RpcEnv = RpcEnv.create("hello-server", "localhost", 5432, conf, manager, true)
    // 创建EndpointRef
    val endpointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress("localhost", 5432), "hello-service")
    val future: Future[String] = endpointRef.ask[String](SayHello("spark-rpc"))
    val s = Await.result(future, Duration.apply("30s"))
    print(s)
  }
}