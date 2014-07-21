package spark.hbase.debugging

import io.netty.channel.nio.NioEventLoopGroup
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.channel.ChannelOption
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelHandlerContext
import io.netty.buffer.ByteBuf

object SimpleNettyServer {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.out.println("SimpleNettyServer {host} {port} ");
      return ;
    }

    val host = args(0);
    val port = args(1);

    run(Integer.parseInt(port));

  }

  def run(port: Integer) {
    val bossGroup = new NioEventLoopGroup(); // (1)
    val workerGroup = new NioEventLoopGroup();
    try {
      val b = new ServerBootstrap(); // (2)
      b.group(bossGroup, workerGroup)
        .channel(classOf[NioServerSocketChannel]) // (3)
        .childHandler(new ChannelInitializer[SocketChannel]() { // (4)
          override def initChannel(ch: SocketChannel) {
            ch.pipeline().addLast(new DiscardServerHandler());
          }
        })
        //.option(ChannelOption.SO_BACKLOG, 128) // (5)
        //.childOption(ChannelOption.SO_KEEPALIVE, true); // (6)

      // Bind and start to accept incoming connections.
      println("about to start server")
      val f = b.bind(port).sync(); // (7)
      println("killed server")
      // Wait until the server socket is closed.
      // In this example, this does not happen, but you can do that to gracefully
      // shut down your server.
      f.channel().closeFuture().sync();
    } finally {
      workerGroup.shutdownGracefully();
      bossGroup.shutdownGracefully();
    }
  }
  
  class DiscardServerHandler extends ChannelInboundHandlerAdapter { // (1)

    override def channelRead(ctx: ChannelHandlerContext , msg: Object) { // (2)
        // Discard the received data silently.
        //((ByteBuf) msg).release(); // (3)
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) { // (4)
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }
  }
}