package com.guoteng;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.bootstrap.ServerBootstrap;

public class NettyServer {
	//与外界通信的端口号
	    private int port;
	    public NettyServer(int port) {
	       this.port = port;
	    }
	    public void run() throws Exception {
	       EventLoopGroup bossGroup = new NioEventLoopGroup(); 
	       EventLoopGroup workerGroup = new NioEventLoopGroup();
	       try {
	           ServerBootstrap b = new ServerBootstrap(); 
	           b.group(bossGroup, workerGroup)
	            .channel(NioServerSocketChannel.class) 
	            .childHandler(new ChannelInitializer<SocketChannel>() {
					@Override
					protected void initChannel(SocketChannel ch)
							throws Exception {
						//添加4个类用来处理和发送信息
						ch.pipeline().addLast(new TimeDecoder(),new XmlToString());
						ch.pipeline().addLast(new XmlStringToJson());
						ch.pipeline().addLast(new StreamMain().new Myhandler());
					}
	            })
	            .option(ChannelOption.SO_BACKLOG, 128)     
	            .childOption(ChannelOption.SO_KEEPALIVE, true); 
	         //绑定端口、同步等待
	           ChannelFuture f = b.bind(port).sync(); 
	           //等待服务监听端口关闭
	           f.channel().closeFuture().sync();
	       } finally {
	    	   //退出，释放线程等相关资源
	           workerGroup.shutdownGracefully();
	           bossGroup.shutdownGracefully();
	       }
	    }
	    public static void main(String[] args) throws Exception {
			NettyServer es=new NettyServer(8888);
			es.run();
		}
}
