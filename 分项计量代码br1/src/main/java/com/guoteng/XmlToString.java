package com.guoteng;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class XmlToString extends ChannelInboundHandlerAdapter{ 
	
	private ByteBuf buf;

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        buf = ctx.alloc().buffer(1692); // (1)
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        buf.release(); // (1)
        buf = null;
    }
    
    public void channelRead(ChannelHandlerContext ctx, Object msg) { 
    	ByteBuf buf = (ByteBuf) msg;
		byte[] result = new byte[buf.readableBytes()];
		buf.readBytes(result);
		String resultStr = new String(result);
		ctx.fireChannelRead(resultStr);
    }

    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    	System.out.println("*");
    	ctx.flush();
    }

}
