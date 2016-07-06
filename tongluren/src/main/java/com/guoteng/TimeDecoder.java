package com.guoteng;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class TimeDecoder extends ByteToMessageDecoder { 

	//保证接收整个xml，不形成断片
	protected void decode(ChannelHandlerContext ctx, ByteBuf in,
			List<Object> out) throws Exception {
		// TODO Auto-generated method stub
		 if (in.readableBytes() < 1696) {
	            return; // (3)
	        }

	        out.add(in.readBytes(1696)); // (4)
	}
}
