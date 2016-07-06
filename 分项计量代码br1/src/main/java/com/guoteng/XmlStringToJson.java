package com.guoteng;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONObject;

import org.jdom.Attribute;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class XmlStringToJson extends ChannelInboundHandlerAdapter {

	public void channelRead(ChannelHandlerContext ctx, Object msg)
			throws Exception {
		System.out.println("111");
		JSONObject obj = new JSONObject();
		InputStream is = new ByteArrayInputStream(msg.toString().getBytes(
				"utf-8"));
		SAXBuilder sb = new SAXBuilder();
		Document doc = sb.build(is);
		Element root = doc.getRootElement();
		obj.put(root.getName(), iterateElement(root));
		ctx.fireChannelRead(obj.toString());
	}

	private static  Map getOrderAttribute(Element element,int i) {
		List jiedian = element.getChildren();
		Element et = null;
		Map obj = new HashMap();
		List list = new LinkedList();
		for(int j=0;j<jiedian.size();j++){
			et=(Element) jiedian.get(j);
			if(et.getAttributes().size()>0){
				Attribute attribute=(Attribute) et.getAttributes().get(i);
				if(obj.containsKey(et.getName()+attribute.getName())){
					list=(List) obj.get(et.getName()+attribute.getName());
				}
				list.add(attribute.getValue());
				obj.put(et.getName()+attribute.getName(), list);
			}
		}
		return obj;
	}

	private static Map iterateElement(Element element) {
		List jiedian = element.getChildren();
		Element et = null;
		Map obj = new HashMap();
		List list = null;
		for (int i = 0; i < jiedian.size(); i++) {
			list = new LinkedList();
			et = (Element) jiedian.get(i);
			if(et.getAttributes().size()>0){
				for(int x=0;x<et.getAttributes().size();x++){
					obj.putAll(getOrderAttribute(et.getParentElement(),x));
				}
			}
			if (et.getTextTrim().equals("")) {
				if (et.getChildren().size() == 0)
					continue;
				if (obj.containsKey(et.getName())) {
					list = (List) obj.get(et.getName());
				}
				list.add(iterateElement(et));
				obj.put(et.getName(), list);
			} else {
				if (obj.containsKey(et.getName())) {
					list = (List) obj.get(et.getName());
				}
				list.add(et.getTextTrim());
				obj.put(et.getName(), list);
			}
		}
		return obj;
	}
}
