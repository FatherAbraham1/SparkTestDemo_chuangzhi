package com.guoteng;

import java.util.List;

public class Entity{
	private Root root;

	public Root getRoot() {
		return root;
	}

	public void setRoot(Root root) {
		this.root = root;
	}
}

class Root {
	private List<Common> common;

	private List<Data> data;

	private List<String> dataoperation;

	public List<String> getDataoperation() {
		return dataoperation;
	}

	public void setDataoperation(List<String> dataoperation) {
		this.dataoperation = dataoperation;
	}

	public List<Common> getCommon() {
		return common;
	}

	public void setCommon(List<Common> common) {
		this.common = common;
	}

	public List<Data> getData() {
		return data;
	}

	public void setData(List<Data> data) {
		this.data = data;
	}
}

class Common {
	private List<String> gateway_id;
	private List<String> building_id;
	private List<String> type;
	public List<String> getGateway_id() {
		return gateway_id;
	}
	public void setGateway_id(List<String> gateway_id) {
		this.gateway_id = gateway_id;
	}
	public List<String> getBuilding_id() {
		return building_id;
	}
	public void setBuilding_id(List<String> building_id) {
		this.building_id = building_id;
	}
	public List<String> getType() {
		return type;
	}
	public void setType(List<String> type) {
		this.type = type;
	}
}

class Data {
	private List<String> total;

	private List<Meter> meter;

	private List<String> time;

	private List<String> meterid;

	private List<String> sequence;

	private List<String> current;

	private List<String> parser;

	public List<String> getTotal() {
		return total;
	}

	public void setTotal(List<String> total) {
		this.total = total;
	}

	public List<Meter> getMeter() {
		return meter;
	}

	public void setMeter(List<Meter> meter) {
		this.meter = meter;
	}

	public List<String> getTime() {
		return time;
	}

	public void setTime(List<String> time) {
		this.time = time;
	}

	public List<String> getMeterid() {
		return meterid;
	}

	public void setMeterid(List<String> meterid) {
		this.meterid = meterid;
	}

	public List<String> getSequence() {
		return sequence;
	}

	public void setSequence(List<String> sequence) {
		this.sequence = sequence;
	}

	public List<String> getCurrent() {
		return current;
	}

	public void setCurrent(List<String> current) {
		this.current = current;
	}

	public List<String> getParser() {
		return parser;
	}

	public void setParser(List<String> parser) {
		this.parser = parser;
	}
}

class Meter {
	private List<String> functioncoding;

	private List<String> functionid;

	private List<String> functionerror;

	private List<String> function;

	public List<String> getFunctioncoding() {
		return functioncoding;
	}

	public void setFunctioncoding(List<String> functioncoding) {
		this.functioncoding = functioncoding;
	}

	public List<String> getFunctionid() {
		return functionid;
	}

	public void setFunctionid(List<String> functionid) {
		this.functionid = functionid;
	}

	public List<String> getFunctionerror() {
		return functionerror;
	}

	public void setFunctionerror(List<String> functionerror) {
		this.functionerror = functionerror;
	}

	public List<String> getFunction() {
		return function;
	}

	public void setFunction(List<String> function) {
		this.function = function;
	}
}