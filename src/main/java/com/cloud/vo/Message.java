package com.cloud.vo;

import java.io.Serializable;

public class Message implements Serializable {

	private static final long serialVersionUID = 4631291403935326789L;

	private String topic;
	
	private String message;
	
	public Message(String topic, String message) {
		this.topic = topic;
		this.message = message;
	}

	public String topic() {
		return topic;
	}

	public String message() {
		return message;
	}
}