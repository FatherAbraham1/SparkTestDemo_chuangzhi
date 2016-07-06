package com.guoteng;
import java.io.IOException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Parse {
  public static Entity parseJsonToRoot(String jsonLine) {
	  ObjectMapper objectMapper = new ObjectMapper();
	    Entity entity= null;

	    try {
	    	entity = objectMapper.readValue(jsonLine, Entity.class);
	    } catch (IOException e) {
	      e.printStackTrace();
	    }
	    return entity;
	  }
}
