package tsukka.p2p.dht.chord.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import javax.naming.spi.ObjectFactory;

public class JSONObjectMapperHolder {
    private static class Holder{
        private static final ObjectMapper MAPPER=new ObjectMapper();
        private static final JsonFactory FACTORY=MAPPER.getFactory();
        private static final JsonNodeFactory NODE_FACTORY=MAPPER.getNodeFactory();
    }
    public static ObjectMapper getObjectMapper(){
        return Holder.MAPPER;
    }
    public static JsonFactory getJsonFactory(){
        return Holder.FACTORY;
    }
    public static JsonNodeFactory getJsonNodeFactory(){
        return Holder.NODE_FACTORY;
    }
}
