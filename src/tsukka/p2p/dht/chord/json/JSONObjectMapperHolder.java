package tsukka.p2p.dht.chord.json;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JSONObjectMapperHolder {
    private static class Holder{
        private static final ObjectMapper INSTANCE=new ObjectMapper();
    }
    public static ObjectMapper getInstance(){
        return Holder.INSTANCE;
    }
}
