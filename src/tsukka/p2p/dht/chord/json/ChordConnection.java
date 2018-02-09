package tsukka.p2p.dht.chord.json;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.*;

import java.net.InetSocketAddress;

@Value
public class ChordConnection {
    InetSocketAddress socketAddress;
    String id;
}
