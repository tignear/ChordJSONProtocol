package tsukka.p2p.dht.chord.json;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Value;

@Value
public class ChordReturnEvent {
    ChordConnection connection;
    ChordEventType type;
    JsonNode node;
}
