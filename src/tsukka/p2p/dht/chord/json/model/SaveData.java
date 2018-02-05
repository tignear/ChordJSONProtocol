package tsukka.p2p.dht.chord.json.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import tsukka.p2p.dht.chord.json.ChordEventType;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SaveData {
    public final String type= ChordEventType.SAVE_DATA.toString();
    public byte[] key;
    public byte[] value;
}
