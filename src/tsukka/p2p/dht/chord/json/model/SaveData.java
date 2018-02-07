package tsukka.p2p.dht.chord.json.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import tsukka.p2p.dht.DHTAddress;
import tsukka.p2p.dht.DHTAddressFactory;
import tsukka.p2p.dht.chord.json.ChordEventType;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SaveData{
    public static final ChordEventType TYPE= ChordEventType.SAVE_DATA;
    @Getter private byte[] key;
    @Getter private byte[] value;

    public ChordEventType getType() {
        return TYPE;
    }

    @JsonCreator
    public SaveData(ChordEventType type,byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }
}
