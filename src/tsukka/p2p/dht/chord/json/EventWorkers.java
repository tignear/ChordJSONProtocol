package tsukka.p2p.dht.chord.json;

import com.fasterxml.jackson.databind.JsonNode;

import java.net.InetSocketAddress;
import java.util.function.BiConsumer;

public final class EventWorkers {
    private static interface EventWorker extends BiConsumer<InetSocketAddress,JsonNode>{

    }
    public static EventWorker getPredecessor(){
        return (socketAddress, jsonNode) -> {};
    }
}
