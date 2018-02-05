package tsukka.p2p.dht.chord.json;

import com.fasterxml.jackson.databind.JsonNode;
import tsukka.p2p.dht.DHTAddress;
import tsukka.p2p.dht.NetworkBase;
import tsukka.p2p.dht.chord.RootingTable;
import tsukka.utility.function.Functions;


import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.EnumMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

public class ChordJSONProtocol<A extends DHTAddress>{
    private final ExecutorService exec = Executors.newSingleThreadExecutor();
    private final NetworkBase network;
    private final RootingTable<A> rootingTable;
    private final byte[] bytes;
    private final ByteBuffer buf;
    private final EnumMap<ChordEventType,BiConsumer<InetSocketAddress,JsonNode>> events;
    public static final String TYPE_STRING="type";
    protected ChordJSONProtocol(DatagramChannel channel, int size, int idLength) {
        bytes = new byte[size];
        buf = ByteBuffer.wrap(bytes);
        rootingTable = new RootingTable((int) Math.ceil(Math.log(idLength)), idLength);
        events = new EnumMap<ChordEventType, BiConsumer<InetSocketAddress, JsonNode>>(ChordEventType.class);
        network = new NetworkBase(channel, sk -> {
            try {
                InetSocketAddress address = (InetSocketAddress) ((DatagramChannel) sk.channel()).receive(buf);
                JsonNode node=JSONObjectMapperHolder.getObjectMapper().readTree(new String(bytes,0,buf.remaining()));
                String type=node.get(TYPE_STRING).asText();
                events.getOrDefault(ChordEventType.get(type), Functions.dump()).accept(address,node);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            buf.clear();
        });
    }

    public static ChordJSONProtocol open(ProtocolFamily family, int idLength) throws IOException {
        DatagramChannel channel = DatagramChannel.open(family);
        int size = channel.getOption(StandardSocketOptions.SO_RCVBUF);
        channel.configureBlocking(false);
        return new ChordJSONProtocol(channel, size, idLength);
    }

    public void workNow() {
        network.workNow();
    }

    public RootingTable<A> getRootingTable() {
        return rootingTable;
    }

    public EnumMap<ChordEventType, BiConsumer<InetSocketAddress, JsonNode>> getEvents() {
        return events;
    }

}

