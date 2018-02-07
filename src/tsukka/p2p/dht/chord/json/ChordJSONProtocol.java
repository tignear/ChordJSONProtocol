package tsukka.p2p.dht.chord.json;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Getter;
import tsukka.p2p.dht.DHTAddress;
import tsukka.p2p.dht.DHTAddressFactory;
import tsukka.p2p.dht.NetworkBase;
import tsukka.p2p.dht.chord.RootingTable;
import tsukka.utility.function.Functions;


import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.EnumMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

public class ChordJSONProtocol<A extends DHTAddress<A>>{
    private final ExecutorService exec = Executors.newSingleThreadExecutor();
    private final NetworkBase network;
    @Getter private final RootingTable<A> rootingTable;
    private final byte[] bytes;
    private final ByteBuffer buf;
    @Getter private final EnumMap<ChordEventType,BiConsumer<InetSocketAddress,JsonNode>> events;
    public static final String TYPE_STRING="type";

    protected ChordJSONProtocol(DatagramChannel channel, int size, int idLength,A myAddress) {
        bytes = new byte[size];
        buf = ByteBuffer.wrap(bytes);
        rootingTable = new RootingTable<>((int) Math.ceil(Math.log(idLength)), idLength);
        rootingTable.me(myAddress);
        events = new EnumMap<>(ChordEventType.class);
        network = new NetworkBase(channel, sk -> {
            try {
                InetSocketAddress address = (InetSocketAddress) ((DatagramChannel) sk.channel()).receive(buf);
                JsonNode node=JSONObjectMapperHolder.getObjectMapper().readTree(new String(bytes,0,buf.remaining()));
                String type=node.get(TYPE_STRING).asText();
                events.getOrDefault(ChordEventType.get(type),Functions.dump()).accept(address,node);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            buf.clear();
        });
    }

    public static<A extends DHTAddress<A>> ChordJSONProtocol<A> open(int idLength, DHTAddressFactory<A> factory) throws IOException {
        DatagramChannel channel = DatagramChannel.open(StandardProtocolFamily.INET6);
        int size = channel.getOption(StandardSocketOptions.SO_RCVBUF);
        channel.configureBlocking(false);
        InetSocketAddress isa=(InetSocketAddress) channel.getLocalAddress();
        return new ChordJSONProtocol<A>(channel, size, idLength,factory.create(DHTAddressFactory.toInet6Address(isa.getAddress()),(short)isa.getPort()));
    }

    public void workNow() {
        network.workNow();
    }
}

