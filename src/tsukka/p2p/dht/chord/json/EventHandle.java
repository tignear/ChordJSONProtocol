package tsukka.p2p.dht.chord.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import tsukka.p2p.dht.DHTAddress;
import tsukka.p2p.dht.DHTAddressFactory;
import tsukka.p2p.dht.DataIO;
import tsukka.p2p.dht.NetworkBase;

import tsukka.p2p.dht.chord.RootingTable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static tsukka.p2p.dht.chord.json.ChordEventType.*;
import static tsukka.p2p.dht.chord.json.KeyStrings.*;

@FieldDefaults(makeFinal = true,level = AccessLevel.PRIVATE)
public class EventHandle <A extends DHTAddress<A>>{
    PassiveHandle<A> passiveHandle;
    ActiveHandle<A> activeHandle;
    public EventHandle(DHTAddressFactory<A> factory,NetworkBase networkBase,RootingTable<A> rootingTable,DataIO<A> io){
        passiveHandle=new PassiveHandle<>(factory,networkBase,rootingTable,io);
        activeHandle=new ActiveHandle<>(factory,networkBase,rootingTable,io);
    }

    /*
    passiveHandle delegate
     */
    public void heartbeat(ChordConnection connection, JsonNode node) {
        passiveHandle.heartbeat(connection, node);
    }

    public void getPredecessor(ChordConnection connection, JsonNode node) {
        passiveHandle.getPredecessor(connection, node);
    }

    public void setPredecessor(ChordConnection connection, JsonNode node) {
        passiveHandle.setPredecessor(connection, node);
    }

    public void saveData(ChordConnection connection, JsonNode node) {
        passiveHandle.saveData(connection, node);
    }

    public void saveDataOrGetNearClientAddress(ChordConnection connection, JsonNode node) {
        passiveHandle.saveDataOrGetNearClientAddress(connection, node);
    }

    public void getData(ChordConnection connection, JsonNode node) {
        passiveHandle.getData(connection, node);
    }

    public void getNearClientAddress(ChordConnection connection, JsonNode node) {
        passiveHandle.getNearClientAddress(connection, node);
    }

    public void getDataOrNearClientAddress(ChordConnection connection, JsonNode node) {
        passiveHandle.getDataOrNearClientAddress(connection, node);
    }
    /*
    passiveHandle delegate end
    */

    /*
    activeHandle delegate
     */
    /**
     * RETURN_ADDRESSとかが帰ってきたときのイベントハンドル設定用
     * @param connection key
     * @param consumer value
     */
    public void addHandle(ChordConnection connection,Consumer<ChordReturnEvent> consumer){
        activeHandle.addHandle(connection, consumer);
    }

    /**
     * RETURN_ADDRESSとかが帰ってきたときのイベントハンドル削除用
     * @param connection key
     */
    public void removeHandle(ChordConnection connection){
        activeHandle.removeHandle(connection);
    }
    public void returnData(ChordConnection connection, JsonNode node) {
        activeHandle.returnData(connection, node);
    }

    public void returnAddress(ChordConnection connection, JsonNode node) {
        activeHandle.returnAddress(connection, node);
    }

    public void successSaveData(ChordConnection connection, JsonNode node) {
        activeHandle.successSaveData(connection, node);
    }

    public void returnHearbeat(ChordConnection connection, JsonNode node) {
        activeHandle.returnHearbeat(connection, node);
    }
    /*
    activeHandle delegate end
     */

}

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true,level = AccessLevel.PRIVATE)
class ActiveHandle<A extends DHTAddress<A>> {
    Map<ChordConnection, Consumer<ChordReturnEvent>> connections=new HashMap<>();
    DHTAddressFactory<A> factory;
    NetworkBase networkBase;
    RootingTable<A> rootingTable;
    DataIO<A> io;

    public void addHandle(ChordConnection connection, Consumer<ChordReturnEvent> consumer) {
        connections.put(connection,consumer);
    }
    public void removeHandle(ChordConnection connection){
        connections.remove(connection);
    }
    public void returnData(ChordConnection connection,JsonNode node){
        delegate(connection,RETURN_DATA,node);
    }
    public void returnAddress(ChordConnection connection,JsonNode node){
        delegate(connection,RETURN_ADDRESS,node);
    }
    public void successSaveData(ChordConnection connection,JsonNode node){
        delegate(connection,SUCCESS_SAVE_DATA,node);
    }
    public void returnHearbeat(ChordConnection connection,JsonNode node){
        delegate(connection,RETURN_HEARTBEAT,node);
    }
    private void delegate(ChordConnection connection,ChordEventType type,JsonNode node){
        connections.getOrDefault(connection,e->{}).accept(new ChordReturnEvent(connection,type,node));
    }
}

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true,level = AccessLevel.PRIVATE)
class PassiveHandle<A extends DHTAddress<A>> {
    DHTAddressFactory<A> factory;
    NetworkBase networkBase;
    RootingTable<A> rootingTable;
    DataIO<A> io;

    public void heartbeat(ChordConnection connection, JsonNode node) {
        networkBase.send(new ReturnHeartbeatBuilder(connection.getId()), connection.getSocketAddress());
    }

    public void getPredecessor(ChordConnection connection, JsonNode node) {
        returnAddress(rootingTable::predecessor, connection);
    }


    public void setPredecessor(ChordConnection connection, JsonNode node) {
        A check = factory.create(DHTAddressFactory.toInet6Address(connection.getSocketAddress().getAddress()), (short) connection.getSocketAddress().getPort());
        if (Objects.isNull(check)) {
            log.warn("{}:bad request(invalidQuery->address)", connection);
            return;
        }
        rootingTable.predecessor().ifPresentOrElse(predecessor -> {
            A me = rootingTable.me().get();
            A[] array = as(check, predecessor);
            me.nearSortNotSorted(array);
            if (array[0] == check) {
                rootingTable.predecessor(check);
            }
        }, () -> rootingTable.predecessor(check));
    }

    public void saveData(ChordConnection connection, JsonNode node) {
        try {
            A a = factory.create(node.get(ADDRESS_STRING).binaryValue());
            if (isMyArea(rootingTable.me().get(), rootingTable.predecessor().get(), a)) {
                io.write(a, node.get(DATA_STRING).binaryValue());
                //TODO
            } else {
                log.warn("{}:bad request(invalidQuery->address)", connection);
                return;
            }
        } catch (IOException e) {
            log.warn("{}:bad request(invalidQuery->address)", connection);
        }
    }

    public void saveDataOrGetNearClientAddress(ChordConnection connection, JsonNode node) {
        try {
            A a = factory.createRaw(node.get(ADDRESS_STRING).binaryValue());
            if (isMyArea(rootingTable.me().get(), rootingTable.predecessor().get(), a)) {
                io.write(a, node.get(DATA_STRING).binaryValue());
            } else {
                returnAddressNonNull(() -> a.nearSortSorted(rootingTable.fingers())[0], connection);

            }
        } catch (IOException | IllegalArgumentException e) {
            log.warn("{}:bad request(invalidQuery->address)", connection);
            return;
        }
    }

    public void getData(ChordConnection connection, JsonNode node) {
        try {
            A a = factory.createRaw(node.get(ADDRESS_STRING).binaryValue());
            returnData(io.read(a), connection);
        } catch (IOException | IllegalArgumentException e) {
            log.warn("{}:bad request(invalidQuery->address)", connection);
            return;
        }
    }

    public void getNearClientAddress(ChordConnection connection, JsonNode node) {
        try {
            A a = factory.createRaw(node.get(ADDRESS_STRING).binaryValue());
            returnAddressNonNull(() -> a.nearSortSorted(rootingTable.fingers())[0], connection);
        } catch (IOException | IllegalArgumentException e) {
            log.warn("{}:bad request(invalidQuery->address)", connection);
            return;
        }
    }

    public void getDataOrNearClientAddress(ChordConnection connection, JsonNode node) {
        try {
            A a = factory.createRaw(node.get(ADDRESS_STRING).binaryValue());
            if (io.exits(a)) {
                returnData(io.read(a), connection);
            } else {
                //ファイルを持っていない
                returnAddressNonNull(() -> a.nearSortSorted(rootingTable.fingers())[0], connection);
            }
        } catch (IOException | IllegalArgumentException e) {
            log.warn("{}:bad request(invalidQuery->address)", connection);
            return;
        }
    }

    private static <A extends DHTAddress<A>> boolean isMyArea(A i, A predecessor, A target) {
        return i.nearSortNotSorted(as(predecessor, target))[0] == predecessor;
    }

    @SafeVarargs
    private static <A> A[] as(A... a) {
        return a;
    }

    private void returnAddress(Supplier<Optional<A>> supplier, ChordConnection address) {
        networkBase.send(new ReturnAddressBuilder<>(supplier, address.getId()), address.getSocketAddress());
    }

    private void returnAddressNonNull(Supplier<A> supplier, ChordConnection address) {
        returnAddress(() -> Optional.of(supplier.get()), address);
    }

    private void returnData(Supplier<Optional<byte[]>> supplier, ChordConnection address) {
        networkBase.send(new ReturnDataBuilder(supplier, address.getId()), address.getSocketAddress());
    }

    private void returnDataNonNull(Supplier<byte[]> supplier, ChordConnection address) {
        returnData(() -> Optional.of(supplier.get()), address);
    }

    private final static class ReturnAddressBuilder<A extends DHTAddress<A>> implements Supplier<Optional<ByteBuffer>> {
        private final ObjectNode node;
        private final Supplier<Optional<A>> address;

        private ReturnAddressBuilder(Supplier<Optional<A>> address, String id) {
            this.address = address;
            node = JSONObjectMapperHolder.getObjectMapper().createObjectNode();
            node.put(TYPE_STRING, RETURN_ADDRESS.toString());
            node.put(ID_STRING, id);
        }

        public Optional<ByteBuffer> get() {
            return address.get().map(address -> {
                node.put(ADDRESS_STRING, address.getAddress());
                try {
                    return ByteBuffer.wrap(JSONObjectMapperHolder.getObjectMapper().writeValueAsBytes(node));
                } catch (JsonProcessingException e) {
                    throw new IllegalStateException(e);
                }
            });
        }
    }

    private final static class ReturnDataBuilder implements Supplier<Optional<ByteBuffer>> {
        private final ObjectNode node;
        private final Supplier<Optional<byte[]>> supplier;

        private ReturnDataBuilder(Supplier<Optional<byte[]>> supplier, String id) {
            this.supplier = supplier;
            node = JSONObjectMapperHolder.getObjectMapper().createObjectNode();
            node.put(TYPE_STRING, RETURN_DATA.toString());
            node.put(ID_STRING, id);
        }

        public Optional<ByteBuffer> get() {
            return supplier.get().map(value -> {
                node.put(ADDRESS_STRING, value);
                try {
                    return ByteBuffer.wrap(JSONObjectMapperHolder.getObjectMapper().writeValueAsBytes(node));
                } catch (JsonProcessingException e) {
                    throw new IllegalStateException(e);
                }
            });
        }
    }

    private final static class ReturnHeartbeatBuilder implements Supplier<Optional<ByteBuffer>> {
        private final ByteBuffer buf;

        private ReturnHeartbeatBuilder(String id) {
            ObjectNode node = JSONObjectMapperHolder.getJsonNodeFactory().objectNode();
            node.put(TYPE_STRING, RETURN_HEARTBEAT.toString());
            node.put(ID_STRING, id);
            try {
                buf = ByteBuffer.wrap(JSONObjectMapperHolder.getObjectMapper().writeValueAsBytes(node));
            } catch (JsonProcessingException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public Optional<ByteBuffer> get() {
            return Optional.of(buf);
        }
    }
}

