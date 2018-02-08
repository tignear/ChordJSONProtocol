package tsukka.p2p.dht.chord.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tsukka.p2p.dht.DHTAddress;
import tsukka.p2p.dht.DHTAddressFactory;
import tsukka.p2p.dht.DataIO;
import tsukka.p2p.dht.NetworkBase;

import tsukka.p2p.dht.chord.RootingTable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import static tsukka.p2p.dht.chord.json.ChordEventType.*;

@Slf4j
@RequiredArgsConstructor
public class EventHandle <A extends DHTAddress<A>>{
    private final DHTAddressFactory<A> factory;
    private final NetworkBase networkBase;
    private final RootingTable<A> rootingTable;
    private final DataIO<A> io;
    private static final String TYPE_STRING="type";
    private static final String ADDRESS_STRING="address";
    private static final String DATA_STRING="data";

    public void heartbeat(InetSocketAddress socketAddress,JsonNode node){
        networkBase.send(this::buildReturnHeartbeat,socketAddress);
    }
    private Optional<ByteBuffer> buildReturnHeartbeat(){
        ObjectNode node=JSONObjectMapperHolder.getJsonNodeFactory().objectNode().put(TYPE_STRING,RETURN_HEARTBEAT.toString());
        try {
            return Optional.of(ByteBuffer.wrap(JSONObjectMapperHolder.getObjectMapper().writeValueAsBytes(node)));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
    public void getPredecessor(InetSocketAddress socketAddress,JsonNode node){
        returnAddress(rootingTable::predecessor,socketAddress);
    }


    public void setPredecessor(InetSocketAddress socketAddress,JsonNode node){
        A check=factory.create(DHTAddressFactory.toInet6Address(socketAddress.getAddress()),(short)socketAddress.getPort());
        if(Objects.isNull(check)){
            log.warn("{}:bad request(invalidQuery->address)",socketAddress);
            return;
        }
        rootingTable.predecessor().ifPresentOrElse(predecessor->{
            A me = rootingTable.me().get();
            A[] array = as(check, predecessor);
            me.nearSortNotSorted(array);
            if (array[0] == check) {
                rootingTable.predecessor(check);
            }
        },()->rootingTable.predecessor(check));
    }
    public void saveData(InetSocketAddress socketAddress,JsonNode node){
        try {
            A a=factory.create(node.get(ADDRESS_STRING).binaryValue());
            if(isMyArea(rootingTable.me().get(),rootingTable.predecessor().get(),a)){

            }else{
                log.warn("{}:bad request(invalidQuery->address)",socketAddress);
                return;
            }
        } catch (IOException e) {
            log.warn("{}:bad request(invalidQuery->address)",socketAddress);
        }
    }
    public void saveDataOrGetNearClientAddress(InetSocketAddress socketAddress,JsonNode node){
        try {
            A a=factory.createRaw(node.get(ADDRESS_STRING).binaryValue());
            if(isMyArea(rootingTable.me().get(),rootingTable.predecessor().get(),a)){
                io.write(a,node.get(DATA_STRING).binaryValue());
            }else{
                returnAddressNonNull(()->a.nearSortSorted(rootingTable.fingers())[0],socketAddress);

            }
        } catch (IOException|IllegalArgumentException e) {
            log.warn("{}:bad request(invalidQuery->address)",socketAddress);
            return;
        }
    }
    public void getData(InetSocketAddress socketAddress,JsonNode node){
        try {
            A a=factory.createRaw(node.get(ADDRESS_STRING).binaryValue());
            returnData(io.read(a),socketAddress);
        } catch (IOException|IllegalArgumentException e) {
            log.warn("{}:bad request(invalidQuery->address)",socketAddress);
            return;
        }
    }
    public void getNearClientAddress(InetSocketAddress socketAddress,JsonNode node){
        try {
            A a=factory.createRaw(node.get(ADDRESS_STRING).binaryValue());
            returnAddressNonNull(()->a.nearSortSorted(rootingTable.fingers())[0],socketAddress);
        } catch (IOException | IllegalArgumentException e) {
            log.warn("{}:bad request(invalidQuery->address)",socketAddress);
            return;
        }
    }
    public void getDataOrNearClientAddress(InetSocketAddress socketAddress,JsonNode node){
        try {
            A a=factory.createRaw(node.get(ADDRESS_STRING).binaryValue());
            //TODO
            if(io.exits(a)){
                returnData(io.read(a),socketAddress);
            }else{
                //ファイルを持っていない
                returnAddressNonNull(()->a.nearSortSorted(rootingTable.fingers())[0],socketAddress);
            }
        } catch (IOException | IllegalArgumentException e) {
            log.warn("{}:bad request(invalidQuery->address)", socketAddress);
            return;
        }
    }
    private static<A extends DHTAddress<A>> boolean isMyArea(A i,A predecessor,A target){
        return i.nearSortNotSorted(as(predecessor,target))[0]==predecessor;
    }

    @SafeVarargs
    private static<A> A[] as(A... a){
        return a;
    }
    private void returnAddress(Supplier<Optional<A>> supplier, InetSocketAddress socketAddress){
        networkBase.send(new ReturnAddressBuilder<>(supplier)::build,socketAddress);
    }
    private void returnAddressNonNull(Supplier<A> supplier, InetSocketAddress socketAddress){
        returnAddress(()->Optional.of(supplier.get()),socketAddress);
    }
    private void returnData(Supplier<Optional<byte[]>> supplier,InetSocketAddress socketAddress){
        networkBase.send(new ReturnDataBuilder(supplier)::build,socketAddress);
    }
    private void returnDataNonNull(Supplier<byte[]> supplier,InetSocketAddress socketAddress){
        returnData(()->Optional.of(supplier.get()),socketAddress);
    }
    private final static class ReturnAddressBuilder<A extends DHTAddress<A>>{
        private final ObjectNode node;
        private final Supplier<Optional<A>> address;
        private ReturnAddressBuilder(Supplier<Optional<A>> address) {
            this.address=address;
            node = JSONObjectMapperHolder.getObjectMapper().createObjectNode();
            node.put(TYPE_STRING, RETURN_ADDRESS.toString());
        }
        private Optional<ByteBuffer> build(){
            return address.get().map(address->{
                node.put(ADDRESS_STRING, address.getAddress());
                try{
                    return ByteBuffer.wrap(JSONObjectMapperHolder.getObjectMapper().writeValueAsBytes(node));
                } catch (JsonProcessingException e) {
                    throw new IllegalStateException(e);
                }
            });
        }
    }
    private final static class ReturnDataBuilder{
        private final ObjectNode node;
        private final Supplier<Optional<byte[]>> supplier;
        private ReturnDataBuilder(Supplier<Optional<byte[]>> supplier){
            this.supplier=supplier;
            node=JSONObjectMapperHolder.getObjectMapper().createObjectNode();
            node.put(TYPE_STRING,RETURN_DATA.toString());
        }
        private Optional<ByteBuffer> build(){
            return supplier.get().map(value->{
                node.put(ADDRESS_STRING,value);
                try {
                    return ByteBuffer.wrap(JSONObjectMapperHolder.getObjectMapper().writeValueAsBytes(node));
                } catch (JsonProcessingException e) {
                    throw new IllegalStateException(e);
                }
            });
        }

    }
}
