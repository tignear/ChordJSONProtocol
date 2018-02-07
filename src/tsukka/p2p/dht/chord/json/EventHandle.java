package tsukka.p2p.dht.chord.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import tsukka.p2p.dht.DHTAddress;
import tsukka.p2p.dht.DHTAddressFactory;
import tsukka.p2p.dht.NetworkBase;

import tsukka.p2p.dht.chord.RootingTable;
import tsukka.p2p.dht.chord.json.model.SaveData;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import static tsukka.p2p.dht.chord.json.ChordEventType.*;

@Slf4j
public class EventHandle <A extends DHTAddress<A>>{
    private final DHTAddressFactory<A> factory;
    private final NetworkBase networkBase;
    private final RootingTable<A> rootingTable;
    private static final String TYPE_STRING="type";
    private static final String ADDRESS_STRING="address";
    public EventHandle(DHTAddressFactory<A> factory, NetworkBase networkBase, RootingTable<A> rootingTable){
        this.factory=factory;
        this.networkBase=networkBase;
        this.rootingTable=rootingTable;
    }

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
        networkBase.send(new ReturnAddressBuilder<>(rootingTable::predecessor)::build,socketAddress);
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
            SaveData sd=JSONObjectMapperHolder.getObjectMapper().treeToValue(node,SaveData.class);
            if(isMyArea(rootingTable.me().get(),rootingTable.predecessor().get(),factory.create(sd.getKey()))){
                //TODO ファイルの保存
            }else{
                log.warn("{}:bad request(invalidQuery->key)",socketAddress);
                return;
            }
        } catch (JsonProcessingException e) {
            log.warn("{}:bad request(parseError)",socketAddress);
            return;
        }
    }
    public void saveDataOrGetNearClientAddress(InetSocketAddress socketAddress,JsonNode node){
        try {
            SaveData saveData=JSONObjectMapperHolder.getObjectMapper().treeToValue(node,SaveData.class);
            A a=factory.create(saveData.getKey());
            if(isMyArea(rootingTable.me().get(),rootingTable.predecessor().get(),a)){
                //TODO ファイルの保存
            }else{
                networkBase.send(new ReturnAddressBuilder<>(()->Optional.of(a.nearSortSorted(rootingTable.fingers())[0]))::build,socketAddress);
            }
        } catch (JsonProcessingException e) {
            log.warn("{}:bad request(parseError)",socketAddress);
            return;
        }
    }

    public static<A extends DHTAddress<A>> boolean isMyArea(A i,A predecessor,A target){
        A[] array=as(predecessor,target);
        i.nearSortNotSorted(array);
        return array[0]==predecessor;
    }

    @SafeVarargs
    private static<A> A[] as(A... a){
        return a;
    }

    private final static class ReturnAddressBuilder<A extends DHTAddress<A>>{
        private final ObjectNode node;
        private final Supplier<Optional<A>> address;
        private ReturnAddressBuilder(Supplier<Optional<A>> address) {
            this.address=address;
            node = JSONObjectMapperHolder.getJsonNodeFactory().objectNode();
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
}
