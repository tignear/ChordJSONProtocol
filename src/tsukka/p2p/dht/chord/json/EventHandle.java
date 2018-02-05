package tsukka.p2p.dht.chord.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import tsukka.p2p.dht.DHTAddress;
import tsukka.p2p.dht.DHTAddressFactory;
import tsukka.p2p.dht.NetworkBase;

import tsukka.p2p.dht.chord.RootingTable;
import tsukka.p2p.dht.chord.json.model.SaveData;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Objects;

import static tsukka.p2p.dht.chord.json.ChordEventType.*;

public class EventHandle <A extends DHTAddress>{
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
    private ByteBuffer buildReturnHeartbeat(){
        ObjectNode node=JSONObjectMapperHolder.getJsonNodeFactory().objectNode().put(TYPE_STRING,RETURN_HEARTBEAT.toString());
        try {
            return ByteBuffer.wrap(JSONObjectMapperHolder.getObjectMapper().writeValueAsBytes(node));
        } catch (JsonProcessingException e) {
            return ByteBuffer.allocate(0);
        }
    }
    public void getPredecessor(InetSocketAddress socketAddress,JsonNode node){
        networkBase.send(this::buildReturnPredecessor,socketAddress);
    }
    private ByteBuffer buildReturnPredecessor(){
        ObjectNode node=JSONObjectMapperHolder.getJsonNodeFactory().objectNode();
        node.put(TYPE_STRING,RETURN_ADDRESS.toString());
        rootingTable.predecessor().ifPresent(address->node.put(ADDRESS_STRING, address.getAddress()));

        try {
            return ByteBuffer.wrap(JSONObjectMapperHolder.getObjectMapper().writeValueAsBytes(node));
        } catch (JsonProcessingException e) {
            return ByteBuffer.allocate(0);
        }
    }
    public void setPredecessor(InetSocketAddress socketAddress,JsonNode node){
        A check=factory.create(DHTAddressFactory.toInet6Address(socketAddress.getAddress()),(short)socketAddress.getPort());
        if(Objects.isNull(check)){
            return;
        }
        rootingTable.predecessor().ifPresentOrElse(predecessor->{
            A me = rootingTable.me().get();
            A[] array = as(check, predecessor);
            me.nearSort(array);
            if (array[0] == check) {
                rootingTable.predecessor(check);
            }
        },()->rootingTable.predecessor(check));
    }
    public void saveData(InetSocketAddress socketAddress,JsonNode node){
        //TODO ファイルの保存と移転
        try {
            JSONObjectMapperHolder.getObjectMapper().treeToValue(node,SaveData.class);
        } catch (JsonProcessingException e) {
            return;
        }
    }
    public void saveDataOrGetNearClientAddress(InetSocketAddress socketAddress,JsonNode node){

    }

    public static<A extends DHTAddress> boolean isMyArea(A i,A predecessor,A target){
        A[] array=as(predecessor,target);
        i.nearSort(array);
        return array[0]==predecessor;
    }
    private static<A> A[] as(A... a){
        return a;
    }
}
