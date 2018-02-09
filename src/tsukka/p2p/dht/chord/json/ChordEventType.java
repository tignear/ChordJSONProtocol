package tsukka.p2p.dht.chord.json;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Value;


public enum ChordEventType {
    HEARTBEAT(true),
    /**
     *
     * これはSuccessorのstabilize時に用いられます
     */
    GET_PREDECESSOR(true),
    /**
     * PREDECESSORが自分じゃないかチャックするように要求
     * SuccessorのStabilize時に使用
     */
    SET_PREDECESSOR(true),
    /**
     * データの保存リクエスト
     */
    SAVE_DATA(true),
    /**
     * データを保存するか一番要求に近い行きすぎないクライアントのアドレスを返します
     */
    SAVE_DATA_OR_GET_NEAR_CLIENT_ADDRESS(true),
    /**
     * データの取得リクエスト
     */
    GET_DATA(true),
    /**
     * 一番要求に近い行きすぎないアドレスを返します
     */
    GET_NEAR_CLIENT_ADDRESS(true),
    /**
     * データか一番要求に近い行きすぎ無いアドレスを返します
     */
    GET_DATA_OR_NEAR_CLIENT_ADDRESS(true),
    /**
     * データを返します
     */
    RETURN_DATA(false),
    /**
     * アドレスを返します
     */
    RETURN_ADDRESS(false),
    /**
     * ハートビートリクエストへの応答です
     */
    RETURN_HEARTBEAT(false),
    SUCCESS_SAVE_DATA(false),

    ;
    public static ChordEventType get(String s){
        return valueOf(s);
    }
    @Getter private final boolean active;
    ChordEventType(boolean active){
        this.active=active;
    }

}
