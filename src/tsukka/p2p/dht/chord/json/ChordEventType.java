package tsukka.p2p.dht.chord.json;

public enum ChordEventType {
    /**
     *
     * これはSuccessorのstabilize時に用いられます
     */
    GET_PREDECESSOR,
    /**
     * PREDECESSORが自分じゃないかチャックするように要求される
     * SuccessorのStabilize時に使用
     */
    CHECK_PREDECESSOR,
    /**
     * データの保存リクエスト
     */
    SAVE_DATA,
    /**
     * データを保存するか一番要求に近い行きすぎないクライアントのアドレスを返します
     */
    SAVE_DATA_OR_GET_NEAR_CLIENT_ADDRESS,
    /**
     * データの取得リクエスト
     */
    GET_DATA,
    /**
     * 一番要求に近い行きすぎないアドレスを返します
     */
    GET_NEAR_CLIENT_ADDRESS,
    /**
     * データか一番要求に近い行きすぎ無いアドレスを返します
     */
    GET_DATA_OR_NEAR_CLIENT_ADDRESS,
    /**
     * データを返します
     */
    RETURN_DATA,
    /**
     * アドレスを返します
     */
    RETURN_ADDRESS;
    public static ChordEventType get(String s){
        return valueOf(s);
    }
}
