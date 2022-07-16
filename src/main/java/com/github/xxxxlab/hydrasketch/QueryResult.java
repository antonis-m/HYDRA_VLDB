/* (C)2021 */
package com.github.xxxxlab.hydrasketch;

public class QueryResult {
    public float entropy;
    public float l2;
    public float l1;
    public float card;
    public String key;

    public QueryResult(float _entropy, float _l2, float _l1, float _card) {
        entropy = _entropy;
        l2 = _l2;
        l1 = _l1;
        card = _card;
    }
}
