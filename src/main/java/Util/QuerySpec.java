package Util;

/**
 * Created by bhushan on 6/9/17.
 */
public class QuerySpec {
    private String key;
    private Object value;
    private Operator operator;
    private Object lhsValue;
    private Object rhsValue;
    private boolean lhsInclusive;
    private boolean rhsInclusive;

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public Operator getOperator() {
        return operator;
    }

    public Object getLhsValue() {
        return lhsValue;
    }

    public Object getRhsValue() {
        return rhsValue;
    }

    public boolean isLhsInclusive() {
        return lhsInclusive;
    }

    public boolean isRhsInclusive() {
        return rhsInclusive;
    }

    public enum Operator {
        EQUAL, GT, LT, GTE, LTE, RANGE
    }

    public QuerySpec(String key, Object value, Operator op) {
        this.key = key;
        this.value = value;
        this.operator = op;
        this.lhsValue = null;
        this.rhsValue = null;
    }

    public QuerySpec(String key, Object lhsValue, Object rhsValue, boolean lhsInclusive, boolean rhsInclusive) {
        this.key = key;
        this.lhsValue = lhsValue;
        this.rhsValue = rhsValue;
        this.lhsInclusive = lhsInclusive;
        this.rhsInclusive = rhsInclusive;
        this.value = null;
    }
}
