package org.apache.hadoop.mapred.openflow;

public class UndirectedPair<A, B> extends Pair<A, B> {
	public UndirectedPair(A a, B b) {
		super(a, b);
	}
	@Override
	public int hashCode() {
		return hashcode(first) + hashcode(second);
	}
    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof Pair))
            return false;
        if(this == obj)
            return true;
        boolean orderSame = equal(first, ((Pair) obj).first) && equal(second, ((Pair) obj).second);
        boolean orderDiff = equal(first, ((Pair) obj).second) && equal(second, ((Pair) obj).first);
        return orderSame || orderDiff;
    }
}
