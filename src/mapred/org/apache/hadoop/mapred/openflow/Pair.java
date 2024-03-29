package org.apache.hadoop.mapred.openflow;

public class Pair<A, B> implements Comparable<Pair<A, B>> {
    public A first;
    public B second;
    public Pair(A a, B b) {
        first = a;
        second = b;
    }
    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = compare(first, o.first);
        return cmp == 0 ? compare(second, o.second) : cmp;
    }
    @SuppressWarnings("unchecked")
    protected static int compare(Object o1, Object o2) {
        if(o1 == null) {
            if(o2 == null)
                return 0;
            return -1;
        }
        else {
            if(o2 == null)
                return 1;
            return ((Comparable) o1).compareTo(o2);
        }
    }
    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }
    protected static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }
    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof Pair))
            return false;
        if(this == obj)
            return true;
        return equal(first, ((Pair) obj).first) && equal(second, ((Pair) obj).second);
    }
    protected boolean equal(Object o1, Object o2) {
        return o1 == null ? o2 == null : (o1 == o2 || o1.equals(o2));
    }
    @Override
    public String toString() {
        return "(" + first + ", " + second + ")";
    }
}
