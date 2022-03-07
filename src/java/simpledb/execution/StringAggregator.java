package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private static String NO_GROUP="_DEFAULT_KEY_";
    private final int gbFieldIndex;
    private final Type gbFieldType;
    private final int aggregateFieldIndex;
    private final Op op;
    private AggParse tupleParse;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbFieldIndex =gbfield;
        this.gbFieldType =gbfieldtype;
        this.aggregateFieldIndex =afield;
        this.op=what;
        switch (what){
            case COUNT:
                tupleParse =new CountAggParse();
                break;
            //case SC_AVG:
            //    break;
            //case SUM_COUNT:
            //    break;
            default:
                break;
        }
    }

    private interface AggParse {
        void parse(String key, Field field);
        ConcurrentHashMap<String, Integer> getResult();
    }

    private class CountAggParse implements AggParse {
        ConcurrentHashMap<String, Integer> groupResult = new ConcurrentHashMap<>();

        @Override
        public void parse(String key, Field field) {
            groupResult.put(key,groupResult.getOrDefault(key,0)+1);
        }

        @Override
        public ConcurrentHashMap<String, Integer> getResult() {
            return groupResult;
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        String key=NO_GROUP;
        if(gbFieldIndex!=-1){
            key=tup.getField(gbFieldIndex).toString();
        }
        tupleParse.parse(key,tup.getField(aggregateFieldIndex));
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        //throw new UnsupportedOperationException("please implement me for lab2");
        ConcurrentHashMap<String, Integer> tupleParseResult = tupleParse.getResult();
        Type[] types;
        String[] names;
        TupleDesc td;
        List<Tuple> tupleList=new LinkedList<>();
        if(gbFieldIndex==-1){
            types=new Type[]{Type.INT_TYPE};
            names=new String[]{"aggResult"};
            td=new TupleDesc(types,names);
            Tuple tuple = new Tuple(td);
            tuple.setField(0,new IntField(tupleParseResult.get(NO_GROUP)));
            tupleList.add(tuple);
        }else{
            types=new Type[]{gbFieldType,Type.INT_TYPE};
            names=new String[]{"groupVal","aggResult"};
            td=new TupleDesc(types,names);
            for (String key : tupleParseResult.keySet()) {
                Tuple tuple = new Tuple(td);
                if(gbFieldType==Type.INT_TYPE){
                    tuple.setField(0,new IntField(Integer.parseInt(key)));
                }else {
                    tuple.setField(0,new StringField(key,key.length()));
                }
                tuple.setField(1,new IntField(tupleParseResult.get(key)));
                tupleList.add(tuple);
            }
        }
        return new TupleIterator(td,tupleList);
    }
}
