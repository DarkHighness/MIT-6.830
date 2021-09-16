package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
    private static final long serialVersionUID = 1L;

    private static final Field NO_GROUPING_FIELD = new IntField(-1);

    private final int groupField;
    private final Type groupFieldType;
    private final int aggragateField;
    private final Op what;
    private final TupleDesc tupleDesc;
    private final Map<Field, InternalResult> results;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.groupField = gbfield;
        this.groupFieldType = gbfieldtype;
        this.aggragateField = afield;
        this.what = what;
        this.results = new LinkedHashMap<>();

        if (gbfield == NO_GROUPING)
            this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        else
            this.tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field keyField;

        if (groupField != NO_GROUPING)
            keyField = tup.getField(groupField);
        else
            keyField = NO_GROUPING_FIELD;

        int newVal = ((IntField) tup.getField(aggragateField)).getValue();

        results.computeIfAbsent(keyField, k -> new InternalResult(what))
                .merge(newVal);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        List<Tuple> tupleList = new ArrayList<>(results.size());

        for(Map.Entry<Field, InternalResult> entry : results.entrySet()) {
            Tuple tuple = new Tuple(tupleDesc);

            if (groupField == NO_GROUPING) {
                tuple.setField(0, new IntField(entry.getValue().getResult()));
            } else {
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(entry.getValue().getResult()));
            }

            tupleList.add(tuple);
        }

        return new TupleIterator(tupleDesc, tupleList);
    }

    private static class InternalResult {
        private int count;
        private float result;
        private final Op op;

        private InternalResult(Op op) {
            this.op = op;
        }

        public void merge(int newVal) {
            if (count == 0) {
                result = newVal;
                count = 1;
                return;
            }

            switch (op) {
                case MIN:
                    result = Math.min(result, newVal);
                    break;
                case MAX:
                    result = Math.max(result, newVal);
                    break;
                case SUM:
                    result += newVal;
                    break;
                case AVG:
                    result = ((result * count) + newVal) / (count + 1);
                    break;
            }

            count++;
        }

        public int getResult() {
            if (op == Op.COUNT)
                return count;

            return (int) result;
        }
    }
}
