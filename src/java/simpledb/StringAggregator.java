package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private String afieldName;
    private String gbfieldName;
    private final Map<Field, Integer> fieldCount;

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
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("aggregate operator != COUNT");
        }

        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.fieldCount = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        this.afieldName = tup.getTupleDesc().getFieldName(this.afield);

        if (this.gbfield != NO_GROUPING) {
            this.gbfieldName = tup.getTupleDesc().getFieldName(this.gbfield);

            // increment count by 1
            this.fieldCount.put(tup.getField(this.gbfield), this.fieldCount.getOrDefault(tup.getField(this.gbfield), 0) + 1);

        } else {
            // use null key for NO_GROUPING case
            this.fieldCount.put(null, this.fieldCount.getOrDefault(tup.getField(this.gbfield), 0) + 1);
        }
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
        ArrayList<Tuple> tuplesArr = new ArrayList<>();
        if (gbfield != NO_GROUPING) {
            // Form TupleDesc
            // name of aggregate column format = aggregate op, aggregate field
            TupleDesc td = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE},
                    new String[]{this.gbfieldName, this.what.toString() + ", " + this.afieldName});

            // create tuples
            for (Field f : fieldCount.keySet()) {

                Tuple tup = new Tuple(td);
                tup.setField(0, f);
                tup.setField(1, new IntField(this.fieldCount.get(f)));
                tuplesArr.add(tup);
            }

            // Create TupleIterator
            return new TupleIterator(td, tuplesArr);

        } else {
            // NO_GROUPING case
            // Form TupleDesc
            Type[] typeAr = new Type[]{Type.INT_TYPE};
            String[] fieldAr = new String[]{this.what.toString() + " " + this.afieldName};
            TupleDesc td = new TupleDesc(typeAr, fieldAr);

            Tuple tup = new Tuple(td);
            tup.setField(0, new IntField(this.fieldCount.get(null)));
            tuplesArr.add(tup);

            // Create TupleIterator
            return new TupleIterator(td, tuplesArr);
        }
    }

}
