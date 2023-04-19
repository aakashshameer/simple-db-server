package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private String afieldName;
    private String gbfieldName;
    private final Map<Field, Integer> fieldCount;
    private final Map<Field, Integer> fieldAggregate;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        this.fieldCount = new HashMap<>();
        this.fieldAggregate = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        this.afieldName = tup.getTupleDesc().getFieldName(this.afield);
        int aggregate = ((IntField) tup.getField(this.afield)).getValue();

        if (this.gbfield != Aggregator.NO_GROUPING) {
            this.gbfieldName = tup.getTupleDesc().getFieldName(this.gbfield);
            mergeTupleIntoGroup(tup.getField(this.gbfield), aggregate);

        } else {
            // if no grouping, use null keys
            mergeTupleIntoGroup(null, aggregate);
        }
    }

    /**
     * Helper method for merging tuple into group
     */
    private void mergeTupleIntoGroup(Field gbfield, int aggregate) {
        // Store aggregate if not yet in map
        if (!this.fieldAggregate.containsKey(gbfield)) {
            this.fieldAggregate.put(gbfield, aggregate);
        } else {
            int currAggregate = this.fieldAggregate.get(gbfield);
            switch (this.what) {
                // perform different actions based on aggregate operator received
                case COUNT:
                    break;  // do nothing
                case SUM:
                case AVG:
                    // SUM same case as AVG; for AVG, take the AVG later
                    this.fieldAggregate.put(gbfield, currAggregate + aggregate);
                    break;
                case MIN:
                    this.fieldAggregate.put(gbfield, Math.min(currAggregate, aggregate));
                    break;
                case MAX:
                    this.fieldAggregate.put(gbfield, Math.max(currAggregate, aggregate));
                    break;
                default:
                    System.err.println("mergeTupleIntoGroup ran into an error");
                    break;
            }
        }

        // Increment count map by 1
        this.fieldCount.put(gbfield, this.fieldCount.getOrDefault(gbfield, 0) + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuplesArr = new ArrayList<>();
        if (this.gbfield != Aggregator.NO_GROUPING) {
            // Form TupleDesc
            // name of aggregate column format = aggregate op, aggregate field
            TupleDesc td = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE},
                    new String[]{this.gbfieldName, this.what.toString() + ", " + this.afieldName});

            // create tuples
            for (Field f : this.fieldAggregate.keySet()) {

                Tuple tup = new Tuple(td);
                tup.setField(0, f);
                tup.setField(1, new IntField(getAggregate(f)));
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
            tup.setField(0, new IntField(getAggregate(null)));
            tuplesArr.add(tup);

            // Create TupleIterator
            return new TupleIterator(td, tuplesArr);
        }
    }

    private int getAggregate(Field f) {
        int aggregate = -1;
        switch (this.what) {
            case COUNT:
                aggregate = this.fieldCount.get(f);
                break;
            case SUM:
            case MIN:
            case MAX:
                aggregate = this.fieldAggregate.get(f);
                break;
            case AVG:
                aggregate = this.fieldAggregate.get(f) / this.fieldCount.get(f);
                break;
            default:
                System.err.println("getAggregate ran into an error");
                break;
        }
        return aggregate;

    }

}
