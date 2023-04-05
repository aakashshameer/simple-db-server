package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return new TDItemIterator();
    }

    private class TDItemIterator implements Iterator<TDItem> {
        private int currIndex;

        public TDItemIterator() {
            this.currIndex = 0;
        }

        @Override
        public boolean hasNext() {
            return this.currIndex < TDItems.size();
        }

        @Override
        public TDItem next() {
            // Cannot use next() on empty or at the end of list
            if (!hasNext()) {
                throw new NoSuchElementException("No more elements in the iteration");
            }

            // return element at currIndex then increment currIndex
            return TDItems.get(currIndex++);
        }
    }

    private static final long serialVersionUID = 1L;
    private List<TDItem> TDItems;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if (typeAr == null || typeAr.length == 0) {
            throw new IllegalArgumentException("typeAr must contain at least one entry");
        }

        // Creates a new TupleDesc with typeAr.length fields
        this.TDItems = new ArrayList<>();

        // Associates each specified typed fields with its name.
        for (int i = 0; i < typeAr.length; i++) {
            this.TDItems.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.TDItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= numFields()) {
            throw new NoSuchElementException("Invalid field index reference");
        }
        return TDItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= numFields()) {
            throw new NoSuchElementException("Invalid field index reference");
        }
        return TDItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here

        if (name != null) {
            for (int i = 0; i < numFields(); i++) {

                // skip over null fields
                if (this.TDItems.get(i).fieldName == null) {
                    continue;
                }

                if (this.TDItems.get(i).fieldName.equals(name)) {
                    return i;
                }
            }
        }
        throw new NoSuchElementException("Matching field name not found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (TDItem item : TDItems) {
            size += item.fieldType.getLen();
        }

        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int totalFields = td1.numFields() + td2.numFields();
        Type[] typeAr = new Type[totalFields];
        String[] fieldAr = new String[totalFields];

        // Keep running count of tuples already inserted using the index
        int index = 0;

        // Copy td1 items into arrays
        for (TDItem item : td1.TDItems) {
            typeAr[index] = item.fieldType;
            fieldAr[index] = item.fieldName;
            index++;
        }

        // Copy td2 items into arrays
        for (TDItem item : td2.TDItems) {
            typeAr[index] = item.fieldType;
            fieldAr[index] = item.fieldName;
            index++;
        }

        // Return resulting merged TupleDesc object
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        // same object reference
        if (this == o) {
            return true;
        }

        // different object references --> need to check matching class and fields
        if (o instanceof TupleDesc) {
            TupleDesc other = (TupleDesc) o;

            // check number of items
            if (this.numFields() != other.numFields()) {
                return false;
            }

            // check matching i-th type of TupleDesc fields
            for (int i = 0; i < TDItems.size(); i++) {
                if (!Objects.equals(TDItems.get(i).fieldType, other.TDItems.get(i).fieldType)) {
                    return false;
                }
            }

            return true;
        }

        // o == null OR this.getClass() != o.getClass()
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < numFields(); i++) {
            TDItem item = TDItems.get(i);
            sb.append(item.fieldType).append("(").append(item.fieldName).append(")");

            if (i < numFields() - 1) {
                sb.append(", ");
            }
        }

        return sb.toString();
    }
}
