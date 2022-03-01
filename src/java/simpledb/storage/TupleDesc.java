package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private ArrayList<TDItem> TDItemList;


    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TDItem)) return false;
            TDItem tdItem = (TDItem) o;
            return fieldType == tdItem.fieldType && Objects.equals(fieldName, tdItem.fieldName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldType, fieldName);
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        //return null;
        return TDItemList.iterator();
    }

    public TupleDesc() {
        // some code goes here
        TDItemList = new ArrayList<>();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        int len = typeAr.length;
        TDItemList = new ArrayList<>();
        for (int i = 0; i < len; i++) {
            TDItemList.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        int len = typeAr.length;
        TDItemList = new ArrayList<>();
        for (int i = 0; i < len; i++) {
            TDItemList.add(new TDItem(typeAr[i], null));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        //return 0;
        return TDItemList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        //return null;
        int len = numFields();
        if (i < 0 || i >= len) {
            throw new NoSuchElementException("i is not a valid field reference");
        }
        return TDItemList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        //return null;
        int len = numFields();
        if (i < 0 || i >= len) {
            throw new NoSuchElementException("i is not a valid field reference");
        }
        return TDItemList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if(name==null){
            throw new NoSuchElementException("no field with a matching name is found");
        }
        int res = -1;
        int len = numFields();
        for (int i = 0; i < len; i++) {
            if (name.equals(TDItemList.get(i).fieldName)) {
                res = i;
                break;
            }
        }
        if (res == -1) {
            throw new NoSuchElementException("no field with a matching name is found");
        }
        return res;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = TDItemList.stream()
                .mapToInt(value -> {
                    return value.fieldType.getLen();
                })
                .sum();
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        TupleDesc tupleDesc = new TupleDesc();
        td1.iterator().forEachRemaining(td -> {
            tupleDesc.TDItemList.add(new TDItem(td.fieldType, td.fieldName));
        });
        td2.iterator().forEachRemaining(td -> {
            tupleDesc.TDItemList.add(new TDItem(td.fieldType, td.fieldName));
        });
        return tupleDesc;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (this == o) return true;
        if (!(o instanceof TupleDesc)) return false;
        if (o == null) return false;
        TupleDesc td = (TupleDesc) o;
        if (td.getSize() != getSize() || td.numFields() != numFields()) return false;
        int index = 0;
        Iterator<TDItem> iterator = td.iterator();
        while (iterator.hasNext()) {
            TDItem next = iterator.next();
            if (next.fieldType != TDItemList.get(index++).fieldType) {
                return false;
            }
        }
        return true;
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
        StringBuilder stringBuilder = new StringBuilder();
        int len = numFields();
        stringBuilder.append(TDItemList.get(0).toString());
        for (int i = 1; i < len; i++) {
            stringBuilder.append(", ");
            stringBuilder.append(TDItemList.get(i).toString());
        }
        return stringBuilder.toString();
    }
}
