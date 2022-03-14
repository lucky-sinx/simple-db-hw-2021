package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 *
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;
    private final int tableId;
    private final TupleDesc td;
    private final int numPages;
    private final int ioCostPerPage;

    protected HashMap<Integer,IntHistogram> intHistMap;
    protected HashMap<Integer,StringHistogram> stringHistMap;
    private HashMap<Integer,Integer> minMap;
    private HashMap<Integer,Integer> maxMap;
    private int numTuples;



    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.ioCostPerPage=ioCostPerPage;
        this.numTuples=0;
        this.tableId=tableid;
        this.td=Database.getCatalog().getTupleDesc(tableid);
        this.numPages=((HeapFile)Database.getCatalog().getDatabaseFile(tableid)).numPages();
        minMap=new HashMap<>();
        maxMap=new HashMap<>();
        intHistMap=new HashMap<>();
        stringHistMap=new HashMap<>();

        try {
            getMinAndMax();
            addHist();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } catch (DbException e) {
            e.printStackTrace();
        }

    }

    private void addHist() throws TransactionAbortedException, DbException {
        //扫描将所有值加入到对应的直方图直方图中去
        TransactionId transactionId = new TransactionId();
        SeqScan seqScan = new SeqScan(transactionId, tableId, "");
        seqScan.open();
        for(int i=0;i<td.numFields();i++){
            if(td.getFieldType(i)==Type.STRING_TYPE){
                stringHistMap.put(i,new StringHistogram(NUM_HIST_BINS));
            }else{
                intHistMap.put(i,new IntHistogram(NUM_HIST_BINS,minMap.get(i),maxMap.get(i)));
            }
        }
        while (true){
            try {
                if (!seqScan.hasNext()) break;
                Tuple tuple = seqScan.next();
                for(int i=0;i<td.numFields();i++){
                    if(td.getFieldType(i)==Type.STRING_TYPE){
                        StringField field = (StringField) tuple.getField(i);
                        String value = field.getValue();
                        stringHistMap.get(i).addValue(value);
                    }else{
                        IntField field = (IntField) tuple.getField(i);
                        int value = field.getValue();
                        intHistMap.get(i).addValue(value);
                    }
                }
            } catch (TransactionAbortedException e) {
                e.printStackTrace();
            } catch (DbException e) {
                e.printStackTrace();
            }
        }
    }

    private void getMinAndMax() throws TransactionAbortedException, DbException {
        TransactionId transactionId = new TransactionId();
        SeqScan seqScan = new SeqScan(transactionId, tableId, "");
        seqScan.open();
        //一次扫描统计所有int类型的最大值
        while (true){
            try {
                if (!seqScan.hasNext()) break;
                Tuple tuple = seqScan.next();
                numTuples++;
                for(int i=0;i<td.numFields();i++){
                    if(td.getFieldType(i)==Type.STRING_TYPE){
                        continue;
                    }else{
                        IntField field = (IntField) tuple.getField(i);
                        int value = field.getValue();
                        minMap.put(i,Math.min(value,minMap.getOrDefault(i,Integer.MAX_VALUE)));
                        maxMap.put(i,Math.max(value,maxMap.getOrDefault(i,Integer.MIN_VALUE)));
                    }
                }
            } catch (TransactionAbortedException e) {
                e.printStackTrace();
            } catch (DbException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     *
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        //return 0;
        return numPages * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        //return 0;
        return (int) (selectivityFactor * numTuples);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        //return 1.0;
        if(td.getFieldType(field)==Type.STRING_TYPE){
            return stringHistMap.get(field).estimateSelectivity(op, ((StringField) constant).getValue());
        }else {
            return intHistMap.get(field).estimateSelectivity(op, ((IntField) constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        //return 0;
        return numTuples;
    }

}
