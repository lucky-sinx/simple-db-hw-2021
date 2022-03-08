package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        //return null;
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        //return null;
        Page page = null;
        int pageSize = BufferPool.getPageSize();
        int offset = pid.getPageNumber() * BufferPool.getPageSize();
        byte[] data = new byte[pageSize];
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            randomAccessFile.seek(offset);
            randomAccessFile.read(data);
            page = new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        //Push the specified page to disk.将指定的页面刷新到内存
        // some code goes here
        // not necessary for lab1
        PageId id = page.getId();
        int pgNo=id.getPageNumber();
        int offset = id.getPageNumber() * BufferPool.getPageSize();
        if(pgNo>numPages()){
            throw new IOException("");
        }
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.seek(offset);
        byte[] pageData = page.getPageData();
        randomAccessFile.write(pageData);
        randomAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        //return 0;
        return (int) Math.ceil(file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        //return null;
        // not necessary for lab1
        List<Page> res = new LinkedList<>();
        for (int i = 0; i < numPages(); i++) {

            PageId pageId = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                res.add(page);
                break;
            }
        }
        if (res.size() == 0) {
            HeapPageId heapPageId = new HeapPageId(getId(), numPages());
            HeapPage newPage = new HeapPage(heapPageId, HeapPage.createEmptyPageData());
            newPage.insertTuple(t);
            newPage.markDirty(true, tid);
            res.add(newPage);
            writePage(newPage);
        }
        return res;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> res = new ArrayList<>();
        RecordId recordId = t.getRecordId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, recordId.getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        res.add(page);
        page.markDirty(true, tid);
        return res;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        //return null;

        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator {
        private final TransactionId tid;
        private int pagePos = 0;
        private Iterator<Tuple> pageIterator = null;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        public Iterator<Tuple> getPageIterator() {
            Page page = null;
            try {
                page = Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pagePos), Permissions.READ_ONLY);
                pagePos++;
            } catch (TransactionAbortedException e) {
                e.printStackTrace();
            } catch (DbException e) {
                e.printStackTrace();
            }
            return ((HeapPage) page).iterator();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            pagePos = 0;
            pageIterator = getPageIterator();
        }

        @Override
        public void close() {
            pageIterator = null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pageIterator = getPageIterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            //return false;
            if (pageIterator == null) return false;//暂未初始化
            if (pageIterator.hasNext()) return true;//当前页还有下一项
            //获取下一页
            while (pagePos < numPages()) {
                pageIterator = getPageIterator();
                if (pageIterator.hasNext()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (pageIterator == null) {
                throw new NoSuchElementException();
            }
            if (pageIterator.hasNext()) {
                return pageIterator.next();
            }
            pageIterator = getPageIterator();
            if (!pageIterator.hasNext()) {
                throw new NoSuchElementException();
            }
            return pageIterator.next();
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }
    }

}

