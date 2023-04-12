package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
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
        return this.f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here
        try {
            int pgSz = BufferPool.getPageSize();

            // BufferPool contains multiple pages, so to start reading
            // from a certain page, need to move the corresponding no
            // of bytes from the start to that page
            int offset = pid.getPageNumber() * pgSz;
            byte[] pgData = new byte[pgSz];

            RandomAccessFile reader = new RandomAccessFile(f, "r");

            reader.seek(offset);  // "seek" to the start of byte of the page
            reader.read(pgData);  // read pgSz amount and store it in pgData
            reader.close();       // close after finish reading

            return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), pgData);
        } catch (IOException e) {
            throw new IllegalArgumentException("page does not exist in this file");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        // divide total num of bytes in file by page size
        return (int) Math.ceil(this.f.length() * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    private class HeapFileIterator implements DbFileIterator {
        private HeapFile hf;
        private TransactionId tid;
        private Iterator<Tuple> tupleIter;
        private int currPgNo;

        public HeapFileIterator(HeapFile hf, TransactionId tid) {
            this.hf = hf;
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.currPgNo = 0;
            this.tupleIter = getHeapPageIterator(this.currPgNo);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (this.tupleIter != null) {
                if (tupleIter.hasNext()) {
                    return true;
                }

                // if hasNext() is false because there are no more tuples
                // in the current page, we go to the next page to get
                // more tuples
                while (!this.tupleIter.hasNext()) {
                    this.currPgNo++;
                    if (this.currPgNo >= hf.numPages()) {
                        // if after incrementing currPgNo it exceeds the num
                        // of pages in HeapFile, there are no more pages left
                        // hence no more tuples left
                        return false;
                    }

                    // re-initialize iterator for the new page
                    this.tupleIter = getHeapPageIterator(this.currPgNo);
                }

                // found a new page with existing tuples
                // because this.tupleIter.hasNext() is true
                return true;
            }

            // iterator does not exist, has not called open()
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            return this.tupleIter.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        @Override
        public void close() {
            this.tupleIter = null;
            this.currPgNo = 0;
        }

        private Iterator<Tuple> getHeapPageIterator(int pgNo) throws DbException, TransactionAbortedException {
            if (pgNo < 0) {
                return null;
            }

            if (pgNo >= hf.numPages()) {
                return null;
            }

            HeapPageId hpId = new HeapPageId(this.hf.getId(), currPgNo);

            // iterator must use the BufferPool.getPage() method
            // to access pages in the HeapFile
            HeapPage hp = (HeapPage) Database.getBufferPool().getPage(this.tid, hpId, Permissions.READ_ONLY);

            return hp.iterator();
        }

    }

}