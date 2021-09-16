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
    private final File diskFile;
    private final RandomAccessFile file;
    private final TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.diskFile = f;

        try {
            this.file = new RandomAccessFile(f, "rw");
        } catch (FileNotFoundException ex) {
            throw new RuntimeException(ex.getMessage());
        }

        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.diskFile;
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
        return this.diskFile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int tid = pid.getTableId();
        int pgNo = pid.getPageNumber();
        int pageSize = BufferPool.getPageSize();

        int offset = pgNo * pageSize;
        byte[] buffer = new byte[pageSize];

        try {
            this.file.seek(offset);
            this.file.read(buffer, 0, pageSize);

            return new HeapPage(new HeapPageId(tid, pgNo), buffer);
        } catch (IOException ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        int pgNo = page.getId().getPageNumber();
        int pgSize = BufferPool.getPageSize();

        this.file.seek(pgNo * pgSize);
        this.file.write(page.getPageData(), 0, pgSize);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(diskFile.length() * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
       List<Page> pages = new ArrayList<>();

       for(int i = 0; i < numPages(); i++) {
           HeapPage page = (HeapPage) Database
                   .getBufferPool()
                   .getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);

           if (page.getNumEmptySlots() > 0) {
               page.insertTuple(t);
               pages.add(page);
               break;
           }
       }

       if (pages.isEmpty()) {
           byte[] emptyPageData = HeapPage.createEmptyPageData();
           HeapPage heapPage = new HeapPage(new HeapPageId(getId(), numPages()), emptyPageData);

           writePage(heapPage);

           HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, heapPage.getId(), Permissions.READ_WRITE);
           page.insertTuple(t);

           pages.add(page);
       }

       return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> pages = new ArrayList<>();

        HeapPage page = ((HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE));

        page.deleteTuple(t);
        pages.add(page);

        return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileDbIterator(this, tid);
    }

}

