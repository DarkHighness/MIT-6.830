package simpledb.storage;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileDbIterator implements DbFileIterator {
    private final HeapFile heapFile;
    private final TransactionId transactionId;
    private final int tableId;
    private Iterator<Tuple> pageIterator;
    private int pageCursor;

    public HeapFileDbIterator(HeapFile heapFile, TransactionId transactionId) {
        this.heapFile = heapFile;
        this.transactionId = transactionId;
        this.tableId = heapFile.getId();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        if (pageCursor >= heapFile.numPages())
            return;

        nextPage();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (pageIterator == null)
            return false;

        while (true) {
            if(pageIterator.hasNext())
                return true;

            pageCursor ++;
            if (pageCursor >= heapFile.numPages()) {
                pageIterator = null;
                return false;
            }

            nextPage();
        }
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (pageIterator == null)
            throw new NoSuchElementException();

        while (true) {
            if (pageIterator.hasNext())
                return pageIterator.next();

            pageCursor++;
            if (pageCursor >= heapFile.numPages()) {
                pageIterator = null;
                throw new NoSuchElementException();
            }

            nextPage();
        }
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        pageCursor = 0;

        nextPage();
    }

    private void nextPage() throws TransactionAbortedException, DbException {
        PageId pageId = new HeapPageId(tableId, pageCursor);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(transactionId, pageId, Permissions.READ_ONLY);
        pageIterator = page.iterator();
    }

    @Override
    public void close() {
        pageIterator = null;
    }
}
