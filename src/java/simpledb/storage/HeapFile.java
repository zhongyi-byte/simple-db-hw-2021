package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

import javax.swing.plaf.basic.BasicMenuUI.ChangeHandler;
import javax.xml.catalog.Catalog;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    File file;
    TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
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
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if(pid.getTableId() != getId())
            throw new IllegalArgumentException("pid.tableid() != getId()");
        if(pid.getPageNumber() < 0 || pid.getPageNumber() > numPages()){
            throw new IllegalArgumentException("illegal pgNumber");
        }
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            raf.seek(pid.getPageNumber() * BufferPool.getPageSize());
            byte[] data = new byte[BufferPool.getPageSize()];
            raf.read(data);
            raf.close();
            HeapPageId pageId = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            return new HeapPage(pageId, data);
        } catch (IOException e) {
            throw new RuntimeException(e);
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
        return (int)file.length()/BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
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
        return new DbFileIterator() {
            Iterator<Tuple> child;
            int pgNo = -1 ;
            private final int tableId = getId();
            private final BufferPool pool  = Database.getBufferPool();

            @Override
            public void open(){
                pgNo = 0;
                child = null;
            }

            @Override
            public boolean hasNext() {
                if(child != null && child.hasNext()) {
                    return true;
                }
                else if(pgNo < 0 || pgNo > numPages()) {
                    return false;
                }
                else {
                    try {
                        child = ((HeapPage)pool.getPage(tid, new HeapPageId(tableId,pgNo++), 
                        		Permissions.READ_ONLY)).iterator();
                    } catch (TransactionAbortedException | DbException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
					return hasNext();
                }
            }

            @Override 
            public Tuple next() {
                if(!hasNext()) {
                    throw new NoSuchElementException();
                }
                return child.next();
            }

            @Override
            public void rewind() {
                pgNo = 0;
                child = null;
            }

            @Override
            public void close() {
                pgNo = -1;
                child = null;
            }
        };
    }

}

