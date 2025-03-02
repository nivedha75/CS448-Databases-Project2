package heap;

import global.*;
import java.io.IOException;
import java.util.TreeMap;
import java.util.Iterator;
import bufmgr.*;
import diskmgr.*;


/**
 * <h3>Minibase Heap Files</h3>
 * A heap file is an unordered set of records, stored on a set of pages. This
 * class provides basic support for inserting, selecting, updating, and deleting
 * records. Temporary heap files are used for external sorting and in other
 * relational operators. A sequential scan of a heap file (via the Scan class)
 * is the most basic access method.
 */
public class HeapFile implements GlobalConst {

  private static final int MIN_FREE_SPACE = 50;  
  private String fileName;
  PageId firstPageId;
  private TreeMap<Integer, PageId> freeSpaceMap;  

  /**
   * If the given name already denotes a file, this opens it; otherwise, this
   * creates a new empty file. A null name produces a temporary heap file which
   * requires no DB entry.
      * @throws HashEntryNotFoundException 
      * @throws PageUnpinnedException 
         * @throws IOException 
         * @throws DiskMgrException 
         * @throws FileIOException 
         * @throws OutOfSpaceException 
         * @throws DuplicateEntryException 
         * @throws InvalidRunSizeException 
         * @throws InvalidPageNumberException 
         * @throws FileNameTooLongException 
            */
    public HeapFile(String name) throws PageUnpinnedException, HashEntryNotFoundException, FileNameTooLongException, InvalidPageNumberException, InvalidRunSizeException, DuplicateEntryException, OutOfSpaceException, FileIOException, DiskMgrException, IOException {
      //PUT YOUR CODE HERE
      fileName = name;
      if (fileName == null) {
        firstPageId = new PageId(INVALID_PAGE);
        freeSpaceMap = new TreeMap<>();
    } else {
        firstPageId = SystemDefs.JavabaseDB.get_file_entry(fileName);
        if (firstPageId == null) {
            Page firstPage = new Page();
            firstPageId = SystemDefs.JavabaseBM.newPage(firstPage, 1);
            if (firstPageId == null) {
                throw new RuntimeException("Failed to allocate first page.");
            }

            HFPage hfPage = new HFPage();
            SystemDefs.JavabaseBM.unpinPage(firstPageId, true);

            SystemDefs.JavabaseDB.add_file_entry(fileName, firstPageId);

            freeSpaceMap = new TreeMap<>();
            freeSpaceMap.put((int) hfPage.getFreeSpace(), firstPageId);
        }
    }

  }

  /**
   * Called by the garbage collector when there are no more references to the
   * object; deletes the heap file if it's temporary.
   */
  protected void finalize() throws Throwable {
      //PUT YOUR CODE HERE
      if (fileName == null) {
        deleteFile();
    }
  }

  /**
   * Deletes the heap file from the database, freeing all of its pages.
      * @throws IOException 
      * @throws DiskMgrException 
      * @throws InvalidPageNumberException 
      * @throws FileIOException 
      * @throws FileEntryNotFoundException 
      */
     public void deleteFile() throws FileEntryNotFoundException, FileIOException, InvalidPageNumberException, DiskMgrException, IOException {
    //PUT YOUR CODE HERE
    PageId currentPageId = new PageId(firstPageId.pid);
        Page page = new Page();

        while (currentPageId.pid != INVALID_PAGE) {
            try {
                SystemDefs.JavabaseBM.pinPage(currentPageId, page, false);
                HFPage hfPage = new HFPage(page);
                PageId nextPageId = hfPage.getNextPage();
                SystemDefs.JavabaseBM.unpinPage(currentPageId, false);
                SystemDefs.JavabaseBM.freePage(currentPageId);
                currentPageId = nextPageId;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (fileName != null) {
            SystemDefs.JavabaseDB.delete_file_entry(fileName);
        }
  }

  /**
   * Inserts a new record into the file and returns its RID.
      * @throws IOException 
      * @throws FileIOException 
      * @throws InvalidPageNumberException 
      * @throws HashEntryNotFoundException 
      * @throws BufferPoolExceededException 
         * @throws PageUnpinnedException 
            * 
            * @throws IllegalArgumentException if the record is too large
            */
  public RID insertRecord(byte[] record) throws BufferPoolExceededException, HashEntryNotFoundException, InvalidPageNumberException, FileIOException, IOException, PageUnpinnedException {
    //PUT YOUR CODE HERE
    if (record.length > MINIBASE_PAGESIZE - HFPage.HEADER_SIZE) {
      throw new IllegalArgumentException("Record size too large.");
  }

  PageId targetPageId = null;
  HFPage targetPage = null;
  Page page = new Page();

  // Find a page with enough free space
  for (Iterator<Integer> it = freeSpaceMap.keySet().iterator(); it.hasNext(); ) {
      int freeSpace = it.next();
      if (freeSpace >= record.length + MIN_FREE_SPACE) {
          targetPageId = freeSpaceMap.get(freeSpace);
          break;
      }
  }

  if (targetPageId == null) {
      targetPageId = SystemDefs.JavabaseBM.newPage(page, 1);
      if (targetPageId == null) {
          throw new RuntimeException("Failed to allocate a new page.");
      }
      targetPage = new HFPage(page);
  } else {
      SystemDefs.JavabaseBM.pinPage(targetPageId, page, false);
      targetPage = new HFPage(page);
  }

  RID rid = targetPage.insertRecord(record);
  freeSpaceMap.put((int) targetPage.getFreeSpace(), targetPageId);

  SystemDefs.JavabaseBM.unpinPage(targetPageId, true);
  return rid;
  }

  /**
   * Reads a record from the file, given its id.
      * @throws IOException 
      * @throws FileIOException 
      * @throws InvalidPageNumberException 
      * @throws HashEntryNotFoundException 
      * @throws BufferPoolExceededException 
         * @throws PageUnpinnedException 
            * 
            * @throws IllegalArgumentException if the rid is invalid
            */
           public byte[] selectRecord(RID rid) throws BufferPoolExceededException, HashEntryNotFoundException, InvalidPageNumberException, FileIOException, IOException, PageUnpinnedException {
    //PUT YOUR CODE HERE
    Page page = new Page();
    SystemDefs.JavabaseBM.pinPage(rid.pageNo, page, false);
    HFPage hfPage = new HFPage(page);
    byte[] record = hfPage.selectRecord(rid);
    SystemDefs.JavabaseBM.unpinPage(rid.pageNo, false);
    return record;
  }

  /**
   * Updates the specified record in the heap file.
      * @throws IOException 
      * @throws PageUnpinnedException 
      * @throws FileIOException 
      * @throws InvalidPageNumberException 
      * @throws HashEntryNotFoundException 
      * @throws BufferPoolExceededException FPage.HEADER_SIZE) {
         throw new IllegalArgumentException("New record size too large.");
       }
   
       deleteRecord(rid);
       insertRecord( throws BufferPoolExceededException, HashEntryNotFoundException, InvalidPageNumberException, FileIOException, PageUnpinnedException, IOException * @throws InvalidPageNumberException 
      * @throws HashEntryNotFoundException 
      * @throws BufferPoolExceededException 
         * @throws PageUnpinnedException 
            * 
            * @throws IllegalArgumentException if the rid is invalid
            */
  public void deleteRecord(RID rid) throws BufferPoolExceededException, HashEntryNotFoundException, InvalidPageNumberException, FileIOException, IOException, PageUnpinnedException {
    //PUT YOUR CODE HERE
    Page page = new Page();
    SystemDefs.JavabaseBM.pinPage(rid.pageNo, page, false);
    HFPage hfPage = new HFPage(page);
    hfPage.deleteRecord(rid);
    freeSpaceMap.put((int) hfPage.getFreeSpace(), rid.pageNo);
    SystemDefs.JavabaseBM.unpinPage(rid.pageNo, true);
  }

  /**
   * Gets the number of records in the file.
   */
  public int getRecCnt() {
    //PUT YOUR CODE HERE
    int count = 0;
    PageId currentPageId = new PageId(firstPageId.pid);
    Page page = new Page();

    while (currentPageId.pid != INVALID_PAGE) {
        try {
            SystemDefs.JavabaseBM.pinPage(currentPageId, page, false);
            HFPage hfPage = new HFPage(page);
            count += hfPage.getSlotCount();
            currentPageId = hfPage.getNextPage();
            SystemDefs.JavabaseBM.unpinPage(currentPageId, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    return count;
  }

  /**
   * Initiates a sequential scan of the heap file.
   */
  public HeapScan openScan() {
    return new HeapScan(this);
  }

  /**
   * Returns the name of the heap file.
   */
  public String toString() {
    //PUT YOUR CODE HERE
    return fileName;
  }

} // public class HeapFile implements GlobalConst
