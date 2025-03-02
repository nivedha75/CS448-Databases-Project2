package heap;

import global.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * <h3>Minibase Heap Files</h3>
 * A heap file is an unordered set of records, stored on a set of pages. This
 * class provides basic support for inserting, selecting, updating, and deleting
 * records. Temporary heap files are used for external sorting and in other
 * relational operators. A sequential scan of a heap file (via the Scan class)
 * is the most basic access method.
 */
public class HeapFile implements GlobalConst {

  private String fName; 
  protected int fStatus;  
  protected int numRec; 
  protected HFPage curr;  
  protected ArrayList<PageId> pages;
  protected ArrayList<Integer> pids;  

  /**
   * If the given name already denotes a file, this opens it; otherwise, this
   * creates a new empty file. A null name produces a temporary heap file which
   * requires no DB entry.
   */
  public HeapFile(String name) {
      //PUT YOUR CODE HERE
      this.fName = name;
      this.fStatus = 0;
      this.numRec = 0;
      this.pages = new ArrayList<>();
      this.pids = new ArrayList<>();
      Page newP = new Page(); 
      if (name != null) {
        PageId first = Minibase.DiskManager.get_file_entry(name);
        if (first != null) {
          loadHF(newP, first);
        } else {
          first = createHF(newP);
        }
      } else {
        initialize(newP);
      }
  }

  private PageId createHF(Page newPage) {
    PageId first = Minibase.BufferManager.newPage(newPage, 1);
    Minibase.DiskManager.add_file_entry(fName, first);
    Minibase.BufferManager.unpinPage(first, UNPIN_DIRTY);
    pids.add(first.pid);
    pages.add(first);
    Minibase.BufferManager.pinPage(first, newPage, PIN_DISKIO);
    curr = new HFPage(newPage);
    curr.initDefaults();
    curr.setCurPage(first);
    Minibase.BufferManager.unpinPage(first, UNPIN_DIRTY);
    return first;
  }

  private void loadHF(Page newP, PageId first) {
    Minibase.BufferManager.pinPage(first, newP, PIN_DISKIO);
    pids.add(first.pid);
    pages.add(first);
    curr = new HFPage(newP);
    curr.setCurPage(first);
    curr.setData(newP.getData());
    Minibase.BufferManager.unpinPage(first, UNPIN_CLEAN);
    RID record = curr.firstRecord();
    while (record != null) {
      numRec++;
      record = curr.nextRecord(record);
    }
    PageId np = curr.getNextPage();
    while (np.pid > 0) {
      HFPage nextHFP = new HFPage();
      Minibase.BufferManager.pinPage(np, nextHFP, PIN_DISKIO);
      pids.add(np.pid);
      pages.add(np);
      record = nextHFP.firstRecord();
      while (record != null) {
        numRec++;
        record = nextHFP.nextRecord(record);
      }
      Minibase.BufferManager.unpinPage(np, UNPIN_CLEAN);
      np = nextHFP.getNextPage();
    }
  }

  private void initialize(Page newP) {
    PageId temp = Minibase.DiskManager.get_file_entry(null);
    pids.add(temp.pid);
    pages.add(temp);
    curr = new HFPage(newP);
    curr.setCurPage(temp);
    Minibase.BufferManager.unpinPage(temp, UNPIN_DIRTY);
  }

  /**
   * Called by the garbage collector when there are no more references to the
   * object; deletes the heap file if it's temporary.
   */
  protected void finalize() throws Throwable {
      //PUT YOUR CODE HERE
      if (fName == null) { 
        deleteFile();
      }
  }

  /**
   * Deletes the heap file from the database, freeing all of its pages.
   */
  public void deleteFile() {
    //PUT YOUR CODE HERE
    if (fStatus == 0) {
      for (PageId i : pages) {
        Minibase.DiskManager.deallocate_page(i);
      }
      Minibase.DiskManager.delete_file_entry(fName);
      fStatus = 1;
      numRec = 0;
      pages.clear();
      pids.clear();
    }
  }

  /**
   * Inserts a new record into the file and returns its RID.
   * 
   * @throws IllegalArgumentException if the record is too large
   */
  public RID insertRecord(byte[] record) throws Exception {
    //PUT YOUR CODE HERE
    if (HFPage.HEADER_SIZE + record.length > 1024) {
      throw new SpaceNotAvailableException("The record is too large!");
    }
    PageId target = pages.get(pages.size() - 1);
    Page cp = new Page();
    Minibase.BufferManager.pinPage(target, cp, PIN_DISKIO);
    HFPage ch = new HFPage(cp);
    ch.setCurPage(target);
    if (record.length < ch.getFreeSpace()) {
      numRec++;
      RID rec = ch.insertRecord(record);
      Minibase.BufferManager.unpinPage(target, UNPIN_DIRTY);
      return rec;
    }
    Minibase.BufferManager.unpinPage(target, UNPIN_CLEAN);
    return createInsertPage(record);
  }

  private RID createInsertPage(byte[] record) throws Exception {
    Page newP = new Page();
    PageId newPid = Minibase.BufferManager.newPage(newP, 1);
    HFPage newHFP = new HFPage(newP);
    curr.setNextPage(newPid);
    newHFP.initDefaults();
    newHFP.setCurPage(newPid);
    newHFP.setPrevPage(curr.getCurPage());
    RID rec = newHFP.insertRecord(record);
    numRec++;
    pids.add(newPid.pid);
    pages.add(newPid);
    curr = newHFP;
    Minibase.BufferManager.unpinPage(newPid, UNPIN_DIRTY);
    Collections.sort(pages, new FreeSpace());
    return rec;
  }

/**
   * Reads a record from the file, given its id.
   * 
   * @throws IllegalArgumentException if the rid is invalid
   */
  public Tuple getRecord(RID rid) throws Exception{
    //PUT YOUR CODE
    if (!pids.contains(rid.pageno.pid)) {
      throw new IllegalArgumentException("The RID is invalid");
    }

    HFPage page = new HFPage();
    Minibase.BufferManager.pinPage(rid.pageno, page, PIN_DISKIO);
    byte[] record = page.selectRecord(rid);
    Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_CLEAN);

    return new Tuple(record, 0, record.length);
  }

  /**
   * Updates the specified record in the heap file.
   * 
   * @throws IllegalArgumentException if the rid or new record is invalid
   */

  public boolean updateRecord(RID rid, Tuple newRecord) throws Exception{
    HFPage page = new HFPage();
    Minibase.BufferManager.pinPage(rid.pageno, page, false);

    try {
      page.updateRecord(rid, newRecord);
      Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_DIRTY);
      return true;
    } catch (IllegalArgumentException e) {
      Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_CLEAN);
      throw new InvalidUpdateException();
    } 

  } 


  /**
   * Deletes the specified record from the heap file.
   * 
   * @throws IllegalArgumentException if the rid is invalid
   */
  public boolean deleteRecord(RID rid) throws Exception{
    HFPage page = new HFPage();
    Minibase.BufferManager.pinPage(rid.pageno, page, false);

    try {
      page.deleteRecord(rid);
      Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_DIRTY);
      return true;
    } catch (IllegalArgumentException e) {
      Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_CLEAN);
      throw new IllegalArgumentException("The RID is invalid");
    }
  }

  /**
   * Reads a record from the file, given its id.
   * 
   * @throws IllegalArgumentException if the rid is invalid
   */
  // public byte[] selectRecord(RID rid) {
  //   //PUT YOUR CODE HERE
  // }

  // /**
  //  * Updates the specified record in the heap file.
  //  * 
  //  * @throws IllegalArgumentException if the rid or new record is invalid
  //  */
  // public void updateRecord(RID rid, byte[] newRecord) {
  //   //PUT YOUR CODE HERE
  // }

  // /**
  //  * Deletes the specified record from the heap file.
  //  * 
  //  * @throws IllegalArgumentException if the rid is invalid
  //  */
  // public void deleteRecord(RID rid) {
  //   //PUT YOUR CODE HERE
  // }

  /**
   * Gets the number of records in the file.
   */
  public int getRecCnt() {
    //PUT YOUR CODE HERE
    return numRec;
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
    return this.fName;
  }

} // public class HeapFile implements GlobalConst

class FreeSpace implements Comparator<PageId>
{
  public int compare(PageId one, PageId two) {
    return getFreeSpace(one) - getFreeSpace(two);
  }

  private short getFreeSpace(PageId pid) {
    HFPage first = new HFPage();
    Minibase.BufferManager.pinPage(pid, first, GlobalConst.PIN_DISKIO);
    short freeSpace = first.getFreeSpace();
    Minibase.BufferManager.unpinPage(pid, GlobalConst.UNPIN_CLEAN);
    return freeSpace;
  }
}