package heap;

import java.io.IOException;
//import java.util.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
//import chainexception.ChainException;

/**
 * A HeapScan object is created only through the function openScan() in the
 * HeapFile class. It supports the getNext interface which will simply retrieve
 * the next record in the file.
 */
public class HeapScan implements GlobalConst {

  private HeapFile heapFile;
  private PageId currentDirPageId;
  private PageId currentDataPageId;
  private HFPage currentDirPage;
  private HFPage currentDataPage;
  private RID currentRID;
  private boolean scanOpen;

  /**
   * Constructs a file scan by pinning the directoy header page and initializing
   * iterator fields.
   */
  protected HeapScan(HeapFile hf) {
    //PUT YOUR CODE HERE
    this.heapFile = hf;
    this.scanOpen = true;
    try {
      if (hf.firstPageId.pid != INVALID_PAGE) {
          Page dirPage = new Page();
          SystemDefs.JavabaseBM.pinPage(hf.firstPageId, dirPage, false);
          currentDirPage = new HFPage(dirPage);
          currentDirPageId = hf.firstPageId;
          currentDataPageId = currentDirPageId;
          loadNextDataPage();
      }
    } catch (Exception e) {
      throw new RuntimeException("HeapScan initialization failed.", e);
    }
  }

  /**
   * Called by the garbage collector when there are no more references to the
   * object; closes the scan if it's still open.
   */
  protected void finalize() throws Throwable {
    //PUT YOUR CODE HERE
    close();
  }

  /**
   * Closes the file scan, releasing any pinned pages.
   */
  public void close() {
    //PUT YOUR CODE HERE
    if (!scanOpen) return;
        scanOpen = false;
        
        try {
            if (currentDataPageId != null && currentDataPageId.pid != INVALID_PAGE) {
                SystemDefs.JavabaseBM.unpinPage(currentDataPageId, false);
            }
            if (currentDirPageId != null && currentDirPageId.pid != INVALID_PAGE) {
                SystemDefs.JavabaseBM.unpinPage(currentDirPageId, false);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error closing HeapScan", e);
        }
  }

  /**
   * Returns true if there are more records to scan, false otherwise.
   */
  public boolean hasNext() {
    //PUT YOUR CODE HERE
    return scanOpen && currentDataPage != null && currentRID != null;
  }

  /**
   * Gets the next record in the file scan.
   * 
   * @param rid output parameter that identifies the returned record
   * @throws IllegalStateException if the scan has no more elements
   */
  public byte[] getNext(RID rid) {
    //PUT YOUR CODE HERE
    if (!hasNext()) {
      throw new IllegalStateException("No more records to scan.");
  }
  try {
      byte[] tuple = currentDataPage.selectRecord(currentRID);
      rid.copyRid(currentRID); 
      currentRID = currentDataPage.nextRecord(currentRID);
      if (currentRID == null) {
          SystemDefs.JavabaseBM.unpinPage(currentDataPageId, false);
          loadNextDataPage();
      }

      return tuple;
  } catch (Exception e) {
      throw new RuntimeException("Error reading next record", e);
  }
  }

  /**
     * Moves to the next data page in the heap file.
        * @throws FileIOException 
        * @throws BufferPoolExceededException 
           * @throws PageUnpinnedException 
                  */
  private void loadNextDataPage() throws IOException, InvalidPageNumberException, HashEntryNotFoundException, ReplacerException, BufferPoolExceededException, FileIOException, PageUnpinnedException {
        while (currentDataPageId != null && currentDataPageId.pid != INVALID_PAGE) {
            Page nextPage = new Page();
            SystemDefs.JavabaseBM.pinPage(currentDataPageId, nextPage, false);
            currentDataPage = new HFPage(nextPage);
            currentRID = currentDataPage.firstRecord();
            if (currentRID != null) {
                return;
            }
            currentDataPageId = currentDataPage.getNextPage();
            SystemDefs.JavabaseBM.unpinPage(currentDataPageId, false);
        }
        currentDataPage = null;
        currentRID = null;
    }
} // public class HeapScan implements GlobalConst
