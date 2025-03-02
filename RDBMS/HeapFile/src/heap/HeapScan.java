package heap;

import java.util.*;
import global.*;
import chainexception.ChainException;


/**
 * A HeapScan object is created only through the function openScan() in the
 * HeapFile class. It supports the getNext interface which will simply retrieve
 * the next record in the file.
 */
public class HeapScan implements GlobalConst {

  Iterator<PageId> pidIter;
  HFPage hfp; 
  RID rec;  

  /**
   * Constructs a file scan by pinning the directoy header page and initializing
   * iterator fields.
   */
  protected HeapScan(HeapFile hf) {
    //PUT YOUR CODE HERE
    pidIter = hf.pages.iterator();
    PageId first = pidIter.next();
    Page newP = new Page();
    Minibase.BufferManager.pinPage(first, newP, false);
    hfp = new HFPage(newP);
    rec = hfp.firstRecord();
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
  public void close() throws ChainException {
    //PUT YOUR CODE HERE
    pidIter = null;
    hfp = null;
    rec = null;
  }

  /**
   * Returns true if there are more records to scan, false otherwise.
   */
  public boolean hasNext() {
    //PUT YOUR CODE HERE
    boolean next = pidIter.hasNext();
    return next;
  }

  /**
   * Gets the next record in the file scan.
   * 
   * @param rid output parameter that identifies the returned record
   * @throws IllegalStateException if the scan has no more elements
   */
  public Tuple getNext(RID rid) {
    //PUT YOUR CODE HERE
    if(rec != null)
      {
        rid.copyRID(rec);
        byte[] records = hfp.selectRecord(rid);
        Tuple tup = new Tuple(records, 0, records.length);
        rec = hfp.nextRecord(rec);
        return tup;
      }
      Minibase.BufferManager.unpinPage(hfp.getCurPage(), false);
      if(pidIter.hasNext() == true) {
        Minibase.BufferManager.pinPage(pidIter.next(), hfp, false);  
        rec = hfp.firstRecord();
        if(rec != null) {
          rid.copyRID(rec);
          byte[] records = hfp.selectRecord(rid);
          Tuple tup = new Tuple(records, 0, records.length);
          rec = hfp.nextRecord(rec);
          return tup; 
        }
        Minibase.BufferManager.unpinPage(pidIter.next(), false); 
        return null;
      }
      return null;
  }

} // public class HeapScan implements GlobalConst
