// package heap;

// import java.io.IOException;
// //import java.util.*;
// import global.*;
// import bufmgr.*;
// import diskmgr.*;
// //import chainexception.ChainException;

// /**
//  * A HeapScan object is created only through the function openScan() in the
//  * HeapFile class. It supports the getNext interface which will simply retrieve
//  * the next record in the file.
//  */
// public class HeapScan implements GlobalConst {

//   private HeapFile heapFile;
//   private PageId currentDirPageId;
//   private PageId currentDataPageId;
//   private HFPage currentDirPage;
//   private HFPage currentDataPage;
//   private RID currentRID;
//   private boolean scanOpen;

//   /**
//    * Constructs a file scan by pinning the directoy header page and initializing
//    * iterator fields.
//    */
//   protected HeapScan(HeapFile hf) {
//     //PUT YOUR CODE HERE
//     this.heapFile = hf;
//     this.scanOpen = true;
//     try {
//       if (hf.firstPageId.pid != INVALID_PAGE) {
//           Page dirPage = new Page();
//           SystemDefs.JavabaseBM.pinPage(hf.firstPageId, dirPage, false);
//           currentDirPage = new HFPage(dirPage);
//           currentDirPageId = hf.firstPageId;
//           currentDataPageId = currentDirPageId;
//           loadNextDataPage();
//       }
//     } catch (Exception e) {
//       throw new RuntimeException("HeapScan initialization failed.", e);
//     }
//   }

//   /**
//    * Called by the garbage collector when there are no more references to the
//    * object; closes the scan if it's still open.
//    */
//   protected void finalize() throws Throwable {
//     //PUT YOUR CODE HERE
//     close();
//   }

//   /**
//    * Closes the file scan, releasing any pinned pages.
//    */
//   public void close() {
//     //PUT YOUR CODE HERE
//     if (!scanOpen) return;
//         scanOpen = false;
        
//         try {
//             if (currentDataPageId != null && currentDataPageId.pid != INVALID_PAGE) {
//                 SystemDefs.JavabaseBM.unpinPage(currentDataPageId, false);
//             }
//             if (currentDirPageId != null && currentDirPageId.pid != INVALID_PAGE) {
//                 SystemDefs.JavabaseBM.unpinPage(currentDirPageId, false);
//             }
//         } catch (Exception e) {
//             throw new RuntimeException("Error closing HeapScan", e);
//         }
//   }

//   /**
//    * Returns true if there are more records to scan, false otherwise.
//    */
//   public boolean hasNext() {
//     //PUT YOUR CODE HERE
//     return scanOpen && currentDataPage != null && currentRID != null;
//   }

//   /**
//    * Gets the next record in the file scan.
//    * 
//    * @param rid output parameter that identifies the returned record
//    * @throws IllegalStateException if the scan has no more elements
//    */
//   public byte[] getNext(RID rid) {
//     //PUT YOUR CODE HERE
//     if (!hasNext()) {
//       throw new IllegalStateException("No more records to scan.");
//   }
//   try {
//       byte[] tuple = currentDataPage.selectRecord(currentRID);
//       rid.copyRid(currentRID); 
//       currentRID = currentDataPage.nextRecord(currentRID);
//       if (currentRID == null) {
//           SystemDefs.JavabaseBM.unpinPage(currentDataPageId, false);
//           loadNextDataPage();
//       }

//       return tuple;
//   } catch (Exception e) {
//       throw new RuntimeException("Error reading next record", e);
//   }
//   }

//   /**
//      * Moves to the next data page in the heap file.
//         * @throws FileIOException 
//         * @throws BufferPoolExceededException 
//            * @throws PageUnpinnedException 
//                   */
//   private void loadNextDataPage() throws IOException, InvalidPageNumberException, HashEntryNotFoundException, ReplacerException, BufferPoolExceededException, FileIOException, PageUnpinnedException {
//         while (currentDataPageId != null && currentDataPageId.pid != INVALID_PAGE) {
//             Page nextPage = new Page();
//             SystemDefs.JavabaseBM.pinPage(currentDataPageId, nextPage, false);
//             currentDataPage = new HFPage(nextPage);
//             currentRID = currentDataPage.firstRecord();
//             if (currentRID != null) {
//                 return;
//             }
//             currentDataPageId = currentDataPage.getNextPage();
//             SystemDefs.JavabaseBM.unpinPage(currentDataPageId, false);
//         }
//         currentDataPage = null;
//         currentRID = null;
//     }
// } // public class HeapScan implements GlobalConst
package heap;

import java.util.*;
import global.* ;
import chainexception.ChainException;

/**
 * A HeapScan object is created only through the function openScan() in the
 * HeapFile class. It supports the getNext interface which will simply retrieve
 * the next record in the file.
 */
public class HeapScan implements GlobalConst {

  HFPage currHF;  //current HFPage in the HeapFile
  RID currRid;  //current record's rid
  Iterator<PageId> pageIdIt;  //iterator through HeapFile's pageIdList

  /**
   * Constructs a file scan by pinning the directoy header page and initializing
   * iterator fields.
   */
  protected HeapScan(HeapFile hf) {

    //initialize iterator
    pageIdIt = hf.pages.iterator();

    //initialize currHF to be the header page (first page in heapfile)
    Page newPage = new Page();
    PageId firstPageno = pageIdIt.next();
    Minibase.BufferManager.pinPage(firstPageno, newPage, false);  //Iterator.next() is the first item
    currHF = new HFPage(newPage);

    //initialize currRid to be the first record in header page
    currRid = currHF.firstRecord();

    //unpin it once done using newPage
    //Minibase.BufferManager.unpinPage(firstPageno, false);  //unpin firstPageno
  } //DONE

  /**
   * Called by the garbage collector when there are no more references to the
   * object; closes the scan if it's still open.
   */
  protected void finalize() throws Throwable {
    //PUT YOUR CODE HERE
    close();
    //System.out.println("Finalized!");
  } //DONE

  /**
   * Closes the file scan, releasing any pinned pages.
   */
  public void close() throws ChainException{
    //Reset class variables
    currHF = null;
    currRid = null;
    pageIdIt = null;
  } //DONE

  /**
   * Returns true if there are more records to scan, false otherwise.
   */
  public boolean hasNext() {
    //if current page is the last page
    return pageIdIt.hasNext();
  } //TODO: more records to scan, not more pageId to scan

  /**
   * Gets the next record in the file scan.
   * 
   * @param rid output parameter that identifies the returned record
   * @throws IllegalStateException if the scan has no more elements
   */
  public Tuple getNext(RID rid) {
    //PUT YOUR CODE HERE
    //if (!this.hasNext()){
    //  throw new IllegalStateException("the scan has no more elements");
    //}
    //else{

      //If there is more record in currentHF page
      if(currRid != null)
      {
        //System.out.println("currRid is NOT NULL: page_"  + currRid.pageno.pid + " slotno_" + currRid.slotno);
        rid.copyRID(currRid); //write currRid to output param rid
        Tuple tuple = new Tuple(currHF.selectRecord(currRid), 0, currHF.selectRecord(currRid).length);
        currRid = currHF.nextRecord(currRid); //update currRid to nextRecord
        return tuple;
      }

      //If there is no more record in currentHF page
      if(currRid == null){
        //We are done with currHf
        PageId currPageId = currHF.getCurPage();
        Minibase.BufferManager.unpinPage(currPageId, false);  //unpin current pageId's page

        //if there is more page to go next to
        if(pageIdIt.hasNext()){
          PageId nextPageId = pageIdIt.next();  //get next pageId
          Minibase.BufferManager.pinPage(nextPageId, currHF, false);  //pin next pageId's page and write to currHF
          currRid = currHF.firstRecord(); //update currRid to first record in newHF

          if(currRid == null){  //if nextPage has 0 record (maybe it's the last page)
            Minibase.BufferManager.unpinPage(nextPageId, false);  //unpin next pageId's page
            return null;
          }else{  //return the first tuple in next page
            rid.copyRID(currRid); //write currRid to output param rid
            Tuple tuple = new Tuple(currHF.selectRecord(currRid), 0, currHF.selectRecord(currRid).length);
            //System.out.println("currRid is NULL & Get Next Page's firstRecord: page_"  + currRid.pageno.pid + " slotno_" + currRid.slotno);
            currRid = currHF.nextRecord(currRid); //update currRid to nextRecord
            return tuple;
          }
        }else{
          //System.out.println("currRId is NULL & last page in list! currHF is : page_"  + currHF.getCurPage().pid);
          return null;
        }
      }
      return null;
    }
  //}

} // public class HeapScan implements GlobalConst