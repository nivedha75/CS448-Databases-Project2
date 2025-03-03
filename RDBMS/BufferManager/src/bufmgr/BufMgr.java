/* ... */

package bufmgr;

import java.io.*;
import java.util.*;
import global.*;
import chainexception.ChainException;

public class BufMgr implements GlobalConst{

    private final Page[] buffers;
    private final LinkedList<Integer> fifoQueue; 
    private final FrameDesc[] fd;
    private final HashTable frame; 

  /**
   * Create the BufMgr object.
   * Allocate pages (frames) for the buffer pool in main memory and
   * make the buffer manage aware that the replacement policy is
   * specified by replacerArg.
   *
   * @param numbufs number of buffers in the buffer pool.
   * @param replacerArg name of the buffer replacement policy.
   */

  public BufMgr(int numbufs, String replacerArg) {
    //YOUR CODE HERE
    this.buffers = new Page[numbufs];
    this.fifoQueue = new LinkedList<>();
    this.fd = new FrameDesc[numbufs];
    this.frame = new HashTable(numbufs);
    replacerArg = "FIFO";
    for (int i = 0; i < numbufs; i++) {
      buffers[i] = new Page();
      fifoQueue.addLast(i);
      fd[i] = new FrameDesc();
    }
  }


  /**
   * Pin a page.
   * First check if this page is already in the buffer pool.
   * If it is, increment the pin_count and return a pointer to this
   * page.  If the pin_count was 0 before the call, the page was a
   * replacement candidate, but is no longer a candidate.
   * If the page is not in the pool, choose a frame (from the
   * set of replacement candidates) to hold this page, read the
   * page (using the appropriate method from {diskmgr} package) and pin it.
   * Also, must write out the old page in chosen frame if it is dirty
   * before reading new page.  (You can assume that emptyPage==false for
   * this assignment.)
   *
   * @param Page_Id_in_a_DB page number in the minibase.
   * @param page the pointer poit to the page.
   * @param emptyPage true (empty page); false (non-empty page)
   */

  public void pinPage(PageId pin_pgid, Page page, boolean emptyPage) throws IOException, ChainException {
    //YOUR CODE HERE
    Integer ind = frame.hashGet(pin_pgid);
    if (frame.hashGet(pin_pgid) == null) {
        ind = allocate(pin_pgid);  
    } else {
        fd[ind].pinCount = fd[ind].pinCount + 1;
        if (fd[ind].pinCount == 1) {
          fifoQueue.remove((Integer) ind);
        }
    }
    page.setpage(buffers[ind].getpage());
  }

  private void evict() throws IOException, ChainException {
    int replacement = 0;
    if (fifoQueue.isEmpty() == true) {
      throw new BufferPoolExceededException(new Exception(), "The buffer pool is full.");
    }
    if (replacement == 0) {
        replacement = fifoQueue.removeFirst();
    }
    PageId pid = new PageId(fd[replacement].pageNumber);
    if (fd[replacement].dirtyBit == true) {
      flushPage(pid);
    }
    buffers[replacement] = new Page();
    fd[replacement] = new FrameDesc();
    frame.hashRemove(pid);
  }

  private int allocate(PageId pid) throws IOException, ChainException {
    Integer ind = 0;
    if (fifoQueue.isEmpty() == true) {
        evict();
    }
    if (ind == 0) {
        ind = fifoQueue.pollFirst();
    }
    if (ind == null) {
        throw new BufferPoolExceededException(new Exception(), "The buffer pool is full.");
    }
    PageId old = new PageId(fd[ind].pageNumber);
    if (old.pid != -1) {
        writeIfDirty(ind);
        frame.hashRemove(old);
    }
    SystemDefs.JavabaseDB.read_page(pid, buffers[ind]);
    frame.hashInsert(new PageId(pid.pid), ind); 
    fd[ind] = new FrameDesc(pid.pid, 1, false);
    return ind;
}

private void writeIfDirty(int fInd) throws IOException, ChainException {
    if (fd[fInd].dirtyBit == false) {
        return;
    }
    fd[fInd].dirtyBit = false;
    PageId pid = new PageId(fd[fInd].pageNumber);
    SystemDefs.JavabaseDB.write_page(pid, buffers[fInd]);
}

  /**
   * Unpin a page specified by a pageId.
   * This method should be called with dirty==true if the client has
   * modified the page.  If so, this call should set the dirty bit
   * for this frame.  Further, if pin_count>0, this method should
   * decrement it. If pin_count=0 before this call, throw an exception
   * to report error.  (For testing purposes, we ask you to throw
   * an exception named PageUnpinnedException in case of error.)
   *
   * @param globalPageId_in_a_DB page number in the minibase.
   * @param dirty the dirty bit of the frame
   */

  public void unpinPage(PageId PageId_in_a_DB, boolean dirty) throws ChainException{
    //YOUR CODE HERE
    Integer ind = frame.hashGet(PageId_in_a_DB);
    if (frame.hashGet(PageId_in_a_DB) == null) {
        throw new HashEntryNotFoundException(new Exception(), "The page entry was not found.");
    } else {
        ind = frame.hashGet(PageId_in_a_DB);
    }
    if (fd[ind].pinCount <= 0) {
        throw new PageUnpinnedException(new Exception(), "No pages were pinned.");
    }
    fd[ind].pinCount = fd[ind].pinCount - 1;
    if (fifoQueue.contains(ind) == false) {
        if (fd[ind].pinCount == 0) {
            fifoQueue.addFirst(ind);
        }
    }
    if (dirty == true) {
        fd[ind].dirtyBit = true;
    }
  }

  /**
   * Allocate new pages.
   * Call DB object to allocate a run of new pages and
   * find a frame in the buffer pool for the first page
   * and pin it. (This call allows a client of the Buffer Manager
   * to allocate pages on disk.) If buffer is full, i.e., you
   * can't find a frame for the first page, ask DB to deallocate
   * all these pages, and return null.
   *
   * @param firstpage the address of the first page.
   * @param howmany total number of allocated new pages.
   *
   * @return the first page id of the new pages.  null, if error.
   */

  public PageId newPage(Page firstpage, int howmany) throws BufferPoolExceededException, IOException, ChainException {
    //YOUR CODE HERE
    if (getNumUnpinnedBuffers() == 0 || getNumUnpinnedBuffers() < 0) {
        throw new BufferPoolExceededException(new Exception(), "No unpinned buffers.");
    } 
    PageId pid = new PageId();
    SystemDefs.JavabaseDB.allocate_page(pid, howmany);
    pinPage(pid, firstpage, false);
    return pid; 
  }

  /**
   * This method should be called to delete a page that is on disk.
   * This routine must call the method in diskmgr package to
   * deallocate the page.
   *
   * @param globalPageId the page number in the data base.
   */

  public void freePage(PageId globalPageId) throws IOException, ChainException{
    //YOUR CODE HERE
    boolean pin = true;
    boolean deallocate = true;
    Integer ind = frame.hashGet(globalPageId);
    if (ind != null) {
        if (fd[ind].pinCount > 0) {
            pin = true;
        }
    }
    else {
        pin = false;
    }
    if (frame.hashGet(globalPageId) == null) {
      SystemDefs.JavabaseDB.deallocate_page(globalPageId);
      return;
    }
    if (fd[ind].pinCount > 1 && pin == true) {
      throw new PagePinnedException(new Exception(), "Cannot free a pinned page.");
    }
    else if (fd[ind].pinCount == 1) {
      unpinPage(globalPageId, fd[ind].dirtyBit);
    }
    if (fd[ind].dirtyBit == true) {
      flushPage(globalPageId);
    }
    buffers[ind] = new Page();
    fifoQueue.add((Integer) ind);
    fd[ind] = new FrameDesc();
    frame.hashRemove(globalPageId);
    if (deallocate == true) {
        SystemDefs.JavabaseDB.deallocate_page(globalPageId);
    }
  }


  /**
   * Used to flush a particular page of the buffer pool to disk.
   * This method calls the write_page method of the diskmgr package.
   *
   * @param pageid the page number in the database.
   */

  public void flushPage(PageId pageid) throws IOException, ChainException {
    //YOUR CODE HERE
    Integer ind = frame.hashGet(pageid);
    if (frame.hashGet(pageid) != null) {
        SystemDefs.JavabaseDB.write_page(pageid, buffers[ind]);
        fd[ind].dirtyBit = false;
        return;
    }
    return;
  }

  /** Flushes all pages of the buffer pool to disk
   */

  public void flushAllPages() throws IOException, ChainException{
      //YOUR CODE HERE
      int i = 0;
      for (FrameDesc j : fd) {
        if (j.dirtyBit == true) {
            flushPage(new PageId(j.pageNumber));
            i = i + 1;
        }
        else {
            i = i + 1;
        }
      }
  }


  /** Gets the total number of buffers.
   *
   * @return total number of buffer frames.
   */

  public int getNumBuffers() {
      //YOUR CODE HERE
      return buffers.length;
  }

  /** Gets the total number of unpinned buffer frames.
   *
   * @return total number of unpinned buffer frames.
   */

  public int getNumUnpinnedBuffers() {
    //YOUR CODE HERE
    int i = 0;
    int num = 0;
    while (fd.length > i) {
        if (fd[i].pinCount == 0) {
            num = num + 1;
            i = i + 1;
        }
        else {
            i = i + 1;
        }
    }
    return num;
  }

}

class FrameDesc {
    int pageNumber;
    int pinCount;
    boolean dirtyBit;
    public FrameDesc() {
        pageNumber = new PageId().pid;
        pinCount = 0;
        dirtyBit = false;
    }
    public FrameDesc(int pageNumber, int pinCount, boolean dirtyBit) {
        this.pageNumber = pageNumber;
        this.pinCount = pinCount;
        this.dirtyBit = dirtyBit;
    }
}

class HashEntry {
    public PageId pageNumber;
    public int frameNumber;
    public HashEntry(PageId pageNumber, int frameNumber) {
        this.pageNumber = pageNumber;
        this.frameNumber = frameNumber;
    }
}

class HashTable {
    private LinkedList<HashEntry>[] directory;
    private int limit;
    @SuppressWarnings("unchecked")
    public HashTable(int elements) {
        this.directory = new LinkedList[elements];
        this.limit = elements;
        for (int i = 0; i < elements; i++) {
            directory[i] = new LinkedList<HashEntry>();
        }
    }
    private int hash(PageId value) {
        final int a = 31;
        final int b = 17;
        return Math.abs((a * value.pid + b) % limit);
    }
    public Integer hashGet(PageId k) {
        int ind = hash(k);
        Iterator<HashEntry> iter = directory[ind].iterator();
        while (iter.hasNext()) {
            HashEntry entry = iter.next();
            if (k.pid == entry.pageNumber.pid) {
                return entry.frameNumber;
            }
        }
        return null;
    }
    public void hashInsert(PageId k, int v) {
        int ind = hash(k);
        LinkedList<HashEntry> entries = directory[ind];
        for (int i = entries.size() - 1; i >= 0; i--) {
            if (k.pid == entries.get(i).pageNumber.pid) {
                entries.get(i).frameNumber = v;
                return;
            }
        }
        entries.addFirst(new HashEntry(k, v));
    }
    public void hashRemove(PageId k) {
        int ind = hash(k);
        directory[ind].removeIf(i -> i.pageNumber.pid == k.pid);
    }
}
