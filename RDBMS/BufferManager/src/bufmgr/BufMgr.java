package bufmgr;

import diskmgr.DB;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import global.GlobalConst;
import global.Page;
import global.PageId;
import global.SystemDefs;
import java.io.IOException;
import java.util.LinkedList;

/**
 * A FIFO-based Buffer Manager for a simplified DB class that has
 * allocate_page() / deallocate_page() / read_page() / write_page().
 */
public class BufMgr implements GlobalConst {

    /** Number of frames in the buffer pool. */
    private final int numBufs;

    /** The array of Pages (the actual in-memory frames). */
    private final Page[] bufPool;

    /** Frame descriptors for each frame in bufPool. */
    private final FrameDesc[] frameTable;

    /** Our DB object (for read_page, write_page, allocate_page, deallocate_page). */
    private final DB db;

    /** A simple hash table (PageId -> frameIndex). */
    private static final int HTSIZE = 31;
    @SuppressWarnings("unchecked")
    private final LinkedList<HashEntry>[] directory = new LinkedList[HTSIZE];

    /** FIFO queue for frames that have pinCount=0 (replacement candidates). */
    private final LinkedList<Integer> fifoQueue = new LinkedList<>();

    // ----------------------------------------------------------------
    //   Constructor
    // ----------------------------------------------------------------

    /**
     * Create the BufMgr object. Allocate 'numbufs' buffers.
     * Use a DB instance to handle disk operations, and use FIFO policy.
     *
     * @param numbufs     number of frames
     * @param replacerArg replacement policy name (e.g., "FIFO")
     */
    public BufMgr(int numbufs, String replacerArg) {
        this.numBufs = numbufs;
        SystemDefs.JavabaseBM = this;
        this.db = new DB();
        for (int i = 0; i < HTSIZE; i++) {
            directory[i] = new LinkedList<>();
        } 
        try {
          db.openDB("MyTempDB", 2000);
        } catch (Exception e) {
          e.printStackTrace();
        }
        this.bufPool = new Page[numbufs];
        this.frameTable = new FrameDesc[numbufs];
        for (int i = 0; i < numbufs; i++) {
            bufPool[i] = new Page(); 
            frameTable[i] = new FrameDesc();
            frameTable[i].pageId = new PageId(INVALID_PAGE);
            frameTable[i].pinCount = 0;
            frameTable[i].dirty = false;
            fifoQueue.addLast(i);
        }

    }

    // ----------------------------------------------------------------
    //   PIN / UNPIN
    // ----------------------------------------------------------------

    /**
     * Pin a page. If the page is already in the pool, increment the pin_count.
     * Otherwise, pick a victim (FIFO), read it into the buffer, etc.
     *
     * @param pin_pgid  page number in the DB
     * @param page      a Page object for the caller to read/write
     * @param emptyPage if true, treat as newly allocated page (ignored in Part 1)
     */
    public void pinPage(PageId pin_pgid, Page page, boolean emptyPage)
            throws BufferPoolExceededException, HashEntryNotFoundException, InvalidPageNumberException, FileIOException, IOException
    {
        int frameIndex = hashLookup(pin_pgid);
        if (frameIndex != -1) {
            FrameDesc fdesc = frameTable[frameIndex];
            if (fdesc.pinCount == 0) {
                fifoQueue.remove(Integer.valueOf(frameIndex));
            }
            fdesc.pinCount++;
            page.setpage(bufPool[frameIndex].getpage());
        }
        else {
            if (fifoQueue.isEmpty()) {
                throw new BufferPoolExceededException(null,
                        "No unpinned frames available for FIFO replacement.");
            }
            int victim = fifoQueue.removeFirst();
            FrameDesc vdesc = frameTable[victim];

            if (vdesc.dirty && vdesc.pageId.pid != INVALID_PAGE) {
                flushPage(vdesc.pageId);
            }
            if (vdesc.pageId.pid != INVALID_PAGE) {
                hashRemove(vdesc.pageId);
            }

            try {
                db.read_page(pin_pgid, bufPool[victim]);
            } catch (Exception e) {
                fifoQueue.addFirst(victim); 
                throw new BufferPoolExceededException(e, "Error reading page from disk");
            }
            vdesc.pageId = new PageId(pin_pgid.pid);
            vdesc.pinCount = 1;
            vdesc.dirty = false;
            hashInsert(pin_pgid, victim);
            page.setpage(bufPool[victim].getpage());
        }
    }

    /**
     * Unpin a page in the buffer (decrement pin_count). If pin_count becomes 0,
     * the frame is placed on FIFO queue as a replacement candidate.
     *
     * @param PageId_in_a_DB the page ID
     * @param dirty          whether the page was modified
     */
    public void unpinPage(PageId PageId_in_a_DB, boolean dirty)
            throws PageUnpinnedException, HashEntryNotFoundException
    {
        int frameIndex = hashLookup(PageId_in_a_DB);
        if (frameIndex == -1) {
            throw new HashEntryNotFoundException(null,
                    "Tried to unpin a page not present in the buffer pool!");
        }
        FrameDesc fdesc = frameTable[frameIndex];
        if (fdesc.pinCount <= 0) {
            throw new PageUnpinnedException(null,
                    "Attempting to unpin a page that has pin_count=0!");
        }

        fdesc.pinCount--;
        if (dirty) {
            fdesc.dirty = true;
        }
        if (fdesc.pinCount == 0) {
            if (!fifoQueue.contains(frameIndex)) {
                fifoQueue.addLast(frameIndex);
            }
        }
    }

    // ----------------------------------------------------------------
    //   ALLOCATE / FREE
    // ----------------------------------------------------------------

    /**
     * Allocate a run of new pages, pin the first page, and return its PageId.
     * If we cannot pin the first page (no free frame), deallocate and return null.
     *
     * @param firstpage Page object that will refer to the pinned first page
     * @param howmany   number of pages to allocate
     * @return the first page id (or null if error)
     */
    public PageId newPage(Page firstpage, int howmany) {
        PageId startPid = new PageId();
        try {
            db.allocate_page(startPid, howmany);
        } catch (Exception e) {
            return null;
        }
        try {
            pinPage(startPid, firstpage, true);
        } catch (Exception e) {
            try {
                db.deallocate_page(startPid, howmany);
            } catch (Exception ignored) { }
            return null;
        }
        return startPid;
    }

    /**
     * Free a single page, removing it from the buffer pool if present.
     * Throw PagePinnedException if it’s still pinned.
     *
     * @param globalPageId the page ID to free
          * @throws HashEntryNotFoundException 
               * @throws PageUnpinnedException 
                    */
    public void freePage(PageId globalPageId) throws PagePinnedException, HashEntryNotFoundException, PageUnpinnedException {
        int frameIndex = hashLookup(globalPageId);
        if (frameIndex == -1) {
          return;
        }
        FrameDesc fdesc = frameTable[frameIndex];
        if (fdesc.pinCount > 1) {  
            throw new PagePinnedException(null, "Cannot free a doubly-pinned page.");
        }
        if (fdesc.pinCount > 0) {
            try {
                unpinPage(globalPageId, false);
            } catch (PageUnpinnedException e) {
                throw new PagePinnedException(e, "Error unpinning page before freeing.");
            }
        }

        try {
            db.deallocate_page(globalPageId);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        hashRemove(globalPageId);
        fdesc.pageId = new PageId(INVALID_PAGE);
        fdesc.dirty = false;
        fdesc.pinCount = 0;
        if (!fifoQueue.contains(frameIndex)) {
            fifoQueue.addLast(frameIndex);
        }
    }

    // ----------------------------------------------------------------
    //   FLUSH
    // ----------------------------------------------------------------

    /**
     * Flush a particular page to disk if it’s dirty.
     *
     * @param pageid page to flush
     * @throws InvalidPageNumberException if the page is not in the buffer pool
     * @throws FileIOException           if there's an error writing to disk
     * @throws IOException               if there's an error writing to disk
     */
    public void flushPage(PageId pageid) throws InvalidPageNumberException, FileIOException, IOException {
        if (pageid == null) return;
        int frameIndex = hashLookup(pageid);
        if (frameIndex == -1) {
            return;
        }
        FrameDesc fdesc = frameTable[frameIndex];
        if (fdesc == null ||  fdesc.pageId.pid == INVALID_PAGE) {
          return;
        }
        if (fdesc.dirty) {
            db.write_page(fdesc.pageId, bufPool[frameIndex]);
            fdesc.dirty = false;
        }
    }

    /**
     * Flush all dirty pages to disk.
     */
    public void flushAllPages() throws InvalidPageNumberException, FileIOException, IOException {
        for (int i = 0; i < numBufs; i++) {
            PageId pid = frameTable[i].pageId;
            if (pid.pid != INVALID_PAGE && frameTable[i].dirty) {
                flushPage(pid);
            }
        }
    }

    // ----------------------------------------------------------------
    //   STATS
    // ----------------------------------------------------------------

    /**
     * @return total number of frames
     */
    public int getNumBuffers() {
        return numBufs;
    }

    /**
     * @return how many frames have pinCount == 0
     */
    public int getNumUnpinnedBuffers() {
        int count = 0;
        for (FrameDesc fd : frameTable) {
            if (fd.pinCount == 0) {
                count++;
            }
        }
        return count;
    }

    // ----------------------------------------------------------------
    //   INTERNAL DATA STRUCTURES
    // ----------------------------------------------------------------

    /** Metadata for each frame in the buffer pool. */
    private static class FrameDesc {
        PageId pageId;
        int pinCount;
        boolean dirty;
    }

    /** Simple structure for hash bucket entries: (PageId -> frameIndex). */
    private static class HashEntry {
        PageId pid;
        int frameIndex;

        HashEntry(PageId pid, int idx) {
            this.pid = new PageId(pid.pid);
            this.frameIndex = idx;
        }
    }

    // ----------------------------------------------------------------
    //   HASH TABLE IMPLEMENTATION
    // ----------------------------------------------------------------

    /** A simple mod-based hash for PageId. */
    private int hash(PageId pid) {
        return (pid.pid & 0x7fffffff) % HTSIZE;
    }

    /** Insert (pageId -> frameIndex) mapping. */
    private void hashInsert(PageId pid, int frameIndex) {
        int bucket = hash(pid);
        directory[bucket].add(new HashEntry(pid, frameIndex));
    }

    /** Lookup the frame index for a given pageId. Return -1 if not found. */
    private int hashLookup(PageId pid) {
        int bucket = hash(pid);
        if (directory[bucket] == null) {
            throw new RuntimeException("Error: directory[" + bucket + "] is null!");
        }
        for (HashEntry entry : directory[bucket]) {
            if (entry.pid.pid == pid.pid) {
                return entry.frameIndex;
            }
        }
        return -1;
    }

    /** Remove a (pageId -> frameIndex) if it exists. */
    private void hashRemove(PageId pid) {
        int bucket = hash(pid);
        HashEntry found = null;
        for (HashEntry entry : directory[bucket]) {
            if (entry.pid.pid == pid.pid) {
                found = entry;
                break;
            }
        }
        if (found != null) {
            directory[bucket].remove(found);
        }
    }

}
