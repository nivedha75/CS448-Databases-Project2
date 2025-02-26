package bufmgr;

import diskmgr.DB;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import global.GlobalConst;
import global.Page;
import global.PageId;
import global.SystemDefs;
import chainexception.ChainException;

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
        this.db = new DB();  // If your code uses a shared DB or an openDB(...), adapt accordingly.
        try {
          db.openDB("MyTempDB", 2000);  // Make sure to open it
        } catch (Exception e) {
          e.printStackTrace();
        }

        // Allocate actual Page array & frame descriptors
        this.bufPool = new Page[numbufs];
        this.frameTable = new FrameDesc[numbufs];

        // Initialize each frame
        for (int i = 0; i < numbufs; i++) {
            bufPool[i] = new Page();  // each is a blank page object
            frameTable[i] = new FrameDesc();
            frameTable[i].pageId = new PageId(INVALID_PAGE);
            frameTable[i].pinCount = 0;
            frameTable[i].dirty = false;

            // Initially all frames are unpinned => they're free for replacement
            fifoQueue.addLast(i);
        }

        // Build the hash directory
        for (int i = 0; i < HTSIZE; i++) {
            directory[i] = new LinkedList<>();
        }

        // replacerArg could be stored or validated if needed
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
        // 1. Check if the page is already in the buffer
        int frameIndex = hashLookup(pin_pgid);
        if (frameIndex != -1) {
            // Already in memory
            FrameDesc fdesc = frameTable[frameIndex];
            if (fdesc.pinCount == 0) {
                // remove from FIFO queue
                fifoQueue.remove(Integer.valueOf(frameIndex));
            }
            fdesc.pinCount++;
            // Copy data to caller's Page. We'll assume setpage(byte[]) is the correct method:
            page.setpage(bufPool[frameIndex].getpage());
        }
        else {
            // Need to bring the page in from disk
            if (fifoQueue.isEmpty()) {
                // No free frame
                throw new BufferPoolExceededException(null,
                        "No unpinned frames available for FIFO replacement.");
            }
            // Choose the oldest unpinned frame
            int victim = fifoQueue.removeFirst();
            FrameDesc vdesc = frameTable[victim];

            // If the victim was dirty, flush it to disk
            if (vdesc.dirty && vdesc.pageId.pid != INVALID_PAGE) {
                flushPage(vdesc.pageId);
            }
            // Remove old page from hash table if it was valid
            if (vdesc.pageId.pid != INVALID_PAGE) {
                hashRemove(vdesc.pageId);
            }

            // If emptyPage == false, do a normal read
            // (Part 1 typically states "assume false", so let's read from disk.)
            try {
                db.read_page(pin_pgid, bufPool[victim]);
            } catch (Exception e) {
                // If there's an error reading from disk, we should put victim frame back
                // in FIFO queue to avoid losing a frame. Then re-throw or handle.
                fifoQueue.addFirst(victim); // revert
                throw new BufferPoolExceededException(e, "Error reading page from disk");
            }

            // Update victim's frame descriptor
            vdesc.pageId = new PageId(pin_pgid.pid);
            vdesc.pinCount = 1;
            vdesc.dirty = false;

            // Insert new mapping
            hashInsert(pin_pgid, victim);

            // Pass the page data back to the caller
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
            // Now it is a candidate for replacement
            fifoQueue.addLast(frameIndex);
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
        // DB.allocate_page requires a PageId out-parameter
        PageId startPid = new PageId();
        try {
            db.allocate_page(startPid, howmany);
        } catch (Exception e) {
            // allocation on disk failed
            return null;
        }

        // Now we have a valid startPid. Try to pin that page in memory.
        try {
            // Typically, newly allocated pages are empty => emptyPage=true
            // but the Part 1 instructions often say "assume false." 
            // We'll go with 'true' for newly allocated pages:
            pinPage(startPid, firstpage, true);
        } catch (Exception e) {
            // If we fail to pin (no free frames), deallocate the pages on disk
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
          */
         public void freePage(PageId globalPageId) throws PagePinnedException, HashEntryNotFoundException {
        int frameIndex = hashLookup(globalPageId);
        if (frameIndex == -1) {
          return;
        }
        FrameDesc fdesc = frameTable[frameIndex];
        if (fdesc.pinCount > 0) {
            throw new PagePinnedException(null,
                        "Cannot free a pinned page!");
        }
            // Remove from hash table
            hashRemove(globalPageId);

            // Mark frame as empty
            fdesc.pageId = new PageId(INVALID_PAGE);
            fdesc.dirty = false;
            fdesc.pinCount = 0;

            // The frame is now unused; put it on FIFO queue if it's not there already
            if (!fifoQueue.contains(frameIndex)) {
                fifoQueue.addLast(frameIndex);
            }
        // Also remove it from disk
        try {
            db.deallocate_page(globalPageId);
        } catch (Exception e) {
            e.printStackTrace();
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
            // Not in buffer => nothing to flush
            return;
        }
        FrameDesc fdesc = frameTable[frameIndex];
        if (fdesc == null ||  fdesc.pageId.pid == INVALID_PAGE) {
          return;
        }
        if (fdesc.dirty) {
            // Write it to disk
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
            // Copy the integer from pid
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

    // ----------------------------------------------------------------
    //   EXCEPTIONS
    // ----------------------------------------------------------------

    public static class BufferPoolExceededException extends ChainException {
        public BufferPoolExceededException(Exception e, String message) {
            super(e, message);
        }
    }

    public static class PagePinnedException extends ChainException {
        public PagePinnedException(Exception e, String message) {
            super(e, message);
        }
    }

    public static class PageUnpinnedException extends ChainException {
        public PageUnpinnedException(Exception e, String message) {
            super(e, message);
        }
    }

    public static class HashEntryNotFoundException extends ChainException {
        public HashEntryNotFoundException(Exception e, String message) {
            super(e, message);
        }
    }
}
