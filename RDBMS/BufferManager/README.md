Group Members:
Ram Laxminarayan
Paraj Goyal
Nivedha Kumar

Features to know/roles: In our group, for the BufMgr class, Nivedha worked on instantiating the BufMgr object with the appropriate number of buffers, and the functions that unpinned and pinned pages in the buffer (pinPage, unPinPage). Ram wrote the functions that created and allocated new pages and pinned them to an open buffer pool (newPage), freed pages from the disk (freePage), and flushed pages of the buffer pool to the disk (flushPage, flushAllPages). Paraj worked on the hash table that mapped page numbers to frame numbers and the frame desc implementations which keeps track of the page number, pin count, and dirty bit, and also the getter methods (getNumBuffers, getNumUnpinnedBuffers).
