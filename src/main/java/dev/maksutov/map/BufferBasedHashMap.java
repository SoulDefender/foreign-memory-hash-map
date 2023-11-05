package dev.maksutov.map;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;


//TODO Do we need put for this map?
public class BufferBasedHashMap<T extends BufferedKey> {

    private static final int ANCILLARY_INFO_SIZE = 16;
    private static final int PREVIOUS_LINK_OFFSET = 4;
    private static final int NEXT_LINK_MASK = 0xFFFFFF00;
    private static final int LOCKED_FLAG = 0x00000001;
    private static final int FREE_FLAG = 0x00000002;
    private static final int NEXT_LINK_FLAG = 0x00000004;
    private static final int PREVIOUS_LINK_FLAG = 0x00000008;
    private static final int LAST_ACCESS_TIME_OFFSET = 8;
    private final int mapSize;
    private final long timeToLive;

    private final AtomicBuffer buffer;
    private final MutableDirectBuffer keyReadWriteBuffer = new UnsafeBuffer();
    private final int keySize;
    private final int chunkSize;
    private final AtomicInteger size = new AtomicInteger(0);
    private final Int2IntHashMap bucketAddressingMap;
    private final Int2IntHashMap offsetToBucketMap;

    public BufferBasedHashMap(int keySize, int chunkSize, int mapSize, long timeToLive) {

        this.keySize = keySize;
        this.chunkSize = chunkSize;
        this.mapSize = mapSize;
        this.timeToLive = timeToLive;
        buffer = new UnsafeBuffer(ByteBuffer.allocate((ANCILLARY_INFO_SIZE + keySize + chunkSize) * mapSize));
        bucketAddressingMap = new Int2IntHashMap(mapSize,
                0.8f, Integer.MIN_VALUE, true);
        offsetToBucketMap = new Int2IntHashMap(mapSize,
                0.8f, Integer.MIN_VALUE, true);
    }

    public void get(T key, DirectBuffer chunkDataBuffer) {
        //TODO Do we need to throw exception here when the size limit is reached?
        int bucket = getBucket(key.hashCode());
        int offset;
        if (bucketAddressingMap.containsKey(bucket)) {
            offset = getOrAllocateNewBucket(key);
        } else {
            offset = allocateNewBucket(key);
            bucketAddressingMap.put(bucket, offset);
            offsetToBucketMap.put(offset, bucket);
        }
        chunkDataBuffer.wrap(buffer, getChunkDataOffset(offset), chunkSize);
    }

    private int getOrAllocateNewBucket(T key) {

        int bucket = getBucket(key.hashCode());
        int offset = bucketAddressingMap.get(bucket);
        int nextOffset = offset;

        while (nextOffset >= 0) {
            if (key.equals(getKeyInfo(offset))) {
                return offset;
            }
            nextOffset = getNextLink(offset);
            if (nextOffset > 0) {
                offset = nextOffset;
            }
        }

        if (offset >= 0) {
            nextOffset = allocateNewBucket(key);
            setNextLink(offset, nextOffset);
            setPreviousLink(nextOffset, offset);
            offset = nextOffset;
        }

        return offset;
    }

    private int allocateNewBucket(T key) {

        int bucketOffset = findBucket(key);
        allocate(bucketOffset, key);
        return bucketOffset;
    }

    public void remove(T key) {

        if (bucketAddressingMap.containsKey(getBucket(key.hashCode()))) {
            int offset = getOffset(key);
            remove(key, offset);
        }
    }

    private void remove(T key, int offset) {

        while (offset >= 0) {
            if (key.equals(getKeyInfo(offset))) {
                remove(offset);
                break;
            }
            offset = getNextLink(offset);
        }
    }

    private void remove(int offset) {

        if (isPrevLinkFlagSet(offset)) {
            int prevLink = getPreviousLink(offset);
            if (isNextLinkFlagSet(offset)) {
                int nextLink = getNextLink(offset);
                setNextLink(prevLink, nextLink);
                setPreviousLink(nextLink, prevLink);
            } else {
                removeNextLink(prevLink);
            }
        } else {
            int bucket = offsetToBucketMap.get(offset);
            if (isNextLinkFlagSet(offset)) {
                int nextLink = getNextLink(offset);
                bucketAddressingMap.put(bucket, nextLink);
                offsetToBucketMap.put(nextLink, bucket);
            } else {
                bucketAddressingMap.remove(bucket);
            }
        }
        offsetToBucketMap.remove(offset);
        removeNextLink(offset);
        removePreviousLink(offset);
        freeBucket(offset);
        size.decrementAndGet();
    }


    public void removeOutdatedRecords() {

        long currentTime = System.nanoTime();
        int totalChunkSize = totalChunkSize();
        int totalSize = totalChunkSize * mapSize;
        int offset = 0;
        do {
            if (isChunkOutdated(offset, currentTime)) {
                remove(offset);
            }
            offset += totalChunkSize;
        } while (offset <= totalSize);
    }


    private boolean isChunkOutdated(int offset, long currentTime) {

        long lastAccessTime = buffer.getLong(offset + LAST_ACCESS_TIME_OFFSET);
        return (currentTime - lastAccessTime) >= timeToLive;
    }


    //TODO Throw exception?
    public boolean isLocked(T key) {

        if (bucketAddressingMap.containsKey(getBucket(key.hashCode()))) {
            int offset = getOffset(key);
            if (offset > 0) {
                return isLocked(offset);
            }
        }
        return false;
    }

    private int getOffset(T key) {
        //TODO avoid duplication, maybe add separate put method?
        int offset = bucketAddressingMap.get(getBucket(key.hashCode()));
        while (isNextLinkFlagSet(offset)) {
            if (key.equals(getKeyInfo(offset))) {
                break;
            }
            offset = getNextLink(offset);
        }
        return offset;
    }

    public void unlock(T key) {
        if (bucketAddressingMap.containsKey(getBucket(key.hashCode()))) {
            int offset = getOffset(key);
            if (offset >= 0) {
                unlock(offset);
            }
        }
    }


    /**
     * Can lock only existing entry
     *
     * @param key of the map
     */
    public void lock(T key) {
        if (bucketAddressingMap.containsKey(getBucket(key.hashCode()))) {
            int offset = getOffset(key);
            if (offset >= 0) {
                lock(offset);
            }
        }
    }

    /**
     * The function will place a flow in the flow buffer (using flow buffer as a hash table)
     * Function determines the offset for the new flow based on the flow key hash.
     * If hash collision occurs the Linear Probing technique is used https://en.wikipedia.org/wiki/Linear_probing
     *
     * @param offset starting offset
     * @param key    of the map
     */
    private void allocate(int offset, T key) {

        int ancillaryInfo = buffer.getInt(offset);
        buffer.putInt(offset, ancillaryInfo | FREE_FLAG);
        key.write(getKeyInfo(offset));
        setLastAccessTime(offset);
        size.incrementAndGet();
    }


    /**
     * Making the method protected
     * to allow introduction of other bucket finding approach (a.k.a Template Method)
     *
     * @param key of the map
     * @return bucket offset
     */
    protected int findBucket(T key) {

        int hashCode = key.hashCode();
        int bucketOffset = -1;
        int lookupStep = 0;
        int totalChunkSize = totalChunkSize();
        int totalMapSize = totalChunkSize * mapSize;
        int offset = getBucket(hashCode) * totalChunkSize;
        do {
            if (isBucketFree(offset)) {
                bucketOffset = offset;
                break;
            }
            lookupStep++;
            offset += totalChunkSize;
            // TODO Reconsider this approach
            if (offset >= totalMapSize) {
                offset = 0;
            }
        } while (lookupStep <= mapSize);

        return bucketOffset;
    }


    public int size() {

        return size.intValue();
    }

    private int totalChunkSize() {

        return ANCILLARY_INFO_SIZE + keySize + chunkSize;
    }

    private int getChunkDataOffset(int offset) {

        return offset + ANCILLARY_INFO_SIZE + keySize;
    }

    private MutableDirectBuffer getKeyInfo(int bufferOffset) {

        keyReadWriteBuffer.wrap(buffer, bufferOffset + ANCILLARY_INFO_SIZE, keySize);
        return keyReadWriteBuffer;
    }

    private boolean isBucketFree(int offset) {

        int ancillaryFlags = buffer.getInt(offset);
        return !((ancillaryFlags & FREE_FLAG) > 0);
    }

    private void freeBucket(int offset) {

        int ancillaryFlags = buffer.getInt(offset);
        buffer.putInt(offset, ancillaryFlags & ~FREE_FLAG);
    }

    private void unlock(int offset) {

        int ancillaryFlags = buffer.getInt(offset);
        buffer.putInt(offset, ancillaryFlags & ~LOCKED_FLAG);
    }

    private void lock(int offset) {

        int ancillaryFlags = buffer.getInt(offset);
        buffer.putInt(offset, ancillaryFlags | LOCKED_FLAG);
    }

    private boolean isLocked(int offset) {

        int ancillaryFlags = buffer.getInt(offset);
        return (ancillaryFlags & LOCKED_FLAG) > 0;
    }

    private boolean isNextLinkFlagSet(int offset) {

        int ancillaryFlags = buffer.getInt(offset);
        return (ancillaryFlags & NEXT_LINK_FLAG) > 0;
    }

    private boolean isPrevLinkFlagSet(int offset) {

        int ancillaryFlags = buffer.getInt(offset);
        return (ancillaryFlags & PREVIOUS_LINK_FLAG) > 0;
    }

    private void removePreviousLink(int offset) {

        int ancillaryFlags = buffer.getInt(offset);
        buffer.putInt(offset, ancillaryFlags & ~PREVIOUS_LINK_FLAG);
    }

    private void removeNextLink(int offset) {

        int ancillaryFlags = buffer.getInt(offset);
        buffer.putInt(offset, ancillaryFlags & ~NEXT_LINK_FLAG);
    }

    int getBucket(int keyHash) {

        return Math.abs(keyHash) % mapSize;
    }

    private void setPreviousLink(int offset, int previousOffset) {

        int ancillaryFlags = buffer.getInt(offset);
        buffer.putInt(offset + PREVIOUS_LINK_OFFSET, previousOffset);
        buffer.putInt(offset, ancillaryFlags | PREVIOUS_LINK_FLAG);
    }

    private void setNextLink(int offset, int nextOffset) {

        int ancillaryInfo = buffer.getInt(offset);
        ancillaryInfo = ancillaryInfo & ~NEXT_LINK_MASK;
        buffer.putInt(offset, ancillaryInfo | (nextOffset << 4) | NEXT_LINK_FLAG);
    }

    private int getNextLink(int offset) {

        if (isNextLinkFlagSet(offset)) {
            return (buffer.getInt(offset) & NEXT_LINK_MASK) >> 4;
        } else {
            return -1;
        }
    }

    private int getPreviousLink(int offset) {

        return buffer.getInt(offset + PREVIOUS_LINK_OFFSET);
    }

    private void setLastAccessTime(int offset) {

        buffer.putLong(offset + LAST_ACCESS_TIME_OFFSET, System.nanoTime());
    }
}
