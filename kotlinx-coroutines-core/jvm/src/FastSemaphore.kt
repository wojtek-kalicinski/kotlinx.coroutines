package kotlinx.coroutines

import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.atomicArrayOfNulls
import kotlinx.atomicfu.getAndUpdate
import kotlinx.coroutines.internal.Segment
import kotlinx.coroutines.internal.SegmentQueue
import kotlinx.coroutines.internal.Symbol
import kotlinx.coroutines.internal.systemProp
import java.util.concurrent.locks.LockSupport

public interface FastSemaphore {
    public fun acquire()
    public fun release()
}

public fun FastSemaphore(permits: Int): FastSemaphore = FastSemaphoreImpl(permits)

private class FastSemaphoreImpl(private val permits: Int) :
        FastSemaphore, SegmentQueue<FastSemaphoreSegment>(createFirstSegmentLazily = true) {
    init {
        require(permits > 0) { "Semaphore should have at least 1 permit" }
    }

    override fun newSegment(id: Long, prev: FastSemaphoreSegment?) = FastSemaphoreSegment(id, prev)

    /**
     * This counter indicates a number of available permits if it is non-negative,
     * or the size with minus sign otherwise. Note, that 32-bit counter is enough here
     * since the maximal number of available permits is [permits] which is [Int],
     * and the maximum number of waiting acquirers cannot be greater than 2^31 in any
     * real application.
     */
    private val _availablePermits = atomic(permits)

    // The queue of waiting acquirers is essentially an infinite array based on `SegmentQueue`;
    // each segment contains a fixed number of slots. To determine a slot for each enqueue
    // and dequeue operation, we increment the corresponding counter at the beginning of the operation
    // and use the value before the increment as a slot number. This way, each enqueue-dequeue pair
    // works with an individual cell.
    private val enqIdx = atomic(0L)
    private val deqIdx = atomic(0L)

    override fun acquire() {
        val p = _availablePermits.getAndDecrement()
        if (p > 0) return // permit acquired
        addToQueueAndSuspend()
    }

    override fun release() {
        val p = _availablePermits.getAndUpdate { cur ->
            check(cur < permits) { "The number of acquired permits cannot be greater than `permits`" }
            cur + 1
        }
        if (p >= 0) return // no waiters
        resumeNextFromQueue()
    }

    private fun addToQueueAndSuspend() {
        val last = this.tail
        val enqIdx = enqIdx.getAndIncrement()
        val segment = getSegment(last, enqIdx / SEGMENT_SIZE)
        val i = (enqIdx % SEGMENT_SIZE).toInt()
        val t = Thread.currentThread()
        if (segment === null || segment.get(i) === RESUMED || !segment.cas(i, null, t)) {
            // already resumed
            return
        }
        while (segment.get(i) === t) {
            try {
                LockSupport.park()
            } catch (e: InterruptedException) {
                segment.cancel(i)
                this.release()
                t.interrupt()
                throw e
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun resumeNextFromQueue() {
        val first = this.head
        val deqIdx = deqIdx.getAndIncrement()
        val segment = getSegmentAndMoveHead(first, deqIdx / SEGMENT_SIZE) ?: return
        val i = (deqIdx % SEGMENT_SIZE).toInt()
        val t = segment.getAndUpdate(i) {
            // Cancelled continuation invokes `release`
            // and resumes next suspended acquirer if needed.
            if (it === CANCELLED) return
            RESUMED
        }
        if (t === null) return // just resumed
        LockSupport.unpark(t as Thread)
    }
}

private class FastSemaphoreSegment(id: Long, prev: FastSemaphoreSegment?) : Segment<FastSemaphoreSegment>(id, prev) {
    private val acquirers = atomicArrayOfNulls<Any?>(SEGMENT_SIZE)

    @Suppress("NOTHING_TO_INLINE")
    inline fun get(index: Int): Any? = acquirers[index].value

    @Suppress("NOTHING_TO_INLINE")
    inline fun cas(index: Int, expected: Any?, value: Any?): Boolean = acquirers[index].compareAndSet(expected, value)

    inline fun getAndUpdate(index: Int, function: (Any?) -> Any?): Any? {
        while (true) {
            val cur = acquirers[index].value
            val upd = function(cur)
            if (cas(index, cur, upd)) return cur
        }
    }

    private val cancelledSlots = atomic(0)
    override val removed get() = cancelledSlots.value == SEGMENT_SIZE

    // Cleans the acquirer slot located by the specified index
    // and removes this segment physically if all slots are cleaned.
    fun cancel(index: Int) {
        // Clean the specified waiter
        acquirers[index].value = CANCELLED
        // Remove this segment if needed
        if (cancelledSlots.incrementAndGet() == SEGMENT_SIZE)
            remove()
    }

    override fun toString() = "SemaphoreSegment[id=$id, hashCode=${hashCode()}]"
}

private val RESUMED = Symbol("RESUMED")
private val CANCELLED = Symbol("CANCELLED")
private val SEGMENT_SIZE = systemProp("kotlinx.coroutines.semaphore.segmentSize", 32)