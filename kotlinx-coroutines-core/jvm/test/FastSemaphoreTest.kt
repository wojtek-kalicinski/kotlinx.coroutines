package kotlinx.coroutines

import org.junit.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

class FastSemaphoreTest {

    @Test
    fun testSimple() {
        val s = FastSemaphore(2)
        s.acquire()
        s.acquire()
        s.release()
        s.acquire()
    }

    @Test
    fun testStressAsMutex() {
        val s = FastSemaphore(1)
        val t = 10
        val n = 100_000
        var i = 0
        val done = AtomicInteger()
        repeat(t) {
            thread {
                repeat(n) {
                    s.acquire()
                    i++
                    s.release()
                }
                done.incrementAndGet()
            }
        }
        while (done.get() != t) {}
        check(i == n * t)
    }
}