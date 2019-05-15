package benchmarks

import kotlinx.coroutines.FastSemaphore
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import java.util.concurrent.Phaser
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

@Warmup(iterations = 3, time = 500, timeUnit = TimeUnit.MICROSECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MICROSECONDS)
@Fork(value = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
open class SemaphoreOnThreadsBenchmark {
    @Param("1", "2", "4", "8", "32", "64", "100000")
    private var _1_maxPermits: Int = 0

//    @Param("1", "2", "4") // local machine
//    @Param("1", "2", "4", "8", "12") // local machine
    @Param("1", "2", "4", "8", "16", "32", "64", "128", "144") // dasquad
//    @Param("1", "2", "4", "8", "16", "32", "64", "96") // Google Cloud
    private var _2_parallelism: Int = 0

    @Benchmark
    fun javaSemaphoreFair() {
        val s = java.util.concurrent.Semaphore(_1_maxPermits, true)
        val t = _2_parallelism
        val p = Phaser(t)
        val n = BATCH_SIZE / t
        repeat(t) {
            thread {
                repeat(n) {
                    s.acquire()
                    doWork(WORK_INSIDE)
                    s.release()
                    doWork(WORK_OUTSIDE)
                }
                p.arrive()
            }
        }
        p.awaitAdvance(0)
    }

    @Benchmark
    fun javaSemaphoreUnfair() {
        val s = java.util.concurrent.Semaphore(_1_maxPermits, false)
        val t = _2_parallelism
        val p = Phaser(t)
        val n = BATCH_SIZE / t
        repeat(t) {
            thread {
                repeat(n) {
                    s.acquire()
                    doWork(WORK_INSIDE)
                    s.release()
                    doWork(WORK_OUTSIDE)
                }
                p.arrive()
            }
        }
        p.awaitAdvance(0)
    }

    @Benchmark
    fun fastSemaphore() {
        val s = FastSemaphore(_1_maxPermits)
        val t = _2_parallelism
        val p = Phaser(t)
        val n = BATCH_SIZE / t
        repeat(t) {
            thread {
                repeat(n) {
                    s.acquire()
                    doWork(WORK_INSIDE)
                    s.release()
                    doWork(WORK_OUTSIDE)
                }
                p.arrive()
            }
        }
        p.awaitAdvance(0)
    }
}


private fun doWork(work: Int) {
    // We use geometric distribution here
    Blackhole.consumeCPU(work.toLong())
//    val p = 1.0 / work
//    val r = ThreadLocalRandom.current()
//    while (true) {
//        if (r.nextDouble() < p) break
//    }
}

private const val WORK_INSIDE = 80
private const val WORK_OUTSIDE = 40
private const val BATCH_SIZE = 100000