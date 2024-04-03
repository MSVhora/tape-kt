// Copyright 2024, Murtuza Vhora<murtazavhora@gmail.com>
package io.github.msvhora.tape

/**
 * Created by Murtuza Vhora(@msvhora) on 02,April,2024
 */

import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.test.runTest
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.util.logging.Logger
import kotlin.random.Random

/**
 * Coroutine Tests for QueueFile.
 *
 * @author Murtuza Vhora (murtazavhora@gmail.com)
 */
@RunWith(Parameterized::class)
class QueueFileCoroutineTest(
    name: String,
    private val forceLegacy: Boolean,
    private val headerLength: Int,
) {
    @JvmField
    @Rule
    var folder = TemporaryFolder()
    private var file: File? = null

    @Throws(IOException::class)
    private fun newQueueFile(): QueueFile {
        return newQueueFile(true)
    }

    @Throws(IOException::class)
    private fun newQueueFile(raf: RandomAccessFile): QueueFile {
        return QueueFile(file!!, raf, true, forceLegacy)
    }

    @Throws(IOException::class)
    private fun newQueueFile(zero: Boolean): QueueFile {
        return QueueFile.Builder(file).zero(zero).forceLegacy(forceLegacy).build()
    }

    @Before
    @Throws(Exception::class)
    fun setUp() {
        val parent = folder.getRoot()
        file = File(parent, "queue-file")
    }

    @Test
    @Throws(IOException::class)
    fun testConcurrentAddElements() = runTest {
        // This test ensures that we update 'first' correctly.
        val queue: QueueFile = newQueueFile()
        val max = 100
        (0..max).map {
            async {
                queue.add(values[it])
            }
        }.awaitAll()
        assertThat(queue.peek()).isEqualTo(values[0])
        queue.close()
    }

    @Test
    @Throws(IOException::class)
    fun testConcurrentAddRemoveElements() = runTest {
        // This test ensures that we update 'first' correctly.
        val queue: QueueFile = newQueueFile()
        val max = 100
        var addIndex = 0
        var removeIndex = 0
        (0..max).map {
            if (Random.nextBoolean()) {
                async {
                    queue.add(values[addIndex++])
                }
            } else {
                async {
                    if (!queue.isEmpty) {
                        assertThat(queue.peek()).isEqualTo(values[removeIndex])
                        queue.remove()
                        removeIndex++
                    }
                }
            }
        }.awaitAll()
        queue.close()
    }

    @Test
    @Throws(IOException::class)
    fun testConcurrentAddFlushElements() = runTest {
        val queue: QueueFile = newQueueFile()
        val max = 10000
        val batchSize = 20
        (0..max).map {
            if (it % 20 == 0) {
                async {
                    while (true) {
                        val size = queue.size()
                        if (size == 0) {
                            break
                        }
                        queue.remove(minOf(size, batchSize))
                    }
                    assertThat(queue.isEmpty).isTrue()
                }
            } else {
                async {
                    queue.add(values[it % values.size])
                }
            }
        }.awaitAll()
        queue.close()
    }


    /**
     * A RandomAccessFile that can break when you go to write the COMMITTED
     * status.
     */
    internal class BrokenRandomAccessFile(file: File?, mode: String?) :
        RandomAccessFile(file, mode) {
        var rejectCommit = true

        @Throws(IOException::class)
        override fun write(b: ByteArray, off: Int, len: Int) {
            if (rejectCommit && filePointer == 0L) {
                throw IOException("No commit for you!")
            }
            super.write(b, off, len)
        }
    }

    companion object {
        private val logger = Logger.getLogger(QueueFileTest::class.java.name)

        /**
         * Takes up 33401 bytes in the queue (N*(N+1)/2+4*N). Picked 254 instead of 255 so that the number
         * of bytes isn't a multiple of 4.
         */
        private val N = 254
        private val values = arrayOfNulls<ByteArray>(N)

        init {
            for (i in 0 until N) {
                val value = ByteArray(i)
                // Example: values[3] = { 3, 2, 1 }
                for (ii in 0 until i) value[ii] = (i - ii).toByte()
                values[i] = value
            }
        }

        @JvmStatic
        @Parameterized.Parameters(name = "{0}")
        fun parameters(): List<Array<Any>> {
            return listOf(arrayOf("Legacy", true, 16), arrayOf("Versioned", false, 32))
        }
    }
}
