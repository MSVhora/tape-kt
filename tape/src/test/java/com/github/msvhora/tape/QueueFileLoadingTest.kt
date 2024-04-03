// Copyright 2012 Square, Inc.
// Copyright 2024, Murtuza Vhora<murtazavhora@gmail.com>
package com.github.msvhora.tape

import com.github.msvhora.tape.QueueTestUtils.Companion.EMPTY_SERIALIZED_QUEUE
import com.github.msvhora.tape.QueueTestUtils.Companion.FRESH_SERIALIZED_QUEUE
import com.github.msvhora.tape.QueueTestUtils.Companion.ONE_ENTRY_SERIALIZED_QUEUE
import com.github.msvhora.tape.QueueTestUtils.Companion.TRUNCATED_EMPTY_SERIALIZED_QUEUE
import com.github.msvhora.tape.QueueTestUtils.Companion.TRUNCATED_ONE_ENTRY_SERIALIZED_QUEUE
import com.github.msvhora.tape.QueueTestUtils.Companion.copyTestFile
import junit.framework.TestCase.assertEquals
import kotlinx.coroutines.test.runTest
import org.junit.After
import org.junit.Assert
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import java.io.File
import java.io.IOException
import java.io.OutputStream

/**
 * This code is migrated from Java to Kotlin with its original logic intact.
 *
 * @author Murtuza Vhora (murtazavhora@gmail.com)
 */
@RunWith(JUnit4::class)
class QueueFileLoadingTest {
    private var testFile: File? = null

    @After
    fun cleanup() {
        Assert.assertTrue("Failed to delete test file " + testFile!!.path, testFile!!.delete())
    }

    @Test
    @Throws(Exception::class)
    fun testMissingFileInitializes() = runTest {
        testFile = File.createTempFile(FRESH_SERIALIZED_QUEUE, "test")
        testFile?.delete()?.let { Assert.assertTrue(it) }
        testFile?.exists()?.let { Assert.assertFalse(it) }
        val queue: QueueFile = QueueFile.Builder(testFile).build()
        assertEquals(0, queue.size())
        testFile?.exists()?.let { Assert.assertTrue(it) }
        queue.close()
    }

    @Test
    @Throws(Exception::class)
    fun testEmptyFileInitializes() = runTest {
        testFile = copyTestFile(EMPTY_SERIALIZED_QUEUE)
        val queue: QueueFile = QueueFile.Builder(testFile).build()
        assertEquals(0, queue.size())
        queue.close()
    }

    @Test
    @Throws(Exception::class)
    fun testSingleEntryFileInitializes() = runTest {
        testFile = copyTestFile(ONE_ENTRY_SERIALIZED_QUEUE)
        val queue: QueueFile = QueueFile.Builder(testFile).build()
        assertEquals(1, queue.size())
        queue.close()
    }

    @Test(expected = IOException::class)
    @Throws(Exception::class)
    fun testTruncatedEmptyFileThrows() {
        testFile = copyTestFile(TRUNCATED_EMPTY_SERIALIZED_QUEUE)
        QueueFile.Builder(testFile).build()
    }

    @Test(expected = IOException::class)
    @Throws(Exception::class)
    fun testTruncatedOneEntryFileThrows() {
        testFile = copyTestFile(TRUNCATED_ONE_ENTRY_SERIALIZED_QUEUE)
        QueueFile.Builder(testFile).build()
    }

    @Test(expected = IOException::class)
    @Throws(Exception::class)
    fun testCreateWithReadOnlyFileThrowsException() {
        testFile = copyTestFile(TRUNCATED_ONE_ENTRY_SERIALIZED_QUEUE)
        Assert.assertTrue(testFile!!.setWritable(false))

        // Should throw an exception.
        QueueFile.Builder(testFile).build()
    }

    @Test(expected = IOException::class)
    @Throws(Exception::class)
    fun testAddWithReadOnlyFileMissesMonitor() = runTest {
        testFile = copyTestFile(EMPTY_SERIALIZED_QUEUE)
        val qf: QueueFile = QueueFile.Builder(testFile).build()

        // Should throw an exception.
        val queue = FileObjectQueue(qf, object : ObjectQueue.Converter<String> {
            @Throws(IOException::class)
            override fun from(source: ByteArray): String? {
                return null
            }

            @Throws(IOException::class)
            override fun toStream(value: String, sink: OutputStream) {
                throw IOException("fake Permission denied")
            }
        })

        // Should throw an exception.
        queue.use {
            it.add("trouble")
        }
    }
}
