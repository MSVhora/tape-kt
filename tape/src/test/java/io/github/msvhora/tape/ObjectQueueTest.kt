package io.github.msvhora.tape

import com.google.common.truth.Truth.assertThat
import com.squareup.burst.BurstJUnit4
import com.squareup.burst.annotation.Burst
import kotlinx.coroutines.test.runTest
import org.junit.Assert
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import java.io.File
import java.io.IOException
import java.io.OutputStream

@RunWith(BurstJUnit4::class)
class ObjectQueueTest {
    @JvmField
    @Rule
    var folder = TemporaryFolder()

    @Burst
    var factory: QueueFactory? = null
    private var queue: ObjectQueue<String>? = null

    @Before
    @Throws(IOException::class)
    fun setUp() = runTest {
        val parent = folder.getRoot()
        val file = File(parent, "object-queue")
        val queueFile: QueueFile = QueueFile.Builder(file).build()
        queue = factory!!.create(queueFile, StringConverter())
        queue?.add("one")
        queue?.add("two")
        queue?.add("three")
    }

    @Test
    @Throws(IOException::class)
    fun size() = runTest {
        assertThat(queue?.size()).isEqualTo(3)
    }

    @Test
    @Throws(IOException::class)
    fun peek() = runTest {
        assertThat(queue?.peek()).isEqualTo("one")
    }

    @Test
    @Throws(IOException::class)
    fun peekMultiple() = runTest {
        assertThat(queue?.peek(2)).containsExactly("one", "two")
    }

    @Test
    @Throws(IOException::class)
    fun peekMaxCanExceedQueueDepth() = runTest {
        assertThat(queue?.peek(6)).containsExactly("one", "two", "three")
    }

    @Test
    @Throws(IOException::class)
    fun asList() = runTest {
        assertThat(queue?.asList()).containsExactly("one", "two", "three")
    }

    @Test
    @Throws(IOException::class)
    fun remove() = runTest {
        queue?.remove()
        assertThat(queue?.asList()).containsExactly("two", "three")
    }

    @Test
    @Throws(IOException::class)
    fun removeMultiple() = runTest {
        queue?.remove(2)
        assertThat(queue?.asList()).containsExactly("three")
    }

    @Test
    @Throws(IOException::class)
    fun clear() = runTest {
        queue?.clear()
        assertThat(queue?.size()).isEqualTo(0)
    }

    @get:Throws(IOException::class)
    @get:Test
    val isEmpty: Unit
        get() = runTest {
            assertThat(queue?.isEmpty()).isFalse()
            queue?.clear()
            assertThat(queue?.isEmpty()).isTrue()
        }

    @Test
    @Throws(IOException::class)
    fun testIterator() = runTest {
        val saw: MutableList<String> = ArrayList()
        queue?.let {
            for (pojo in it) {
                saw.add(pojo)
            }
        }
        assertThat(saw).containsExactly("one", "two", "three")
    }

    @Test
    @Throws(IOException::class)
    fun testIteratorNextThrowsWhenEmpty() = runTest {
        queue?.clear()
        val iterator = queue?.iterator()
        try {
            iterator?.next()
            Assert.fail()
        } catch (ignored: NoSuchElementException) {
        }
    }

    @Test
    @Throws(IOException::class)
    fun testIteratorNextThrowsWhenExhausted() {
        val iterator = queue?.iterator()
        iterator?.next()
        iterator?.next()
        iterator?.next()
        try {
            iterator?.next()
            Assert.fail()
        } catch (ignored: NoSuchElementException) {
        }
    }

    @Test
    @Throws(IOException::class)
    fun testIteratorRemove() = runTest {
        val iterator = queue?.iterator()
        iterator?.next()
        iterator?.remove()
        assertThat(queue?.asList()).containsExactly("two", "three")
        iterator?.next()
        iterator?.remove()
        assertThat(queue?.asList()).containsExactly("three")
    }

    @Test
    @Throws(IOException::class)
    fun testIteratorRemoveDisallowsConcurrentModification() = runTest {
        if (queue is InMemoryObjectQueue) {
            val iterator = queue?.iterator()
            iterator?.next()
            queue?.remove()
            try {
                iterator?.remove()
                Assert.fail()
            } catch (ignored: ConcurrentModificationException) {
            }
        } else {
            val iterator = queue?.iterator()
            iterator?.next()
            queue?.remove()
            try {
                iterator?.remove()
            } catch (ignored: ConcurrentModificationException) {
                Assert.fail()
            }
        }
    }

    @Test
    @Throws(IOException::class)
    fun testIteratorHasNextDisallowsConcurrentModification() = runTest {
        if (queue is InMemoryObjectQueue) {
            val iterator = queue?.iterator()
            iterator?.next()
            queue?.remove()
            try {
                iterator?.hasNext()
                Assert.fail()
            } catch (ignored: ConcurrentModificationException) {
            }
        } else {
            val iterator = queue?.iterator()
            iterator?.next()
            queue?.remove()
            try {
                iterator?.hasNext()
            } catch (ignored: ConcurrentModificationException) {
                Assert.fail()
            }
        }
    }

    @Test
    @Throws(IOException::class)
    fun testIteratorDisallowsConcurrentModificationWithClear() = runTest {
        if (queue is InMemoryObjectQueue) {
            val iterator = queue?.iterator()
            iterator?.next()
            queue?.clear()
            try {
                iterator?.hasNext()
                Assert.fail()
            } catch (ignored: ConcurrentModificationException) {
            }
        } else {
            val iterator = queue?.iterator()
            iterator?.next()
            queue?.clear()
            try {
                iterator?.hasNext()
            } catch (ignored: ConcurrentModificationException) {
                Assert.fail()
            }
        }
    }

    @Test
    @Throws(IOException::class)
    fun testIteratorOnlyRemovesFromHead() = runTest {
        if (queue is InMemoryObjectQueue) {
            val iterator = queue?.iterator()
            iterator?.next()
            iterator?.next()
            try {
                iterator?.remove()
                Assert.fail()
            } catch (ex: UnsupportedOperationException) {
                assertThat(ex).hasMessageThat()
                    .isEqualTo("Removal is only permitted from the head.")
            }
        } else {
            val iterator = queue?.iterator()
            iterator?.next()
            iterator?.next()
            try {
                iterator?.remove()
            } catch (ex: UnsupportedOperationException) {
                assertThat(ex).hasMessageThat()
                    .isEqualTo("Removal is only permitted from the head.")
                Assert.fail()
            }
        }
    }

    @Test
    @Throws(IOException::class)
    fun iteratorThrowsIOException() = runTest {
        val parent = folder.getRoot()
        val file = File(parent, "object-queue")
        val queueFile: QueueFile = QueueFile.Builder(file).build()
        val queue = ObjectQueue.create(queueFile, object : ObjectQueue.Converter<Any> {
            @Throws(IOException::class)
            override fun from(source: ByteArray): String? {
                throw IOException()
            }

            override fun toStream(value: Any, sink: OutputStream) {}
        })
        queue.add(Any())
        val iterator = queue.iterator()
        try {
            iterator.next()
            Assert.fail()
        } catch (ioe: Exception) {
            assertThat(ioe).isInstanceOf(IOException::class.java)
        }
    }

    enum class QueueFactory {
        FILE {
            @Throws(IOException::class)
            override fun <T> create(
                queueFile: QueueFile,
                converter: ObjectQueue.Converter<T>,
            ): ObjectQueue<T> {
                return ObjectQueue.create(queueFile, converter)
            }
        },
        MEMORY {
            override fun <T> create(
                queueFile: QueueFile,
                converter: ObjectQueue.Converter<T>,
            ): ObjectQueue<T> {
                return ObjectQueue.createInMemory()
            }
        };

        @Throws(IOException::class)
        abstract fun <T> create(
            queueFile: QueueFile,
            converter: ObjectQueue.Converter<T>,
        ): ObjectQueue<T>?
    }

    internal class StringConverter : ObjectQueue.Converter<String> {
        @Throws(IOException::class)
        override fun from(source: ByteArray): String {
            return String(source, charset("UTF-8"))
        }

        override fun toStream(value: String, sink: OutputStream) {
            value.toByteArray(charset("UTF-8")).let { sink.write(it) }
        }
    }
}
