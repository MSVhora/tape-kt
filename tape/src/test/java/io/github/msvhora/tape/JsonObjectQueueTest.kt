package io.github.msvhora.tape

/**
 * Created by Murtuza Vhora(@msvhora) on 03,April,2024
 */

import com.google.common.truth.Truth.assertThat
import com.squareup.burst.BurstJUnit4
import com.squareup.burst.annotation.Burst
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.junit.Assert
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import java.io.File
import java.io.IOException
import java.io.OutputStream
import java.util.UUID
import kotlin.random.Random

/**
 * Tests for Json Object Queue.
 *
 * @author Murtuza Vhora (murtazavhora@gmail.com)
 */
@RunWith(BurstJUnit4::class)
class JsonObjectQueueTest {
    @JvmField
    @Rule
    var folder = TemporaryFolder()

    @Burst
    var factory: QueueFactory? = null
    private var queue: ObjectQueue<JsonData?>? = null

    @Before
    @Throws(IOException::class)
    fun setUp() = runTest {
        val parent = folder.getRoot()
        val file = File(parent, "json-object-queue")
        val queueFile: QueueFile = QueueFile.Builder(file).build()
        queue = factory!!.create(queueFile, JsonConverter())
        queue?.add(JsonData("one"))
        queue?.add(JsonData("two"))
        queue?.add(JsonData("three"))
    }

    @Test
    @Throws(IOException::class)
    fun size() = runTest {
        assertThat(queue?.size()).isEqualTo(3)
    }

    @Test
    @Throws(IOException::class)
    fun peek() = runTest {
        assertThat(queue?.peek()).isEqualTo(JsonData("one"))
    }

    @Test
    @Throws(IOException::class)
    fun peekMultiple() = runTest {
        assertThat(queue?.peek(2)).containsExactly(JsonData("one"), JsonData("two"))
    }

    @Test
    @Throws(IOException::class)
    fun peekMaxCanExceedQueueDepth() = runTest {
        assertThat(queue?.peek(6)).containsExactly(
            JsonData("one"),
            JsonData("two"),
            JsonData("three")
        )
    }

    @Test
    @Throws(IOException::class)
    fun asList() = runTest {
        assertThat(queue?.asList()).containsExactly(
            JsonData("one"),
            JsonData("two"),
            JsonData("three")
        )
    }

    @Test
    @Throws(IOException::class)
    fun remove() = runTest {
        queue?.remove()
        assertThat(queue?.asList()).containsExactly(JsonData("two"), JsonData("three"))
    }

    @Test
    @Throws(IOException::class)
    fun removeMultiple() = runTest {
        queue?.remove(2)
        assertThat(queue?.asList()).containsExactly(JsonData("three"))
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
        val saw: MutableList<JsonData> = ArrayList()
        queue?.let {
            for (pojo in it) {
                pojo?.let { data ->
                    saw.add(data)
                }
            }
        }
        assertThat(saw).containsExactly(JsonData("one"), JsonData("two"), JsonData("three"))
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
        assertThat(queue?.asList()).containsExactly(JsonData("two"), JsonData("three"))
        iterator?.next()
        iterator?.remove()
        assertThat(queue?.asList()).containsExactly(JsonData("three"))
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
        val queue = ObjectQueue.create(queueFile, object : ObjectQueue.Converter<Any?> {
            @Throws(IOException::class)
            override fun from(source: ByteArray?): String {
                throw IOException()
            }

            override fun toStream(value: Any?, sink: OutputStream?) {}
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

    @Test
    @Throws(IOException::class)
    fun testConcurrentAddRemoveElements() = runTest {
        // This test ensures that we update 'first' correctly.
        val max = 100
        var addIndex = -1
        var removeIndex = -1
        queue?.clear()
        (0..max).map {
            if (Random.nextBoolean()) {
                async {
                    queue?.add(JsonData((++addIndex).toString()))
                }
            } else {
                async {
                    if (queue?.isEmpty() == false) {
                        assertThat(queue?.peek()).isEqualTo(JsonData((++removeIndex).toString()))
                        queue?.remove()
                    }
                }
            }
        }.awaitAll()
    }

    @Test
    @Throws(IOException::class)
    fun testConcurrentAddFlushElements() = runTest {
        val max = 100000
        val batchSize = 20
        val delayInMilliseconds = 20000L

        // Flush Elements after 2s
        val job = async {
            while (true) {
                delay(delayInMilliseconds)
                while (true) {
                    val size = queue?.size() ?: 0
                    if (size == 0) {
                        break
                    }
                    queue?.remove(minOf(size, batchSize))
                }
                assertThat(queue?.isEmpty()).isTrue()
            }
        }
        job.start()
        (0..max).map {
            async {
                queue?.add(JsonData(it.toString()))
                if (it == max) {
                    job.cancel()
                }
            }
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

    @Serializable
    internal data class JsonData(
        @SerialName("value")
        val value: String,
        @SerialName("id")
        val id: String = UUID.randomUUID().toString(),
        @SerialName("list")
        val list: List<String> = listOf(
            "one",
            "two",
            "three",
            "four",
            "five",
        ),
        val intData: Int = -1,
        val longData: Long = 0L,
        val floatData: Float = 0f,
        val doubleData: Double = 0.0,
    ) {
        override fun equals(other: Any?): Boolean {
            return other is JsonData
                    && other.value == this.value
                    && other.list == list
                    && other.intData == intData
                    && other.longData == longData
                    && other.floatData == floatData
                    && other.doubleData == doubleData
        }

        override fun hashCode(): Int {
            var result = value.hashCode()
            result = 31 * result + id.hashCode()
            return result
        }
    }

    internal class JsonConverter : ObjectQueue.Converter<JsonData?> {
        private val json = Json

        @Throws(IOException::class)
        override fun from(source: ByteArray?): JsonData? {
            return source?.let {
                try {
                    json
                        .decodeFromString(
                            deserializer = JsonData.serializer(),
                            string = it.decodeToString()
                        )
                } catch (e: Exception) {
                    null
                }
            }
        }

        override fun toStream(value: JsonData?, sink: OutputStream?) {
            val jsonString = value?.let {
                json.encodeToString(
                    serializer = JsonData.serializer(),
                    value = it
                )
            }
            jsonString?.toByteArray(charset("UTF-8"))?.let { sink?.write(it) }
        }
    }
}
