package org.alfresco.jlan.test.inmemory;

import static org.testng.Assert.assertTrue;

import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class TestngSampleTest {
    @Test
    public void testngFunctionality() {
        assertThat("not nice", true, is(equalTo(!false)));
    }

    @Test
    public void testngAssertTrue() {
        assertTrue(true);
    }

//    @Test
//    public void expectThrownException() {
//        TestThing testThing = new TestThing();
//        RuntimeException ex = expectThrows(RuntimeException.class, () -> testThing.call());
//        assertThat(testThing.counter, is(equalTo(1)));
//        assertThat(ex.getMessage(), is(equalTo("some Message")));
//    }

    private class TestThing {
        public int counter = 0;
        public void call() {
            this.counter++;
            throw new RuntimeException("some Message");
        }
    }

    /**
     * Asserts that {@code runnable} throws an exception of type {@code expectedThrowable} when
     * executed. If it does, the exception object is returned. If it does not throw an exception, an
     * {@link AssertionError} is thrown. If it throws the wrong type of exception, an {@code
     * AssertionError} is thrown describing the mismatch; the exception that was actually thrown can
     * be obtained by calling {@link AssertionError#getCause}.
     *
     * @param expectedThrowable the expected type of the exception
     * @param runnable       a function that is expected to throw an exception when executed
     * @return the exception thrown by {@code runnable}
     * @since 4.13
     */
    public static <T extends Throwable> T expectThrows(Class<T> expectedThrowable, ThrowingRunnable runnable) {
        try {
            runnable.run();
        } catch (Throwable actualThrown) {
            if (expectedThrowable.isInstance(actualThrown)) {
                @SuppressWarnings("unchecked") T retVal = (T) actualThrown;
                return retVal;
            } else {
                String mismatchMessage = format("unexpected exception type thrown;",
                    expectedThrowable.getSimpleName(), actualThrown.getClass().getSimpleName());

                // The AssertionError(String, Throwable) ctor is only available on JDK7.
                AssertionError assertionError = new AssertionError(mismatchMessage);
                assertionError.initCause(actualThrown);
                throw assertionError;
            }
        }
        String message = String.format("expected %s to be thrown, but nothing was thrown",
                expectedThrowable.getSimpleName());
        throw new AssertionError(message);
    }
    static String format(String message, Object expected, Object actual) {
        String formatted = "";
        if (message != null && !"".equals(message)) {
            formatted = message + " ";
        }
        String expectedString = String.valueOf(expected);
        String actualString = String.valueOf(actual);
      if (expectedString.equals(actualString)) {
          return formatted + "expected: "
                  + formatClassAndValue(expected, expectedString)
                  + " but was: " + formatClassAndValue(actual, actualString);
      } else {
          return formatted + "expected:<" + expectedString + "> but was:<"
                  + actualString + ">";
      }
  }
    private static String formatClassAndValue(Object value, String valueString) {
        String className = value == null ? "null" : value.getClass().getName();
        return className + "<" + valueString + ">";
    }
}
