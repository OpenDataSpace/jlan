package org.alfresco.jlan.test.inmemory;

import static org.junit.Assert.fail;

import org.junit.Rule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class JUnitSampleTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void shouldThrow() {
        TestThing testThing = new TestThing();
        thrown.expect(RuntimeException.class);
        thrown.expectMessage(startsWith("some Message"));
        testThing.call();
        assertThat(testThing.counter, is(equalTo(1)));
        fail("BLA");
    }

    private class TestThing {
        public int counter = 0;
        public void call() {
            this.counter++;
            throw new RuntimeException("some Message");
        }
    }
}
