package org.apache.camel.component.zookeeper;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.camel.component.zookeeper.NaturalSortComparator.Order;
import org.junit.Test;

public class NaturalSortComparatorTest {

    @Test
    public void testSortOrder() throws Exception {

        List<String> sorted = Arrays.asList(new String[] {"0", "1", "3", "4.0", "11", "30", "55", "225", "333",
                                                          "camel-2.1.0", "camel-2.1.1", "camel-2.2.0"});
        List<String> unsorted = new ArrayList<String>(sorted);
        Collections.shuffle(unsorted);
        Collections.sort(unsorted, new NaturalSortComparator());
        compareLists(sorted, unsorted);

        Collections.shuffle(unsorted);
        Collections.sort(unsorted, new NaturalSortComparator(Order.Descending));
        Collections.reverse(sorted);
        compareLists(sorted, unsorted);
    }

    private void compareLists(List<String> sorted, List<String> unsorted) {
        for (int x = 0; x < unsorted.size(); x++) {
            assertEquals(sorted.get(x), unsorted.get(x));
        }
    }
}
