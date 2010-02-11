package org.apache.camel.component.zookeeper;

public class SequenceComparator extends NaturalSortComparator {

    public final static int ZookeeperSequenceLength = 10;

    @Override
    public int compare(CharSequence sequencedNode, CharSequence otherSequencedNode) {
        if (sequencedNode == null && otherSequencedNode == null) {
            return 0;
        }
        if (sequencedNode != null && otherSequencedNode == null) {
            return 1;
        }
        if (sequencedNode == null && otherSequencedNode != null) {
            return -1;
        }
        return super.compare(getZooKeeperSequenceNumber(sequencedNode), getZooKeeperSequenceNumber(otherSequencedNode));
    }

    private CharSequence getZooKeeperSequenceNumber(CharSequence sequencedNodeName)
    {
        int len = sequencedNodeName.length();
        return sequencedNodeName.subSequence(len- ZookeeperSequenceLength, len);
    }
}
