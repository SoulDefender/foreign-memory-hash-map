package dev.maksutov.map;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Hashing;


public class FlowKey implements BufferedKey {

    private int sourceAddress;

    private int destinationAddress;

    private int sourcePort;

    private int destinationPort;


    public FlowKey(int sourceAddress, int destinationAddress, int sourcePort, int destinationPort) {

        this.sourceAddress = sourceAddress;
        this.destinationAddress = destinationAddress;
        this.sourcePort = sourcePort;
        this.destinationPort = destinationPort;
    }


    public FlowKey() {
    }


    public int getSourceAddress() {

        return sourceAddress;
    }


    public void setSourceAddress(int sourceAddress) {

        this.sourceAddress = sourceAddress;
    }


    public int getDestinationAddress() {

        return destinationAddress;
    }


    public void setDestinationAddress(int destinationAddress) {

        this.destinationAddress = destinationAddress;
    }


    public int getSourcePort() {

        return sourcePort;
    }


    public void setSourcePort(int sourcePort) {

        this.sourcePort = sourcePort;
    }


    public int getDestinationPort() {

        return destinationPort;
    }


    public void setDestinationPort(int destinationPort) {

        this.destinationPort = destinationPort;
    }


    @Override
    public int hashCode() {

        int keyPartA = getSourceAddress() + getSourcePort() << 6;
        int keyPartB = getDestinationAddress() + getDestinationPort() << 6;
        return Hashing.hash(keyPartA + keyPartB);
    }

    public boolean equals(DirectBuffer buffer) {

        int offset = 0;
        if (sourceAddress != buffer.getInt(offset)) {
            return false;
        }
        offset += 4;
        if (sourcePort != buffer.getInt(offset)) {
            return false;
        }
        offset += 4;
        if (destinationAddress != buffer.getInt(offset)) {
            return false;
        }
        offset += 4;
        return destinationPort == buffer.getInt(offset);
    }


    @Override
    public void write(MutableDirectBuffer buffer) {

        int offset = 0;
        buffer.putInt(offset, sourceAddress);
        offset += 4;
        buffer.putInt(offset, sourcePort);
        offset += 4;
        buffer.putInt(offset, destinationAddress);
        offset += 4;
        buffer.putInt(offset, destinationPort);
    }


    @Override
    public int keySize() {

        return 16;
    }
}
