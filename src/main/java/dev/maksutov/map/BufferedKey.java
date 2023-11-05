package dev.maksutov.map;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;


//TODO Figure out better naming
public interface BufferedKey {

    boolean equals(DirectBuffer buffer);
    
    void write(MutableDirectBuffer buffer);
    
    int keySize();
    
}
