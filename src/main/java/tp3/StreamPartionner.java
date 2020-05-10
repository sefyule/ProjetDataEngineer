package tp3;

import org.apache.kafka.streams.processor.StreamPartitioner;

public class StreamPartionner implements StreamPartitioner {

    @Override
    public Integer partition(String s, Object o, Object o2, int i) {
        int randomPartition = (int)(Math.random() * ((3)));
        System.out.println("Partition : "+randomPartition);
        return randomPartition;
    }
}
