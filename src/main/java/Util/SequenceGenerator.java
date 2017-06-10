package Util;

/**
 * Created by bhushan on 5/15/17.
 */
public class SequenceGenerator {
    private Long seq_num;

    public SequenceGenerator() {
        this.seq_num = new Long(0);
    }

    public SequenceGenerator(Long initialSeq) {
        this.seq_num = initialSeq;
    }

    public synchronized String getNextSequence() {
        this.seq_num++;
        return this.seq_num.toString();
    }

    public synchronized Long getNextSequenceNumber() {
        this.seq_num++;
        return this.seq_num;
    }
}
