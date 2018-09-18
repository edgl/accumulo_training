package solution;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class SumCountValue implements Serializable {

    private static final long serialVersionUID = 1L;

    private int sum = 0;
    private int count = 0;

    public Double getAverage() {
        if (count == 0) {
            return 0.0;
        }
        return (double)sum / (double)count;
    }

    public SumCountValue(int sum, int count) {
        this.count = count;
        this.sum = sum;
    }

    public SumCountValue(int sum) {
        this(sum, 1);
    }

    public SumCountValue() {
        this.sum = 0;
        this.count = 0;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    private void writeObject(ObjectOutputStream out)
            throws IOException {
        out.defaultWriteObject();
        out.writeInt(sum);
        out.writeChar(';');
        out.writeInt(count);
    }

    private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.sum = in.readInt();
        in.readChar();
        this.count = in.readInt();
    }

    public String toString() {
        return getAverage().toString();
    }

    public void apply(int sum, int count) {
        this.sum += sum;
        this.count += count;
    }

    public void apply(SumCountValue value) {
        this.sum += value.sum;
        this.count += value.count;
    }



}
