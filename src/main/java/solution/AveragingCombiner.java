package solution;

import org.apache.accumulo.core.client.lexicoder.impl.AbstractLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.ValueFormatException;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;

public class AveragingCombiner extends TypedValueCombiner<SumCountValue> {

    private static final SumCountValueEncoder SCVE = new SumCountValueEncoder();

    @Override
    public SumCountValue typedReduce(Key key, Iterator<SumCountValue> iter) {
        if (!iter.hasNext())
            return null;

        final SumCountValue scv = new SumCountValue();
        while(iter.hasNext()) {
            SumCountValue val = iter.next();
            scv.apply(val);
        }
        return scv;
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        setEncoder(SCVE);
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions io = super.describeOptions();
        io.setName("avg");
        io.setDescription("AveragingCombiner interprets Values as SumCountValue that contains sum and count fields necessary to find the average over windowed values");
        return io;
    }

    public static class SumCountValueEncoder extends AbstractLexicoder<SumCountValue> implements
            Encoder<SumCountValue> {

        @Override
        protected SumCountValue decodeUnchecked(byte[] b, int offset, int len) throws ValueFormatException {
            String asString = new String(b, Charset.forName("UTF-8"));
            String[] fields = asString.split(";");

            if (fields.length > 1) {
                int sum = Integer.parseInt(fields[1]);
                int count = Integer.parseInt(fields[2]);

                return new SumCountValue(sum, count);
            }

            return new SumCountValue(Integer.parseInt(fields[0]));

        }

        @Override
        public byte[] encode(SumCountValue value) {
            return (value.getAverage().toString() + ";" + value.getSum() + ";" + value.getCount()).getBytes();
        }
    }
}
