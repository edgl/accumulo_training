package working;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class EqualityFilter extends Filter {

    private static final String EQUALITY_OPTION = "equalityOption";
    private static final String VALUE_OPTION = "valueOption";
    private static final Set<String> OPTION_KEYS = new HashSet<>(Arrays.asList("eq", "neq", "lt", "lte", "gt", "gte"));

    private String eqOption = null;
    private Double value = null;

    @Override
    public boolean accept(Key k, Value v) {

        // We'll use a switch option in here
        // We have listed that it should support 5 filter options
        // "eg", "lte", "lt", "gt", "gte". Code this below
        // and provide the actual boolean logic
        //
        // EX:
        // switch (option) {
        //   case "eg":
        //      return Double == Double
        //   ...
        // }

        return false;
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions opts = super.describeOptions();

        // Add the two options available for this filter. Be sure
        // to put meaningful descriptions
        // use opts.addNamedOption()

        return opts;
    }

    @Override
    public boolean validateOptions(Map<String, String> options) {
        if (!super.validateOptions(options) || !options.containsKey(EQUALITY_OPTION) || !options.containsKey(VALUE_OPTION)) {
            return false;
        }

        String eqOpts = options.get(EQUALITY_OPTION);
        String valOpts = options.get(VALUE_OPTION);

        if (!OPTION_KEYS.contains(eqOpts)) {
            return false;
        }

        try {
            this.value = Double.parseDouble(valOpts);
        } catch (Exception ex) {
            return false;
        }

        return true;

    }

    /*
     * Provide static methods for setting the options
     */
    public static void setEqualityOption(final IteratorSetting setting, String equalityOption) {
        setting.addOption(EQUALITY_OPTION, equalityOption);
    }

    // Add the setValueOption here.
    // CODE


    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);

        if (options.containsKey(EQUALITY_OPTION)) {
            this.eqOption = options.get(EQUALITY_OPTION);
        }

        if (options.containsKey(VALUE_OPTION)) {
            this.value = Double.parseDouble(options.get(VALUE_OPTION));
        }

    }
}
