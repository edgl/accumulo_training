package solution.lab10;

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
        // Our filter needs to support several operations.
        // These are eq for "equals", neq for "not equals",
        // you get the idea. Think of how you will implement this.
        // remember the eqOption would have been provided when
        // the iterator was installed. Use a switch.
        // CODE
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions opts = super.describeOptions();
        opts.addNamedOption(EQUALITY_OPTION, "Determines what type of inequality to apply. Options are <eq | neq | lt | lte | gt | gte>");
        opts.addNamedOption(VALUE_OPTION, "Value that will be used to filter based on the " + EQUALITY_OPTION);

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

    public static void setEqualityOption(final IteratorSetting setting, String equalityOption) {
        setting.addOption(EQUALITY_OPTION, equalityOption);
    }

    public static void setValueOption(final IteratorSetting setting, Double valueOption) {
        setting.addOption(EQUALITY_OPTION, valueOption.toString());
    }

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
