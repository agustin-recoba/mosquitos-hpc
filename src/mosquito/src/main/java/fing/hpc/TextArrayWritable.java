package fing.hpc;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

class TextArrayWritable extends ArrayWritable {

    public TextArrayWritable() {
        super(Text.class);
    }

    public TextArrayWritable(Text[] values) {
        super(Text.class, values);
    }

    @Override
    public Text[] get() {
        Writable[] writables = super.get();
        Text[] values = new Text[writables.length];
        for (int i = 0; i < writables.length; i++) {
            values[i] = (Text) writables[i];
        }

        return values;
    }

    @Override
    public String toString() {
        Text[] values = get();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            sb.append(values[i].toString());
            if (i < values.length - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

}