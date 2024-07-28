package fing.hpc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReduceJoinReducer {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> locales = new ArrayList<>();
        List<String> productos = new ArrayList<>();
        List<String> ventas = new ArrayList<>();

        for (Text t : values) {
            String value = t.toString();
            if (value.startsWith("local:")) {
                locales.add(value.substring(6));
            } else if (value.startsWith("producto:")) {
                productos.add(value.substring(8));
            } else if (value.startsWith("venta_local:") || value.startsWith("venta_producto:")) {
                ventas.add(value.substring(value.indexOf(':') + 1));
            }
        }

        for (String venta : ventas) {
            for (String local : locales) {
                for (String producto : productos) {
                    context.write(new Text(venta), new Text(local + ";" + producto));
                }
            }
        }
    }
}