package fing.hpc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProductoReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> ventas = new ArrayList<>();
        String categoria = "";

        for (Text t : values) {
            String value = t.toString();
            if (value.startsWith("producto:")) {
                categoria = value.substring(9);
            } else if (value.startsWith("venta:")) {
                ventas.add(value.substring(6));
            }
        }

        for (String venta : ventas) {
            String[] parts = venta.split(";"); // CODIGO_LOCAL;CANT_VTA
            context.write(new Text(parts[0]), new Text(categoria + ";" + parts[1])); // (CODIGO_LOCAL,
                                                                                     // CATEGORIA;CANT_VTA)
        }
    }
}