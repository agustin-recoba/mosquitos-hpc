package fing.hpc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LocalReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> ventas = new ArrayList<>();
        String departamento = "";

        for (Text t : values) {
            String value = t.toString();
            if (value.startsWith("local:")) {
                departamento = value.substring(6);
            } else if (value.startsWith("venta:")) {
                ventas.add(value.substring(6));
            }
        }

        for (String venta : ventas) {
            String[] parts = venta.split(";"); // CATEGORIA;CANT_VTA
            context.write(new Text(parts[0] + "\t" + departamento + "\t" + parts[1]), new Text("NULL"));
            // (CATEGORIA,
            // DEPARTAMENTO,
            // CANT_VTA)
        }
    }
}