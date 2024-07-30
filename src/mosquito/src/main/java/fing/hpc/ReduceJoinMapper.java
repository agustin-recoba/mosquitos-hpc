package fing.hpc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReduceJoinMapper {
    Parsers.Ventas ventasParser = new Parsers.Ventas();

    public static class LocalesMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(";");

            // FILTRADO DE DEPARTAMENTOS
            if (parts[0] == null || (!parts[0].equals("MONTEVIDEO")) && (!parts[0].equals("CANELONES")))
                return;

            context.write(new Text(parts[1]), new Text("local:" + parts[0])); // (CODIGO_LOCAL, DEPARTAMENTO)
        }
    }

    public static class ProductosMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(";");

            // FILTRADO DE CATEGORIAS
            if (parts[0] == null || parts[0].equals("CENSURADO"))
                return;

            context.write(new Text(parts[1]), new Text("producto:" + parts[0])); // (CODIGO_PRODUCTO, CATEGORIA)
        }
    }

    public static class ProductoVentasMapper extends Mapper<LongWritable, Text, Text, Text> {
        Parsers.Ventas ventasParser = new Parsers.Ventas();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            ventasParser.parse(value);

            // FILTRADO DE VENTAS (devoluciones, precios disparatados)
            if (ventasParser.cant_vta_original < 0 || ventasParser.precio_unitario < 10
                    || ventasParser.precio_unitario > 2000)
                return;

            context.write(new Text("" + ventasParser.clave_producto),
                    new Text("venta:" + ventasParser.clave_local + ";" + ventasParser.cant_vta_original)); // (CODIGO_PRODUCTO,
                                                                                                           // CODIGO_LOCAL;CODIGO_VENTA)

        }
    }

    public static class LocalVentaMapper extends Mapper<LongWritable, Text, Text, Text> {
        Parsers.Ventas ventasParser = new Parsers.Ventas();

        public void map(Text codLocal, Text cat_cantVta, Context context) throws IOException, InterruptedException {
            context.write(codLocal, new Text("venta:" + cat_cantVta.toString())); // (CODIGO_LOCAL, CATEGORIA;CANT_VTA)

        }
    }

}