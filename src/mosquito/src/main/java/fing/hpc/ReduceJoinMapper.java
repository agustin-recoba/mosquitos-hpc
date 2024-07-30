package fing.hpc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReduceJoinMapper {

    public static class LocalesMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
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
        @Override
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

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                ventasParser.parse(value);
            } catch (NumberFormatException e) {
                return;
            }
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
        // Parsers.Ventas ventasParser = new Parsers.Ventas();
        @Override
        public void map(LongWritable codLocal, Text codLocal_cat_cantVta, Context context)
                throws IOException, InterruptedException {
            // Parsear
            String[] parts = codLocal_cat_cantVta.toString().split("\t");
            context.write(new Text(parts[0]), new Text("venta:" + parts[1])); // (CODIGO_LOCAL, CATEGORIA;CANT_VTA)
        }
    }

}