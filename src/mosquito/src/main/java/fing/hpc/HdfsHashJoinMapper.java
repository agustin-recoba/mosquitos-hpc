package fing.hpc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

class HdfsHashJoinMapper extends CacheHdfs.CMapper<LongWritable, Text, Text, Text> {
    Parsers.Ventas ventasParser = new Parsers.Ventas();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            ventasParser.parse(value);

            String categoria = cacheHdfs.baseProductos.get(ventasParser.clave_producto);

            String departamento = cacheHdfs.baseLocales.get(ventasParser.clave_local);

            // FILTRADO DE CATEGORIAS
            if (categoria == null || categoria.equals("CENSURADO"))
                return;

            // FILTRADO DE DEPARTAMENTOS
            if (departamento == null || (!departamento.equals("MONTEVIDEO") && !departamento.equals("CANELONES")))
                return;

            // FILTRADO DE VENTAS (devoluciones, precios disparatados)
            if (ventasParser.cant_vta_original < 0 || ventasParser.precio_unitario < 10
                    || ventasParser.precio_unitario > 2000)
                return;

            // new LongWritable(ventasParser.clave_local)
            // new LongWritable(ventasParser.clave_producto)
            // new LongWritable(ventasParser.clave_venta)
            // new FloatWritable(ventasParser.precio_unitario)
            // new FloatWritable(ventasParser.cant_vta)
            // new FloatWritable(ventasParser.cant_vta_original)

            // ELECCION DE CLAVE
            context.write(
                    new CustomKey(
                            categoria,
                            ventasParser.fecha.substring(0, 4),
                            ventasParser.clave_producto).text, // Cat, año y producto
                    new ValuePair(
                            ventasParser.fecha,
                            Float.toString(ventasParser.precio_unitario)).text);

        } catch (NumberFormatException e) {
            System.err.println(e);
        }
    }
}