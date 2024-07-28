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
                context.write(new Text(parts[1]), new Text("local:" + parts[0])); // (CODIGO_LOCAL, DEPARTAMENTO)
            }
        }   


        public static class ProductosMapper extends Mapper<LongWritable, Text, Text, Text> {
            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String record = value.toString();
                String[] parts = record.split(";");
                context.write(new Text(parts[1]), new Text("producto:" + parts[0])); // (CODIGO_PRODUCTO, CATEGORIA)
            }
        }

        public static class VentasMapper extends Mapper<LongWritable, Text, Text, Text> {
            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String record = value.toString();
                String[] parts = record.split(";");
                context.write(new Text(parts[0]), new Text("venta_local:" + record)); // (CODIGO_LOCAL, resto de la línea)
                context.write(new Text(parts[1]), new Text("venta_producto:" + record)); // (CODIGO_PRODUCTO, resto de la línea)
            }
        }

}