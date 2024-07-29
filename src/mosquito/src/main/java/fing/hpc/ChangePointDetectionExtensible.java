package fing.hpc;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

class ChangePointDetectionExtensible {

    static interface CPAlgorithmIface {
        abstract List<Date> detectChangePoints(List<DataPoint> dataPoints) throws IOException, InterruptedException;
    }

    static class CPReducer extends Reducer<Text, Text, Text, TextArrayWritable> {
        public CPAlgorithmIface changePointDetectionAlgorithm = null;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Convertimos en DataPoints
            List<DataPoint> dataPoints = new ArrayList<>();
            for (Text k_v : values) {
                ValuePair vars = new ValuePair(k_v);
                try {
                    dataPoints.add(new DataPoint(vars.x, Float.parseFloat(vars.y)));
                } catch (ParseException e) {
                    // Nada
                }
            }

            // Ordenamos por fecha
            Collections.sort(dataPoints, new Comparator<DataPoint>() {
                @Override
                public int compare(DataPoint x1, DataPoint x2) {
                    return x1.date.compareTo(x2.date);
                }
            });

            // Invocamos la detección de puntos de cambio
            print("Invocando algoritmo de detección de puntos de cambio para la clave '" + key.toString() + "'.");

            RunnableAlgo timeredRun = new RunnableAlgo(dataPoints, changePointDetectionAlgorithm);
            Thread t = new Thread(timeredRun);
            t.setDaemon(true);
            t.start();
            try {
                t.join(Constants.MAX_SECONDS_CHANGEPOINT_EXECUTION * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            List<Date> changePoints = timeredRun.changePoints;
            print("Fin del algoritmo de detección de puntos de cambio para la clave '" + key.toString() + "'.");

            if (changePoints == null) {
                print("Timeout en el algoritmo");
                Text[] changePointsArray = new Text[1];
                changePointsArray[0] = new Text("Timeout");
                context.write(key, new TextArrayWritable(changePointsArray));
            } else if (!changePoints.isEmpty()) {
                print("Puntos de cambio detectados");
                Text[] changePointsArray = new Text[changePoints.size()];
                for (int i = 0; i < changePoints.size(); i++) {
                    changePointsArray[i] = new Text(DataPoint.formater.format(changePoints.get(i)).trim());
                }
                context.write(key, new TextArrayWritable(changePointsArray));
            } else {
                print("Ningun punto detectado");
                Text[] changePointsArray = new Text[1];
                changePointsArray[0] = new Text("NoChangePoint");
            }
        }

        public static void print(String s) {
            System.out.println(s);
        }
    }

    static class RunnableAlgo implements Runnable {
        List<DataPoint> dataPoints = null;
        List<Date> changePoints = null;
        CPAlgorithmIface changePointDetectionAlgorithm = null;

        public RunnableAlgo(List<DataPoint> dataPoints, CPAlgorithmIface changePointDetectionAlgorithm) {
            this.dataPoints = dataPoints;
            this.changePointDetectionAlgorithm = changePointDetectionAlgorithm;
        }

        @Override
        public void run() {
            System.out.println("Ejecutando...");
            try {
                changePoints = changePointDetectionAlgorithm.detectChangePoints(dataPoints);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    static class CPDriver extends CacheHdfs.CDriver {
        public Class<? extends CPReducer> changePointDetectionReducer = null;

        public CPDriver() {
            super();
            jobCount = 2;
        }

        @Override
        public void configureJob(Job job, int i) throws Exception {
            job.setJarByClass(SimplePELTDriver.class);

            if (i == 1) {
                super.configureJob(job, i); // join cache

                job.setMapperClass(HdfsHashJoinMapper.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);

                // job.setCombinerClass(job_combine_class);
                job.setReducerClass(changePointDetectionReducer);

                job.setOutputKeyClass(Text.class); // Escribe clave
                job.setOutputValueClass(TextArrayWritable.class); // Escribe fecha y Average(valor)
                job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");
            } else if (i == 2) {
                job.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ";");

                job.setInputFormatClass(KeyValueTextInputFormat.class);

                job.setMapperClass(ConclusionStep.M.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(TextArrayWritable.class);

                // job.setCombinerClass(job_combine_class);
                job.setReducerClass(ConclusionStep.R.class);
                job.setNumReduceTasks(1); // Solo un reducer, para que solo haya un archivo de salida

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
            }

        }
    }

}