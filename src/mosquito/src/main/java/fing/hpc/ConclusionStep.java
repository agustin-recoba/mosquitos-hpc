package fing.hpc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class ConclusionStep {
	public static class M extends Mapper<Text, Text, Text, TextArrayWritable> {
		@Override
		public void map(Text key, Text detectedChangePoints, Context context)
				throws IOException, InterruptedException {

			CustomKey oldKey = new CustomKey(key);

			// Armo clave de nuevo, pero descartando codigoProd y codigoLocal
			CustomKey newKey = new CustomKey(
					oldKey.categoria,
					oldKey.departamento,
					oldKey.fecha,
					CustomKey.NULL_I,
					CustomKey.NULL_I);

			String[] detectedChangePointsArrayString = detectedChangePoints.toString().split(",");
			Text[] detectedChangePointsArrayText = new Text[detectedChangePointsArrayString.length];
			for (int i = 0; i < detectedChangePointsArrayString.length; i++) {
				detectedChangePointsArrayText[i] = new Text(detectedChangePointsArrayString[i]);
			}

			// Escribo la nueva clave con los puntos de cambio detectados en la clave
			// anterior (o sea, por producto)
			context.write(newKey.text, new TextArrayWritable(detectedChangePointsArrayText));
		}
	}

	public static class R extends Reducer<Text, TextArrayWritable, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<TextArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			// La clave es la nueva clave, que no tiene codigoProd ni codigoLocal
			// Los valores son los puntos de cambio detectados en el paso anterior con una
			// granularidad más fina.
			// Los uno todos para extraer conclusiones a nivel de la nueva clave formada.

			// Conteo de ocurrencias de cada punto de cambio
			Map<String, Integer> changePointsOcurrences = new HashMap<>();
			for (TextArrayWritable changePoints : values) {
				for (Text changePoint : changePoints.get()) {
					String point = changePoint.toString().trim();
					if (point == null || point.isEmpty())
						continue;

					if (changePointsOcurrences.containsKey(point)) {
						changePointsOcurrences.put(point, changePointsOcurrences.get(point) + 1);
					} else {
						changePointsOcurrences.put(point, 1);
					}
				}
			}

			context.write(key, new Text(getJsonString(changePointsOcurrences)));
		}

		public static String getJsonString(Map<String, Integer> map) {
			StringBuilder sb = new StringBuilder();
			String comilla = "\"";

			List<Map.Entry<String, Integer>> list = new ArrayList<>(map.entrySet());
			Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
				@Override
				public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
					return o2.getValue().compareTo(o1.getValue());
				}
			});

			sb.append("{");
			for (Map.Entry<String, Integer> entry : list)
				sb.append(comilla + entry.getKey().trim() + comilla + ": " + entry.getValue() + ", ");

			if (sb.length() > 1)
				sb.delete(sb.length() - 2, sb.length());
			sb.append("}");
			return sb.toString();
		}
	}

}