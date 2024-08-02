package fing.hpc;

import org.apache.hadoop.io.Text;

public class Parsers {
	public static class Productos {
		public String categoria;
		public long clave;

		public void parse(String record) {
			String[] records = record.split(";", 0);

			categoria = records[0];
			clave = Long.parseLong(records[1]);
		}

		public void parse(Text record) {
			parse(record.toString());
		}
	}

	public static class Locales {

		public String departamento;
		public long clave;

		public void parse(String record) {
			String[] records = record.split(";", 0);

			departamento = records[0];
			clave = Long.parseLong(records[1]);
		}

		public void parse(Text record) {
			parse(record.toString());
		}
	}

	public static class Ventas {
		public long clave_local;
		public long clave_producto;
		public String fecha;
		public float cant_vta_original;
		public float cant_vta;
		public float precio_unitario;
		public long clave_venta;

		public void parse(String record) {
			String[] records = record.split(";", 0);

			clave_local = Long.parseLong(records[0]);
			clave_producto = Long.parseLong(records[1]);
			fecha = records[2];
			cant_vta_original = Float.parseFloat(records[3]);
			cant_vta = Float.parseFloat(records[4]);
			precio_unitario = Float.parseFloat(records[5]);
			clave_venta = Long.parseLong(records[6]);
		}

		public void parse(Text record) {
			parse(record.toString());
		}
	}
}