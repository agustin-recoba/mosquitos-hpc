package fing.hpc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

class CacheHdfs {

	public HashMap<Long, String> baseLocales = new HashMap<Long, String>();
	public HashMap<Long, String> baseProductos= new HashMap<Long, String>();

	public Parsers.Locales localesParser = new Parsers.Locales();
	public Parsers.Productos productosParser = new Parsers.Productos();

	public void leer(Mapper.Context context) throws IOException, InterruptedException {
		URI[] cacheFiles = context.getCacheFiles();
		FileSystem fs = FileSystem.get(context.getConfiguration());

		read(cacheFiles, fs);
	}

	public void leer(Reducer.Context context) throws IOException, InterruptedException {
		URI[] cacheFiles = context.getCacheFiles();
		FileSystem fs = FileSystem.get(context.getConfiguration());

		read(cacheFiles, fs);
	}

	public void read(URI[] cacheFiles, FileSystem fs) throws IOException {
		if (cacheFiles != null && cacheFiles.length == 2) {
			for (URI cacheFile : cacheFiles) {
				boolean esLocales = cacheFile.getPath().contains("locales");
				try {
					String line = "";

					Path getFilePath = new Path(cacheFile.toString());

					BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));

					while ((line = reader.readLine()) != null) {
						if (esLocales) {
							localesParser.parse(line);
							System.out.println(line);

							baseLocales.put(localesParser.clave, localesParser.departamento);
						} else {
							productosParser.parse(line);
							System.out.println(line);

							baseProductos.put(productosParser.clave, productosParser.categoria);
						}
					}
				} catch (NumberFormatException e) {
					System.err.println(e);
				} catch (Exception e) {
					throw new IOException("No se pudo leer el archivo de cache.");
				}
			}
		} else {
			throw new IOException("Archivo chache no se cargó.");
		}
	}

	public static void addCacheFiles(Job job) {
		try {
			job.addCacheFile(new URI("hdfs://hadoop-master:9000/data/locales.csv"));
		} catch (Exception e) {
			System.out.println("Archivo de locales no se agregó al caché distribuido");
			System.exit(1);
		}

		try {
			job.addCacheFile(new URI("hdfs://hadoop-master:9000/data/productos.csv"));
		} catch (Exception e) {
			System.out.println("Archivo de productos no se agregó al caché distribuido");
			System.exit(1);
		}
	}
	
	public static class CDriver extends ExtensibleDriver {

		@Override
		public void configureJob(Job job, int i) throws Exception {
			if (i==1)
				CacheHdfs.addCacheFiles(job);
		}

	}
	
	public static class CMapper<INKEY, INVAL, OUTKEY, OUTVAL> extends Mapper<INKEY, INVAL, OUTKEY, OUTVAL> {
		/*
		 * MAPPER QUE PUEDE ACCEDER A LAS BASES DE PRODUCTOS O LOCALES DESDE EL CACHE
		 * DISTRIBUIDO
		 */
		public CacheHdfs cacheHdfs = new CacheHdfs();

		public void setup(Context context) throws IOException, InterruptedException {
			cacheHdfs.leer(context);
		}

	}

	public static class CReducer<INKEY, INVAL, OUTKEY, OUTVAL> extends Reducer<INKEY, INVAL, OUTKEY, OUTVAL> {
		/*
		 * REDUCER QUE PUEDE ACCEDER A LAS BASES DE PRODUCTOS O LOCALES DESDE EL CACHE
		 * DISTRIBUIDO
		 */
		public CacheHdfs cacheHdfs = new CacheHdfs();

		public void setup(Context context) throws IOException, InterruptedException {
			cacheHdfs.leer(context);
		}
	}

}