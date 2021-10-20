import java.io.File;
import java.util.Collection;
import java.util.Vector;

abstract class MapReduce {
	public static final boolean DEBUG = false;

	private String InputPath;
	private String OutputPath;
	private int nfiles;

	private Vector<Map> Mappers = new Vector<Map>();
	private Vector<Reduce> Reducers = new Vector<Reduce>();

	public MapReduce() {
		SetInputPath("");
		SetOutputPath("");
	}

	// Constructor MapReduce: número de reducers a utilizar. Los parámetros de
	// directorio/fichero entrada
	// y directorio salida se inicilizan mediante Set* y las funciones Map y reduce
	// sobreescribiendo los
	// métodos abstractos.

	public MapReduce(String input, String output, int nReducers) {
		SetInputPath(input);
		SetOutputPath(output);
		SetReducers(nReducers);
	}

	private void AddMap(Map map) {
		Mappers.add(map);
	}

	private void AddReduce(Reduce reducer) {
		Reducers.add(reducer);
	}

	public void SetInputPath(String path) {
		InputPath = path;
	}

	public void SetOutputPath(String path) {
		OutputPath = path;
	}

	public void SetReducers(int nReducers) {
		for (int x = 0; x < nReducers; x++) {
			AddReduce(new Reduce(this, OutputPath + "/result.r" + (x + 1)));
		}
	}

	// Procesa diferentes fases del framework mapreduc: split, map, suffle/merge,
	// reduce.
	public Error Run() {

		if (Split(InputPath) != Error.COk)
			Error.showError("MapReduce::Run-Error Split");
			

		// if (Maps() != Error.COk)
		// Error.showError("MapReduce::Run-Error Map");

		// if (Suffle() != Error.COk)
		// Error.showError("MapReduce::Run-Error Merge");
		Thread thr[];
		thr = new Thread[Reducers.size()];

		for (int i = 0; i < Reducers.size(); i++) {
			thr[i] = new Thread(new Fases_Concurentes_2(i));
			thr[i].start();
		}

		for (int i = 0; i < Reducers.size(); i++) {
			try {
				thr[i].join();
			} catch (InterruptedException e) { // Fin //
			}
			System.out.println("Thread number " + thr[i].getId() + " is alive: " + thr[i].isAlive());
		}

		// if (Reduces() != Error.COk)
		// Error.showError("MapReduce::Run-Error Reduce");

		/*
		 * //Primeros threads for (int i = 0; i < nfiles; i++) { // creat thread
		 * thr[i]=new Thread(new splitObject(map[i], listOfFiles[i].getAbsolutePath()));
		 * 
		 * 
		 * } for (int i = 0; i < nfiles; i++) { // parar thread }
		 * 
		 * // Segundos threads for (int i = 0; i < nreducers; i++) { // creat thread }
		 * for (int i = 0; i < nreducers; i++) { // parar thread }
		 */

		return (Error.COk);
	}

	public class Fases_Concurentes_1 implements Runnable {
		Map map;
		String splitPath;

		public Fases_Concurentes_1(Map map, String splitPath) {
			this.map = map;
			this.splitPath = splitPath;
		}

		@Override
		public void run() {
			// System.out.println("***** Test ****");
			// System.out.println("HHHHHHHHH --- Child Thread ThreadId: ");
			this.map.ReadFileTuples(this.splitPath);

			// System.out.println("***** Read Done ****");
			// this.MapReduce.Split("ww");
			Maps(map);
			Suffle(map);

			// map ordenar palabras lineas
			// Suffle contar palabras

		}
	}

	public class Fases_Concurentes_2 implements Runnable {
		int num_reducer;

		public Fases_Concurentes_2(int num_reducer) {
			this.num_reducer = num_reducer;
		}

		@Override
		public void run() {
			//System.out.println("Fase 2 -->" + Thread.currentThread().getId());
			Reduces(num_reducer);
		}
	}

	// Genera y lee diferentes splits: 1 split por fichero.
	// Versión secuencial: asume que un único Map va a procesar todos los splits.
	private Error Split(String input) {
		File folder = new File(input);
		Thread thr[];

		if (folder.isDirectory()) {
			File[] listOfFiles = folder.listFiles();
			thr = new Thread[listOfFiles.length];
			Map map[] = new Map[listOfFiles.length];
			nfiles = listOfFiles.length;
			System.out.println("nfiles: " + nfiles);
			/* Read all the files and directories within directory */
			for (int i = 0; i < listOfFiles.length; i++) {
				if (listOfFiles[i].isFile()) {
					map[i] = new Map(this);
					AddMap(map[i]);
					thr[i] = new Thread(new Fases_Concurentes_1(map[i], listOfFiles[i].getAbsolutePath()));
					System.out.println("Processing input file " + listOfFiles[i].getAbsolutePath() + ".");
					thr[i].start();
				} else if (listOfFiles[i].isDirectory()) {
					System.out.println("Directory " + listOfFiles[i].getName());
				}
			}

			for (int i = 0; i < listOfFiles.length; i++) {
				try {
					thr[i].join();
				} catch (InterruptedException e) { // Fin //
				}
				System.out.println("Thread number " + thr[i].getId() + " is alive: " + thr[i].isAlive());
			}
		} else {
			thr = new Thread[1];
			thr[0] = new Thread(new Fases_Concurentes_1(new Map(this), folder.getAbsolutePath()));
			System.out.println("Processing input file " + folder.getAbsolutePath() + ".");
			thr[0].start();
			try {
				thr[1].join();
			} catch (InterruptedException e) { // Fin //
			}
		}
		return (Error.COk);
	}

	// Ejecuta cada uno de los Maps.
	private Error Maps(Map map) {

		if (map.Run() != Error.COk)
			Error.showError("MapReduce::Map Run error.\n");

		return (Error.COk);
	}

	public Error Map(Map map, MapInputTuple tuple) {
		System.err.println("MapReduce::Map -> ERROR map must be override.");
		return (Error.CError);
	}

	// Ordena y junta todas las tuplas de salida de los maps. Utiliza una función de
	// hash como
	// función de partición, para distribuir las claves entre los posibles reducers.
	// Utiliza un multimap para realizar la ordenación/unión.
	private Error Suffle(Map map) {

		for (String key : map.GetOutput().keySet()) {
			// Calcular a que reducer le corresponde está clave:
			// System.out.println("key.hashCode() " + key +" "+ key.hashCode() + "
			// Reducers.size() " + Reducers.size() + " >>>> "+
			// Thread.currentThread().getId());

			int r = Math.abs(key.hashCode()) % Reducers.size();

			// r no puede ser negativo, ya que el numero de reducer son 0, 1, 2, 3...

			if (MapReduce.DEBUG) { //
				System.err.println("DEBUG::MapReduce::Suffle merge key " + key + " to reduce " + r);
				// System.out.println("DEBUG::MapReduce::Suffle merge key " + key + " to reduce
				// " + r + " >>>> " + Thread.currentThread().getId());
				// Añadir todas las tuplas de la clave al reducer correspondiente.
			}
			Reducers.get(r).AddInputKeys(key, map.GetOutput().get(key));
		}
		map.GetOutput().clear();
		return (Error.COk);
	}

	// Ejecuta cada uno de los Reducers.
	private Error Reduces(int num_reducer) {
		if (Reducers.get(num_reducer).Run() != Error.COk)
			Error.showError("MapReduce::Reduce Run error.\n");
		return (Error.COk);
	}

	public Error Reduce(Reduce reduce, String key, Collection<Integer> values) {
		System.err.println("MapReduce::Reduce  -> ERROR Reduce must be override.");
		return (Error.CError);
	}
}
