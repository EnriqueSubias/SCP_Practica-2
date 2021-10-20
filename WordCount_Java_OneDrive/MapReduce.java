import java.io.File;
import java.util.*;

 
abstract class MapReduce 
{
	public static final boolean DEBUG = false;	
	
	private String 	InputPath;
	private String 	OutputPath;
	private int numberOfFiles;
		
	private Vector<Map> Mappers =  new Vector<Map>();
	private Vector<Reduce> Reducers =  new Vector<Reduce>();

	public MapReduce()
	{
		SetInputPath("");
		SetOutputPath("");
	}
		
	// Constructor MapReduce: número de reducers a utilizar. Los parámetros de directorio/fichero entrada 
	// y directorio salida se inicilizan mediante Set* y las funciones Map y reduce sobreescribiendo los
	// métodos abstractos.
	
	public MapReduce(String input, String output, int nReducers)
	{
		SetInputPath(input);
		SetOutputPath(output);
		SetReducers(nReducers);
	}
	
	
	private void AddMap(Map map) 
	{ 
		Mappers.add(map); 
	}
	
	private void AddReduce(Reduce reducer) 
	{ 
		Reducers.add(reducer); 
	}
		
	
	public void SetInputPath(String path) {
		InputPath = path;
	}
	
	public void SetOutputPath(String path) {
		OutputPath = path;
	}
	
	public void SetReducers(int nReducers)
	{
		for(int x=0;x<nReducers;x++)
		{
			AddReduce(new Reduce(this, OutputPath+"/result.r"+(x+1)));
		}
	}

	
	// Procesa diferentes fases del framework mapreduc: split, map, suffle/merge, reduce.
	public Error Run()
	{

		nfiles = getNumberFiles(InputPath);

		if (Split(InputPath)!=Error.COk)
			Error.showError("MapReduce::Run-Error Split");

		if (Maps()!=Error.COk)
			Error.showError("MapReduce::Run-Error Map");

		if (Suffle()!=Error.COk)
			Error.showError("MapReduce::Run-Error Merge");

		if (Reduces()!=Error.COk)
			Error.showError("MapReduce::Run-Error Reduce");

		return(Error.COk);
	}


	public class splitObject implements Runnable{		
		Map map;
		String splitPath;
		public splitObject (Map map, String splitPath){
			this.map = map;
			this.splitPath = splitPath;
		}

		@Override
		public void run(){
			this.map.ReadFileTuples(this.splitPath);
			//map ordenar palabras lineas
			// Suffle contar palabras 
		}
	}

	// Genera y lee diferentes splits: 1 split por fichero.
	// Versión secuencial: asume que un único Map va a procesar todos los splits.
	private Error Split(String input)
	{
		File folder = new File(input);
		Thread thr[];
	
		if (folder.isDirectory()) 
		{
			File[] listOfFiles = folder.listFiles();
			thr = new Thread [listOfFiles.length];
			Map map[] = new Map[listOfFiles.length];
			numberOfFiles=listOfFiles.length;
			/* Read all the files and directories within directory */
		    for (int i = 0; i < listOfFiles.length; i++) 
		    {
		    	if (listOfFiles[i].isFile()) 
		    	{
		    		map[i]=new Map(this);
		    		AddMap(map[i]);
		    		thr[i]=new Thread(new splitObject(map[i], listOfFiles[i].getAbsolutePath()));
		    		System.out.println("Processing input file " + listOfFiles[i].getAbsolutePath() + ".");
		    		thr[i].start();
		    	}
		    	else if (listOfFiles[i].isDirectory()) {
		    		System.out.println("Directory " + listOfFiles[i].getName());
		    	}
		    }
		}
		else
		{
			thr = new Thread [1];
			thr[0]=new Thread(new splitObject(new Map(this), folder.getAbsolutePath()));
			System.out.println("Processing input file " + folder.getAbsolutePath() + ".");
			thr[0].start();
		}
		
		return(Error.COk);
	}
	
	public class mapObject implements Runnable{		
		Map map;
		public mapObject (Map map){
			this.map = map;
		}

		@Override
		public void run(){
			if (this.map.Run()!=Error.COk)
				Error.showError("MapReduce::Map Run error.\n");
		}
	}

	// Ejecuta cada uno de los Maps.
	private Error Maps()
	{	
		for(Map map : Mappers)
		{
			if (MapReduce.DEBUG) System.err.println("DEBUG::Running Map "+ map);
			new Thread(new mapObject(map)).start();
		}
		return(Error.COk);
	}
	
	
	public Error Map(Map map, MapInputTuple tuple)
	{
		System.err.println("MapReduce::Map -> ERROR map must be override.");
		return(Error.CError);
	}
	
	public class suffleObject implements Runnable{		
		Map map;
		public suffleObject (Map map){
			this.map = map;
		}

		@Override
		public void run(){
			if (MapReduce.DEBUG) this.map.PrintOutputs();
			while(this.map.keyQueue.isEmpty())
				this.map.sleep();
			String key = this.map.keyQueue.poll();
			while(!key.equals("-1"))
			{
			    // Calcular a que reducer le corresponde está clave:
				int r = key.hashCode()%Reducers.size();
				if (r<0) r+=Reducers.size();
				if (MapReduce.DEBUG) System.err.println("DEBUG::MapReduce::Suffle merge key " + key +" to reduce " + r);

				// Añadir todas las tuplas de la clave al reducer correspondiente.
				Reducers.get(r).AddInputKey(key, new Integer(1));
				while(this.map.keyQueue.isEmpty())
					this.map.sleep();
				key = this.map.keyQueue.poll();
			}
		}
	}
	
	// Ordena y junta todas las tuplas de salida de los maps. Utiliza una función de hash como 
	// función de partición, para distribuir las claves entre los posibles reducers.
	// Utiliza un multimap para realizar la ordenación/unión.
	private Error Suffle()
	{
		Thread thr[] = new Thread[numberOfFiles];
		int i = 0;
		for(Map map : Mappers)
		{
			thr[i] = new Thread(new suffleObject(map));
			thr[i].start();
			i++;
		}
		for (int j = 0; j < numberOfFiles; j++){
			try{
                thr[j].join();
            }catch(Exception IllegalThreadStateException){
                System.err.println(IllegalThreadStateException);
            }
		}
		return(Error.COk);
	}

	
	private class objectReduce implements Runnable{
		Reduce reduce;
		private objectReduce(Reduce reduce){
			this.reduce = reduce;
		}
		@Override
		public void run (){
			if (this.reduce.Run()!=Error.COk)
				Error.showError("MapReduce::Reduce Run error.\n");
		}
	}


	// Ejecuta cada uno de los Reducers.
	private Error Reduces()
	{
		int j = 0;
		Thread thr[] = new Thread[Reducers.size()];
		for(Reduce reduce : Reducers)
		{
			thr[j] = new Thread(new objectReduce(reduce));
			thr[j].start();
			j++;
		}
		for(int i=0; i<thr.length; i++){
            try{
                thr[i].join();
            }catch(Exception IllegalThreadStateException){
                System.err.println(IllegalThreadStateException);
            }
        }

		return(Error.COk);
	}
	
		
	
	public Error Reduce(Reduce reduce, String key, Collection<Integer> values)
	{
		System.err.println("MapReduce::Reduce  -> ERROR Reduce must be override.");
		return(Error.CError);
	}
}