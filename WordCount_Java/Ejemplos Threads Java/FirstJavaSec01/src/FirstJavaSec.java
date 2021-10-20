
public class FirstJavaSec extends Thread
{
	int Digito;
	int Digito2;

	public FirstJavaSec(int dig, int dig2){
		setDigito(dig2);
	}

	public FirstJavaSec(){
		setDigito(7);
	}

	public void setDigito(int dig){
		Digito = dig;
	}
	
	public void test1(int dig, int dig2){
		setDigito(dig2);
	}
	public int getDigito() {
		return (Digito);
	}
	
	@Override
	public void run() {
		
		for(int i=0; i<50; i++)
		{
			System.out.println(
					"Child Thread ThreadId: " + Thread.currentThread().getId() + " -" + i + "-> " + getDigito());
		}
		super.run();
	}

	static public void main(String[] args) {
		System.out.println("Main ThreadId: " + Thread.currentThread().getId());

		//for(int i=0; i<3; i++) {
			new FirstJavaSec(1,5).start();
			//new Thread(new FirstJavaSec('2')).start();
			// De nuevo, No invocar directamente a run!
			// si se quiere tener un hilo concurrente
			new FirstJavaSec().start();
		//}
			//PrintNumbers.printNumbers();
	}
}
