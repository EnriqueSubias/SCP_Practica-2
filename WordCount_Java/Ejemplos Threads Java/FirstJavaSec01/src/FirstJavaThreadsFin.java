
public class FirstJavaThreadsFin implements Runnable 
{
	char Digito;
	boolean cancel = false;
	Thread thisThread = null;
	private final int interval = 10;

	public FirstJavaThreadsFin(char dig){
		setDigito(dig);
	}

	public void setDigito(char dig){
		Digito = dig;
	}
	
	public char getDigito(){
		return (Digito);
	}

	public void cancel(){
		cancel=true;
		thisThread.interrupt();
	}

	@Override
	public void run() 
	{
		thisThread = Thread.currentThread();
		System.out.println("Child Thread ThreadId: " + thisThread.getId());
		//for (int i = 0; cancel == false && !thisThread.interrupted(); i++)
		while (cancel == false && !Thread.interrupted())
		//for (int i = 0; i < 200; i++)		
		{
			try {
				Thread.sleep(interval); 
				System.out.print(getDigito());
			}
			catch (InterruptedException e){ 
			} 
		}
	}

	static public void main(String[] args) 
	{
		System.out.println("Main ThreadId: " + Thread.currentThread().getId());
	
		FirstJavaThreadsFin R1 = new FirstJavaThreadsFin('1');
		Thread T1 = new Thread(R1);
		FirstJavaThreadsFin R2 = new FirstJavaThreadsFin('2'); 	
		Thread T2 = new Thread(R2);
		
		T1.start();
		T2.start();

		try {
			System.in.read();
			R1.cancel();
			T1.join();
			R2.cancel();
			T2.join();
		} 
		catch (Exception e) {
    		System.err.println("Join Exception: " + e.getMessage());
    		e.printStackTrace();
    	}
		
		System.out.println("Main ThreadId: " + Thread.currentThread().getId() + "-> End Child Threads");
	}

}
