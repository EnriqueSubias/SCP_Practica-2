
public class FirstJavaSecThreads implements Runnable
{
	char Digito;

	public FirstJavaSecThreads(char dig){
		setDigito(dig);
	}

	public void setDigito(char dig){
		Digito = dig;
	}

	public char getDigito(){
		return (Digito);
	}

	@Override
	public void run()
	{
		System.out.println("Child Thread ThreadId: " + Thread.currentThread().getId());
		for (int i = 0; i < 200; i++) {
			System.out.print(getDigito());
		}
	}

	static public void main(String[] args)
	{
		System.out.println("Main ThreadId: " + Thread.currentThread().getId());

		//new FirstJavaSec('1').run();
		//new FirstJavaSec('2').run();

		new Thread(new FirstJavaSecThreads('1')).start();
		new Thread(new FirstJavaSecThreads('2')).start();

		System.out.println("Main ThreadId: " + Thread.currentThread().getId() + "-> End Main Thread");
	}

}
