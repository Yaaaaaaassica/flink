package io.github.spafka;

public class App
{
	public static void main( String[] args ) {
		for(int i = 0; i < 100000; i++) {
			System.out.println(produceString());
		}
	}

	private static String produceString() {
		return "Hello World";
	}
}
