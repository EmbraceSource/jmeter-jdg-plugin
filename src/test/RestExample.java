package test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Rest example accessing Infinispan Cache.
 * 
 * @author Samuel Tauil (samuel@redhat.com)
 * 
 */
public class RestExample {

	/**
	 * Method that puts a String value in cache.
	 * 
	 * @param urlServerAddress
	 * @param value
	 * @throws IOException
	 */
	public void putMethod(String urlServerAddress, String value)
			throws IOException {
		System.out.println("----------------------------------------");
		System.out.println("Executing PUT");
		System.out.println("----------------------------------------");
		URL address = new URL(urlServerAddress);
		System.out.println("executing request " + urlServerAddress);
		HttpURLConnection connection = (HttpURLConnection) address
				.openConnection();
		System.out.println("Executing put method of value: " + value);
		connection.setRequestMethod("PUT");
		connection.setRequestProperty("Content-Type", "text/plain");
		connection.setDoOutput(true);

		OutputStreamWriter outputStreamWriter = new OutputStreamWriter(
				connection.getOutputStream());
		outputStreamWriter.write(value);

		connection.connect();
		outputStreamWriter.flush();

		System.out.println("----------------------------------------");
		System.out.println(connection.getResponseCode() + " "
				+ connection.getResponseMessage());
		System.out.println("----------------------------------------");

		connection.disconnect();
	}

	/**
	 * Method that gets an value by a key in url as param value.
	 * 
	 * @param urlServerAddress
	 * @return String value
	 * @throws IOException
	 */
	public String getMethod(String urlServerAddress) throws IOException {
		String line = new String();
		StringBuilder stringBuilder = new StringBuilder();

		System.out.println("----------------------------------------");
		System.out.println("Executing GET");
		System.out.println("----------------------------------------");

		URL address = new URL(urlServerAddress);
		System.out.println("executing request " + urlServerAddress);

		HttpURLConnection connection = (HttpURLConnection) address
				.openConnection();
		connection.setRequestMethod("GET");
		connection.setRequestProperty("Content-Type", "text/plain");
		connection.setDoOutput(true);

		BufferedReader bufferedReader = new BufferedReader(
				new InputStreamReader(connection.getInputStream()));

		connection.connect();

		while ((line = bufferedReader.readLine()) != null) {
			stringBuilder.append(line + '\n');
		}

		System.out.println("Executing get method of value: "
				+ stringBuilder.toString());

		System.out.println("----------------------------------------");
		System.out.println(connection.getResponseCode() + " "
				+ connection.getResponseMessage());
		System.out.println("----------------------------------------");

		connection.disconnect();

		return stringBuilder.toString();
	}

	/**
	 * Main method example.
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// Attention to the cache name "cacheX" it was configured in xml file
		// with tag <namedCache name="cacheX">
		RestExample restExample = new RestExample();
//		restExample.putMethod("http://192.168.0.103:8080/rest/left/1a",
//				"Infinispan REST Test");
		restExample.getMethod("http://192.168.0.103:8080/rest/left/2");
	}
}