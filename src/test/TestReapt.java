package test;

import java.io.IOException;

import org.infinispan.marshall.Marshaller;
import org.infinispan.marshall.jboss.GenericJBossMarshaller;

public class TestReapt {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Marshaller marshaller=new GenericJBossMarshaller();
		
		System.out.println(marshaller.objectToByteBuffer("123", 2).length);
		System.out.println(marshaller.objectToByteBuffer("123", 64).length);
		System.out.println(marshaller.objectToByteBuffer("123", 128).length);


		
		System.out.println(marshaller.objectFromByteBuffer(marshaller.objectToByteBuffer("123", 63)));
		System.out.println(marshaller.objectFromByteBuffer(marshaller.objectToByteBuffer("123", 64)));
		System.out.println(marshaller.objectFromByteBuffer(marshaller.objectToByteBuffer("123", 65)));
	}
}
