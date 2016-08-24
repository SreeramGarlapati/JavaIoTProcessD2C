package com.mycompany.app;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.*;
import com.microsoft.windowsazure.services.servicebus.models.*;

/**
 * Hello world!
 *
 */
public class App {
  // ServiceBus
  private final static String serviceBusNamespace = "serviceBusNamespace";
  private final static String serviceBusSasKeyName = "listen";
  private final static String serviceBusSASKey = "serviceBusSASKey";
  private final static String serviceBusRootUri = ".servicebus.windows.net";
  private final static String queueName = "d2ctutorial";
  private static ServiceBusContract service = null;

  private static class MessageReceiver implements Runnable {
	public void run() {
	  ReceiveMessageOptions opts = ReceiveMessageOptions.DEFAULT;
	  //opts.setReceiveMode(ReceiveMode.RECEIVE_AND_DELETE);

	  try {
		while (true) {
		  ReceiveQueueMessageResult resultQM = service.receiveQueueMessage(
			  queueName, opts);
		  BrokeredMessage message = resultQM.getValue();
		  if (message != null && message.getMessageId() != null) {
			System.out.println("MessageID: " + message.getMessageId());
			// Display the queue message.
			System.out.print("From queue: ");
			byte[] b = new byte[200];
			String s = null;
			int numRead = message.getBody().read(b);
			while (-1 != numRead) {
			  s = new String(b);
			  s = s.trim();
			  System.out.print(s);
			  numRead = message.getBody().read(b);
			}
			System.out.println();
			//service.deleteMessage(message);
		  } else {
			Thread.sleep(1000);
		  }
		}
	  } catch (InterruptedException e) {
		System.out.println("Finished.");
	  } catch (ServiceException e) {
		System.out.println("ServiceException: " + e.getMessage());
	  } catch (IOException e) {
		System.out.println("IOException: " + e.getMessage());
	  }
	}
  }

  public static void main(String args[]) throws ServiceException, IOException {
	System.out.println("Process interactive messages");

	// Setup ServiceBus queue
	Configuration config = ServiceBusConfiguration
		.configureWithSASAuthentication(serviceBusNamespace,
			serviceBusSasKeyName, serviceBusSASKey, serviceBusRootUri);
	service = ServiceBusService.create(config);

	MessageReceiver receiver = new MessageReceiver();

	ExecutorService executor = Executors.newFixedThreadPool(2);
	executor.execute(receiver);

	System.out.println("Press ENTER to exit.");
	System.in.read();
	executor.shutdownNow();
  }
}
