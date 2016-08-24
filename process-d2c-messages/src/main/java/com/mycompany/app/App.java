package com.mycompany.app;
import com.microsoft.azure.eventprocessorhost.*;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.services.servicebus.ServiceBusConfiguration;
import com.microsoft.windowsazure.services.servicebus.ServiceBusService;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.concurrent.*;


/**
 * Hello world!
 *
 */
public class App 
{
  // Storage account
  private final static String storageAccountName = "storageAccountName";
  private final static String storageAccountName = "storageAccountName";
  private final static String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=" + storageAccountName + ";AccountKey=" + storageAccountKey;

  // ServiceBus
  private final static String serviceBusNamespace = "serviceBusNamespace";
  private final static String serviceBusSasKeyName = "send";
  private final static String serviceBusSASKey = "serviceBusSASKey";
  private final static String serviceBusRootUri = ".servicebus.windows.net";
  public final static String queueName = "d2ctutorial";
  
  // IoT Hub Event Hubs-compatible endpoint
  private final static String consumerGroupName = "$Default";
  private final static String namespaceName = "namespaceName";
  private final static String eventHubName = "eventHubName";
  private final static String sasKeyName = "iothubowner";
  private final static String sasKey = "sasKey";
  
    public static void main(String args[]) throws InvalidKeyException, URISyntaxException, StorageException
    {
        System.out.println("Process D2C messages using EventProcessorHost");
        
        // Setup storage account
    	CloudStorageAccount account = CloudStorageAccount.parse(storageConnectionString);
    	CloudBlobClient client =  account.createCloudBlobClient();
    	EventProcessor.blobContainer = client.getContainerReference("d2cjavatutorial");
    	EventProcessor.blobContainer.createIfNotExists();
    	
    	// Setup ServiceBus queue
    	Configuration config =
	        ServiceBusConfiguration.configureWithSASAuthentication(
	        	serviceBusNamespace,
	        	serviceBusSasKeyName,
	        	serviceBusSASKey,
	            serviceBusRootUri
	         );
		EventProcessor.serviceBusContract = ServiceBusService.create(config);

    	// Setup Event Processor Host
        ConnectionStringBuilder eventHubConnectionString = new ConnectionStringBuilder(namespaceName, eventHubName, sasKeyName, sasKey);
        EventProcessorHost host = new EventProcessorHost(eventHubName, consumerGroupName, eventHubConnectionString.toString(), storageConnectionString);
        EventProcessorOptions options = new EventProcessorOptions();
        options.setExceptionNotification(new ErrorNotificationHandler());
        try
        {
            System.out.println("Registering host named " + host.getHostName());
            host.registerEventProcessor(EventProcessor.class, options).get();
        }
        catch (Exception e)
        {
            System.out.print("Failure while registering: ");
            if (e instanceof ExecutionException)
            {
                Throwable inner = e.getCause();
                System.out.println(inner.toString());
            }
            else
            {
                System.out.println(e.toString());
            }
            System.out.println(e.toString());
        }

        System.out.println("Press enter to stop");
        try
        {
            System.in.read();
            host.unregisterEventProcessor();

            System.out.println("Calling forceExecutorShutdown");
            EventProcessorHost.forceExecutorShutdown(120);
        }
        catch(Exception e)
        {
            System.out.println(e.toString());
            e.printStackTrace();
        }

        System.out.println("End of sample");
    }
}
