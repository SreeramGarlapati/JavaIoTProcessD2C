package com.mycompany.app;

import com.microsoft.azure.iothub.DeviceClient;
import com.microsoft.azure.iothub.IotHubClientProtocol;
import com.microsoft.azure.iothub.Message;
import com.microsoft.azure.iothub.IotHubStatusCode;
import com.microsoft.azure.iothub.IotHubEventCallback;
import com.google.gson.Gson;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

public class App {
//  private static String connString = "connString";
//  private static String deviceId = "myFirstJavaDevice";
  private static String connString = "connString";
  private static String deviceId = "myFirstDevice";
  private static DeviceClient client;
  private static IotHubClientProtocol protocol = IotHubClientProtocol.AMQPS;

  private static class TelemetryDataPoint {
    public String deviceId;
    public double windSpeed;

    public String serialize() {
      Gson gson = new Gson();
      return gson.toJson(this);
    }
  }

  private static class EventCallback implements IotHubEventCallback {
    public void execute(IotHubStatusCode status, Object context) {
      System.out.println("IoT Hub responded to message with status: "
        + status.name());

      if (context != null) {
        synchronized (context) {
          context.notify();
        }
      }
    }
  }

  private static class MessageSender implements Runnable {
    public void run() {
      try {
        double avgWindSpeed = 10; // m/s
        Random rand = new Random();

        while (true) {
          double currentWindSpeed = avgWindSpeed + rand.nextDouble() * 4 - 2;
          TelemetryDataPoint telemetryDataPoint = new TelemetryDataPoint();
          telemetryDataPoint.deviceId = deviceId;
          telemetryDataPoint.windSpeed = currentWindSpeed;

          String msgStr = telemetryDataPoint.serialize();
          Message msg = new Message(msgStr);
          System.out.println("Sending: " + msgStr);

          Object lockobj = new Object();
          EventCallback callback = new EventCallback();
          client.sendEventAsync(msg, callback, lockobj);

          synchronized (lockobj) {
            lockobj.wait();
          }
          Thread.sleep(1000);
        }
      } catch (InterruptedException e) {
        System.out.println("Finished.");
      }
    }
  }
  
  private static class InteractiveMessageSender implements Runnable {
    public void run() {
      try {
        while (true) {
          String msgStr = "Alert message!";
          Message msg = new Message(msgStr);
          msg.setMessageId(java.util.UUID.randomUUID().toString());
          msg.setProperty("messageType", "interactive");
          System.out.println("Sending interactive message: " + msgStr);

          Object lockobj = new Object();
          EventCallback callback = new EventCallback();
          client.sendEventAsync(msg, callback, lockobj);

          synchronized (lockobj) {
            lockobj.wait();
          }
          Thread.sleep(10000);
        }
      } catch (InterruptedException e) {
        System.out.println("Finished sending interactive messages.");
      }
    }
  }

  public static void main(String[] args) throws IOException, URISyntaxException {
    client = new DeviceClient(connString, protocol);
    client.open();

    MessageSender sender = new MessageSender();
    InteractiveMessageSender interactiveSender = new InteractiveMessageSender();

    ExecutorService executor = Executors.newFixedThreadPool(2);
    executor.execute(sender);
    executor.execute(interactiveSender);

    System.out.println("Press ENTER to exit.");
    System.in.read();
    executor.shutdownNow();
    client.close();
  }
}
