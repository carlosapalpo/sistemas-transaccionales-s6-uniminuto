package co.edu.uniminuto.transacciones;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import javax.jms.*;
import java.util.UUID;
public final class MessagingDemo {
 public static void run() throws Exception {
  BrokerService broker=new BrokerService();broker.setBrokerName("semana6");broker.setPersistent(true);broker.setUseJmx(false);
  broker.setDataDirectory("data/activemq");broker.setSchedulerSupport(false);broker.start();broker.waitUntilStarted();
  Connection connection=null;
  try {
   connection=new ActiveMQConnectionFactory("vm://semana6?create=false").createConnection();connection.start();
   Session producerSession=connection.createSession(true,Session.SESSION_TRANSACTED);
   String id=UUID.randomUUID().toString();
   Queue queue=producerSession.createQueue("pedidos.confirmados");
   MessageProducer producer=producerSession.createProducer(queue);producer.setDeliveryMode(DeliveryMode.PERSISTENT);
   TextMessage message=producerSession.createTextMessage("Pedido confirmado para el producto 1");message.setStringProperty("eventId",id);
   producer.send(message);producerSession.commit();System.out.println("ACTIVEMQ ENVIADO id="+id+" cola=pedidos.confirmados");
   Session consumerSession=connection.createSession(true,Session.SESSION_TRANSACTED);
   MessageConsumer consumer=consumerSession.createConsumer(queue);
   Message received=consumer.receive(10000);
   DbConfig.check(received!=null,"No se recibio el mensaje");
   System.out.println("ACTIVEMQ RECIBIDO: "+((TextMessage)received).getText());
   consumerSession.rollback();System.out.println("ACTIVEMQ ROLLBACK: se solicita la reentrega");
   received=consumer.receive(10000);DbConfig.check(received!=null && received.getJMSRedelivered(),"Falta reentrega JMS");
   DbConfig.event(received.getStringProperty("eventId"),"ACTIVEMQ",((TextMessage)received).getText());
   consumerSession.commit();System.out.println("ACTIVEMQ REENTREGA=true; registro MySQL y COMMIT JMS completados");
   producerSession.close();consumerSession.close();
  } finally {if(connection!=null)connection.close();broker.stop();broker.waitUntilStopped();}
 }
}
