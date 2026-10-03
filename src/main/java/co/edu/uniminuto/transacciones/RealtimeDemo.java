package co.edu.uniminuto.transacciones;
import org.apache.storm.*;
import org.apache.storm.topology.*;
import org.apache.storm.topology.base.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.*;
import org.apache.storm.tuple.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
public final class RealtimeDemo {
 static volatile CountDownLatch done;
 static final Set<String> completed=ConcurrentHashMap.newKeySet();
 public static class TransactionsSpout extends BaseRichSpout {
  private SpoutOutputCollector collector;private int cursor;private String runId;
  private final Map<String,Values> pending=new HashMap<>();
  @Override public void open(Map<String,Object> conf,TopologyContext ctx,SpoutOutputCollector c){collector=c;runId=(String)conf.get("demo.runId");}
  @Override public void nextTuple(){
   if(cursor<3){String id=runId+"-"+cursor;int amount=new int[]{150,2500,-10}[cursor++];Values v=new Values(id,amount);pending.put(id,v);collector.emit(v,id);}
   else org.apache.storm.utils.Utils.sleep(50);
  }
  @Override public void ack(Object id){pending.remove(id.toString());if(completed.add(id.toString()))done.countDown();}
  @Override public void fail(Object id){Values v=pending.get(id.toString());if(v!=null)collector.emit(v,id);}
  @Override public void declareOutputFields(OutputFieldsDeclarer d){d.declare(new Fields("id","monto"));}
 }
 public static class ValidationBolt extends BaseBasicBolt {
  @Override public void execute(Tuple t,BasicOutputCollector c){int amount=t.getIntegerByField("monto");String state=amount<=0?"RECHAZADA":amount>=2000?"ALERTA":"APROBADA";c.emit(new Values(t.getStringByField("id"),amount,state));}
  @Override public void declareOutputFields(OutputFieldsDeclarer d){d.declare(new Fields("id","monto","estado"));}
 }
 public static class DatabaseBolt extends BaseRichBolt {
  private OutputCollector collector;
  @Override public void prepare(Map<String,Object> conf,TopologyContext ctx,OutputCollector c){collector=c;}
  @Override public void execute(Tuple t){
   try(Connection db=DbConfig.open()) {
    db.setAutoCommit(false);
    try(PreparedStatement p=db.prepareStatement("INSERT IGNORE INTO transacciones_tiempo_real(id,monto,estado) VALUES(?,?,?)")) {
     p.setString(1,t.getStringByField("id"));p.setInt(2,t.getIntegerByField("monto"));p.setString(3,t.getStringByField("estado"));p.executeUpdate();db.commit();
    } catch(Exception e){db.rollback();throw e;}
    System.out.println("STORM id="+t.getStringByField("id")+" monto="+t.getIntegerByField("monto")+" estado="+t.getStringByField("estado")+" COMMIT MySQL");
    collector.ack(t);
   } catch(Exception e){collector.reportError(e);collector.fail(t);}
  }
  @Override public void declareOutputFields(OutputFieldsDeclarer d){}
 }
 public static void run() throws Exception {
  done=new CountDownLatch(3);completed.clear();String runId=UUID.randomUUID().toString();
  TopologyBuilder b=new TopologyBuilder();b.setSpout("transacciones",new TransactionsSpout(),1);
  b.setBolt("validacion",new ValidationBolt(),1).shuffleGrouping("transacciones");
  b.setBolt("persistencia",new DatabaseBolt(),1).fieldsGrouping("validacion",new Fields("id"));
  Config conf=new Config();conf.setDebug(false);conf.setNumWorkers(1);conf.setNumAckers(1);conf.setMessageTimeoutSecs(20);conf.put("demo.runId",runId);
  try(LocalCluster cluster=new LocalCluster()) {
   cluster.submitTopology("transacciones-s6",conf,b.createTopology());
   DbConfig.check(done.await(90,TimeUnit.SECONDS),"Storm no confirmo las tres transacciones");
   try(Connection db=DbConfig.open();PreparedStatement p=db.prepareStatement("SELECT COUNT(*) FROM transacciones_tiempo_real WHERE id LIKE ?")) {
    p.setString(1,runId+"-%");try(ResultSet r=p.executeQuery()){r.next();DbConfig.check(r.getInt(1)==3,"Conteo incorrecto en MySQL");}
   }
   System.out.println("STORM VERIFICADO: 3 registros unicos; APROBADA, ALERTA y RECHAZADA");cluster.killTopology("transacciones-s6");
  }
 }
}
