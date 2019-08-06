/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hbasecompaction.hbasecompaction;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.crypto.dom.DOMStructure;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;

/**
 *
 * @author msumbul
 */
public class compactBigTable {

    public static void main(String[] args) throws Exception {
        doRunCompactionRegionByRegion(args[0], Integer.valueOf(args[1]));
    }

    public void compact(String tableName, Integer concurrency) {
        Configuration config = HBaseConfiguration.create();

        String path = this.getClass()
                .getClassLoader()
                .getResource("hbase-site.xml")
                .getPath();
        config.addResource(new Path(path));

        try {
            HBaseAdmin.checkHBaseAvailable(config);
            Connection connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin();
            TableName t = TableName.valueOf(tableName);
            List<HRegionInfo> l_HRegionTable_toCompact = admin.getTableRegions(t);
            List<HRegionInfo> l_HRegionTable_Compacting = new ArrayList<>();
            List<HRegionInfo> l_HRegionTable_Compacted = new ArrayList<>();

            while (l_HRegionTable_toCompact.size() > 0) {

                if ((l_HRegionTable_Compacting.size() < concurrency
                        || l_HRegionTable_toCompact.size() < concurrency)
                        && l_HRegionTable_toCompact.size() > 0) {

                    HRegionInfo HR = l_HRegionTable_toCompact.get(0);
                    l_HRegionTable_Compacting.add(HR);
                    admin.compactRegion(HR.getEncodedNameAsBytes());
                    l_HRegionTable_toCompact.remove(HR);
                }

                for (HRegionInfo HRCompacting : l_HRegionTable_Compacting) {
                    if (admin.getCompactionStateForRegion(HRCompacting.getEncodedNameAsBytes()).equals(CompactionState.NONE)) {

                        l_HRegionTable_Compacted.add(HRCompacting);
                        l_HRegionTable_Compacting.remove(HRCompacting);

                    }
                }
                waitSeconds(1);

            }

        } catch (ZooKeeperConnectionException ex) {
            Logger.getLogger(compactBigTable.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ServiceException | IOException ex) {
            Logger.getLogger(compactBigTable.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void doRunCompactionRegionByRegion(String tableName, Integer concurrency) {
        try {
            Configuration config = HBaseConfiguration.create();

            String path = "/usr/hdp/current/hbase-client/conf/hbase-site.xml";
            config.addResource(new Path(path));

            HBaseAdmin.checkHBaseAvailable(config);
            Connection connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin();
            TableName t = TableName.valueOf(tableName);
            List<HRegionInfo> l_HRegionTable = admin.getTableRegions(t);
            
            List<region> l_HRegionTable_toCompact = new ArrayList<>();
            
            for(HRegionInfo hr : l_HRegionTable){
                l_HRegionTable_toCompact.add(new region(hr));
                System.out.println("Region listed for compaction (regionnameasstring) \t" + hr.getRegionNameAsString());

            }
            

            List<region> l_HRegionTable_Compacting = new ArrayList<>();
            List<region> l_HRegionTable_Compacted = new ArrayList<>();
            
            while (l_HRegionTable_toCompact.size() > 0 || l_HRegionTable_Compacting.size() > 0) {
                
                  if ((l_HRegionTable_Compacting.size() < concurrency
                        || l_HRegionTable_toCompact.size() < concurrency)
                        && l_HRegionTable_toCompact.size() > 0) {
                      
                     region r =  l_HRegionTable_toCompact.get(0);
                     r.doMajorCompact(admin);
                     
                      l_HRegionTable_Compacting.add(r);
                      l_HRegionTable_toCompact.remove(r);
                      System.out.println("Start compacting " + r.getHRInfo().getRegionNameAsString());
                      
                  }

                   for(int i = (l_HRegionTable_Compacting.size() -1); i >= 0; i--){
                       region r = l_HRegionTable_Compacting.get(i);
                       if(r.checkEndMajorCompact(admin)){
                           
                           l_HRegionTable_Compacted.add(r);
                           l_HRegionTable_Compacting.remove(r);
                           long duration = r.getFinishedMajorCompactionTime() - r.getStartMajorCompactionTime();
                           System.out.println("Finished compacting after " + duration + " ms \t" + r.getHRInfo().getRegionNameAsString());
                       }
                   }
                   System.out.println("Number of regions compacted " + l_HRegionTable_Compacted.size() + " over " + (l_HRegionTable_toCompact.size() + l_HRegionTable_Compacting.size() + l_HRegionTable_Compacted.size()));
                   
                waitSeconds(1);
                
            }
            
            
            
            

        } catch (ZooKeeperConnectionException ex) {
            Logger.getLogger(compactBigTable.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ServiceException | IOException ex) {
            Logger.getLogger(compactBigTable.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void waitSeconds(Integer s) {
        try {
            TimeUnit.SECONDS.sleep(s);
        } catch (InterruptedException ex) {
            Logger.getLogger(compactBigTable.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
