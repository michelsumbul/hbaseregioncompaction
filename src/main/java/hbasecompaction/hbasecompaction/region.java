/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hbasecompaction.hbasecompaction;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;

/**
 *
 * @author msumbul
 */
public class region {

    private HRegionInfo HRInfo;
    private long StartMajorCompactionTime;
    private long FinishedMajorCompactionTime;
    private String Status;

    public region(HRegionInfo HR) {
        this.setHRInfo(HR);
    }

    public void doMajorCompact(Admin admin) {
        try {

            admin.majorCompactRegion(HRInfo.getRegionName());

            this.setStartMajorCompactionTime(dateUtil.getNowUnixtimeMilli());
        } catch (IOException ex) {
            Logger.getLogger(region.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public boolean checkEndMajorCompact(Admin admin) {

        try {
            System.out.println(dateUtil.getNow("dd/MM/yyyy hh:mm:ss") + "\tDEBUG: \t state \t " + admin.getCompactionStateForRegion(HRInfo.getEncodedNameAsBytes()) + " \t region:" + HRInfo.getRegionNameAsString());
            System.out.println(dateUtil.getNow("dd/MM/yyyy hh:mm:ss") + "\tDEBUG: \t starttime \t " + this.getStartMajorCompactionTime() + " \t region:" + HRInfo.getRegionNameAsString());
            System.out.println(dateUtil.getNow("dd/MM/yyyy hh:mm:ss") + "\tDEBUG: \t endtime \t " + admin.getLastMajorCompactionTimestampForRegion(HRInfo.getRegionName()) + " \t region:" + HRInfo.getRegionNameAsString());
            System.out.println("Region compacting since " + (dateUtil.getNowUnixtimeMilli() - this.getStartMajorCompactionTime()) + " ms " + HRInfo.getRegionNameAsString());

            if (admin.getCompactionStateForRegion(HRInfo.getRegionName()).equals(CompactionState.NONE)
                    && admin.getLastMajorCompactionTimestampForRegion(HRInfo.getRegionName()) > this.getStartMajorCompactionTime()) {
                this.setFinishedMajorCompactionTime(admin.getLastMajorCompactionTimestampForRegion(HRInfo.getRegionName()));
                return true;
            }

        } catch (IOException ex) {
            Logger.getLogger(region.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }

    /**
     * @return the HRInfo
     */
    public HRegionInfo getHRInfo() {
        return HRInfo;
    }

    /**
     * @param HRInfo the HRInfo to set
     */
    public final void setHRInfo(HRegionInfo HRInfo) {
        this.HRInfo = HRInfo;
    }

    /**
     * @return the StartMajorCompactionTime
     */
    public long getStartMajorCompactionTime() {
        return StartMajorCompactionTime;
    }

    /**
     * @param StartMajorCompactionTime the StartMajorCompactionTime to set
     */
    public void setStartMajorCompactionTime(long StartMajorCompactionTime) {
        this.StartMajorCompactionTime = StartMajorCompactionTime;
    }

    /**
     * @return the FinishedMajorCompactionTime
     */
    public long getFinishedMajorCompactionTime() {
        return FinishedMajorCompactionTime;
    }

    /**
     * @param FinishedMajorCompactionTime the FinishedMajorCompactionTime to set
     */
    public void setFinishedMajorCompactionTime(long FinishedMajorCompactionTime) {
        this.FinishedMajorCompactionTime = FinishedMajorCompactionTime;
    }

    /**
     * @return the Status
     */
    public String getStatus() {
        return Status;
    }

    /**
     * @param Status the Status to set
     */
    public void setStatus(String Status) {
        this.Status = Status;
    }

}
