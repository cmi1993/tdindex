package cn.edu.scnu.dtindex.model;

public class PatitionsInfo {
    private long partitionid;//分区编号
    private ScopeInfo xscope;//x轴的范围
    private ScopeInfo yscope;//y轴的范围

    public PatitionsInfo(ScopeInfo xscope, ScopeInfo yscope) {
        this.xscope = xscope;
        this.yscope = yscope;
    }

    /**
     * 判断某个时间点是否属于该分区
     * @param point 某个时间点
     * @return 布尔值
     */
    public boolean isBelongToThisPatition(ValidTime point){
        return (xscope.isBelongToThisScope(point.getStart())&&yscope.isBelongToThisScope(point.getEnd()))?true:false;
    }

    public long getPartitionid() {
        return partitionid;
    }

    public void setPartitionid(long partitionid) {
        this.partitionid = partitionid;
    }

    public ScopeInfo getXscope() {
        return xscope;
    }

    public void setXscope(ScopeInfo xscope) {
        this.xscope = xscope;
    }

    public ScopeInfo getYscope() {
        return yscope;
    }

    public void setYscope(ScopeInfo yscope) {
        this.yscope = yscope;
    }
}
