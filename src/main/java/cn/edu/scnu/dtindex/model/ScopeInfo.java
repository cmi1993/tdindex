package cn.edu.scnu.dtindex.model;

public class ScopeInfo {
    private long scope_start;
    private long scope_end;

    public ScopeInfo(long scope_start, long scope_end) {
        this.scope_start = scope_start<scope_end?scope_start:scope_end;
        this.scope_end = scope_start<scope_end?scope_end:scope_start;
    }

    public boolean isBelongToThisScope(long point){
        return (point>=scope_start&&point<=scope_end)?true:false;
    }

    public long getScope_start() {
        return scope_start;
    }

    public void setScope_start(long scope_start) {
        this.scope_start = scope_start;
    }

    public long getScope_end() {
        return scope_end;
    }

    public void setScope_end(long scope_end) {
        this.scope_end = scope_end;
    }
}
