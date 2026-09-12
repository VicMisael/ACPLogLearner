package ufc.victor.experiment;

public enum AlpsFeatureProfile {
    GENERIC(false, false),
    GENERIC_WITH_NODE_IDENTITY(false, true),
    GENERIC_WITH_WORKLOAD(true, false),
    COMPLETE(true, true);

    private final boolean workloadAware;
    private final boolean nodeAware;

    AlpsFeatureProfile(boolean workloadAware, boolean nodeAware) {
        this.workloadAware = workloadAware;
        this.nodeAware = nodeAware;
    }

    public boolean includesWorkloadFeatures() {
        return workloadAware;
    }

    public boolean includesNodeIdentity() {
        return nodeAware;
    }
}
