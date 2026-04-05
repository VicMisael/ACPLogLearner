package ufc.victor;

import com.yahoo.labs.samoa.instances.*;
import moa.classifiers.trees.HoeffdingTree;
import ufc.victor.protocol.SelectedProtocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

public class VFDTProtocolPredictor {

    private final HoeffdingTree vfdt;
    private final Instances datasetHeader;
    private final double explorationRate;

    private final SelectedProtocol[] protocols = {
            SelectedProtocol.TWO_PC,
            SelectedProtocol.TWO_PC_PRESUMED_ABORT,
            SelectedProtocol.TWO_PC_PRESUMED_COMMIT
    };

    public VFDTProtocolPredictor() {
        this(0.10);
    }

    public VFDTProtocolPredictor(double explorationRate) {
        this.explorationRate = explorationRate;

        // 1. Define the Schema (Features + Label)
        ArrayList<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("avg_network_latency_ms"));
        attributes.add(new Attribute("max_network_latency_ms"));
        attributes.add(new Attribute("avg_disk_io_time_ms"));
        attributes.add(new Attribute("max_disk_io_time_ms"));
        attributes.add(new Attribute("abort_rate"));
        attributes.add(new Attribute("participant_count"));

        ArrayList<String> classLabels = new ArrayList<>();
        for (SelectedProtocol p : protocols) classLabels.add(p.name());
        attributes.add(new Attribute("best_protocol", classLabels));

        this.datasetHeader = new Instances("ProtocolData", attributes, 0);
        this.datasetHeader.setClassIndex(6); // The label is the last attribute

        // 2. Initialize the Hoeffding Tree
        this.vfdt = new HoeffdingTree();
        this.vfdt.gracePeriodOption.setValue(5);
        this.vfdt.splitConfidenceOption.setValue(1.0e-3f);
        this.vfdt.tieThresholdOption.setValue(0.05f);
        this.vfdt.setModelContext(new InstancesHeader(datasetHeader));
        this.vfdt.prepareForUse();
    }

    /**
     * Called when a new transaction arrives.
     */
    public SelectedProtocol predict(LogFeatures features) {
        // Explore: Gather new data randomly
        if (ThreadLocalRandom.current().nextDouble() < explorationRate) {
            return protocols[ThreadLocalRandom.current().nextInt(protocols.length)];
        }

        // Exploit: Ask the VFDT
        Instance inst = createInstance(features, null);
        double[] votes = vfdt.getVotesForInstance(inst);

        // Find the protocol with the highest probability
        int bestIndex = 0;
        for (int i = 1; i < votes.length; i++) {
            if (votes[i] > votes[bestIndex]) bestIndex = i;
        }

        return protocols[bestIndex];
    }

    /**
     * Called AFTER a transaction finishes to update the tree's math.
     */
    public void train(LogFeatures features, SelectedProtocol optimalProtocol) {
        Instance inst = createInstance(features, optimalProtocol);
        vfdt.trainOnInstance(inst);
    }

    private Instance createInstance(LogFeatures features, SelectedProtocol label) {
        DenseInstance inst = new DenseInstance(7);
        inst.setWeight(1.0); // Safety net for MOA
        inst.setDataset(datasetHeader);
        inst.setValue(0, features.averageNetworkLatencyMs());
        inst.setValue(1, features.maxNetworkLatencyMs());
        inst.setValue(2, features.averageDiskIoTimeMs());
        inst.setValue(3, features.maxDiskIoTimeMs());
        inst.setValue(4, features.abortRate());
        inst.setValue(5, features.participantCount());

        if (label != null) {
            int classIndex = Arrays.asList(protocols).indexOf(label);
            inst.setClassValue(classIndex);
        }
        return inst;
    }

    // For your Thesis Defense!
    public String getLearnedRules() {
        StringBuilder sb = new StringBuilder();
        vfdt.getModelDescription(sb, 0);
        return sb.toString();
    }

    public int getNodeCount() {
        return vfdt.getNodeCount();
    }

    public int getTreeDepth() {
        return vfdt.measureTreeDepth();
    }
}
