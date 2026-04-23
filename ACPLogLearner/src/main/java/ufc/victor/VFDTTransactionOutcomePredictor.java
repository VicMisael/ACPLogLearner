package ufc.victor;

import com.yahoo.labs.samoa.instances.Attribute;
import com.yahoo.labs.samoa.instances.DenseInstance;
import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.Instances;
import com.yahoo.labs.samoa.instances.InstancesHeader;
import moa.classifiers.trees.HoeffdingTree;
import ufc.victor.experiment.TransactionOutcomeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class VFDTTransactionOutcomePredictor {

    private final HoeffdingTree vfdt;
    private final Instances datasetHeader;
    private final List<String> knownParticipantNodeIds;
    private final TransactionOutcomeClass[] outcomeClasses = TransactionOutcomeClass.values();

    public VFDTTransactionOutcomePredictor(List<String> knownParticipantNodeIds) {
        this.knownParticipantNodeIds = List.copyOf(knownParticipantNodeIds);

        ArrayList<Attribute> attributes = new ArrayList<>();
        for (String nodeId : this.knownParticipantNodeIds) {
            attributes.add(new Attribute("has_" + nodeId));
        }

        ArrayList<String> classLabels = new ArrayList<>();
        for (TransactionOutcomeClass outcomeClass : outcomeClasses) {
            classLabels.add(outcomeClass.name());
        }
        attributes.add(new Attribute("transaction_outcome", classLabels));

        this.datasetHeader = new Instances("TransactionOutcomeData", attributes, 0);
        this.datasetHeader.setClassIndex(attributes.size() - 1);

        this.vfdt = new HoeffdingTree();
        this.vfdt.leafpredictionOption.setChosenLabel("MC");
        this.vfdt.gracePeriodOption.setValue(2);
        this.vfdt.splitConfidenceOption.setValue(1.0e-3f);
        this.vfdt.tieThresholdOption.setValue(0.2f);
        this.vfdt.setModelContext(new InstancesHeader(datasetHeader));
        this.vfdt.prepareForUse();
    }

    public TransactionOutcomeClass predict(LogFeatures features) {
        Instance inst = createInstance(features, null);
        double[] votes = vfdt.getVotesForInstance(inst);

        int bestIndex = 0;
        for (int i = 1; i < votes.length; i++) {
            if (votes[i] > votes[bestIndex]) {
                bestIndex = i;
            }
        }
        return outcomeClasses[bestIndex];
    }

    public void train(LogFeatures features, TransactionOutcomeClass label) {
        Instance inst = createInstance(features, label);
        vfdt.trainOnInstance(inst);
    }

    private Instance createInstance(LogFeatures features, TransactionOutcomeClass label) {
        DenseInstance inst = new DenseInstance(datasetHeader.numAttributes());
        inst.setWeight(1.0);
        inst.setDataset(datasetHeader);
        int attributeIndex = 0;
        for (String nodeId : knownParticipantNodeIds) {
            inst.setValue(attributeIndex++, features.participantNodeIds().contains(nodeId) ? 1.0 : 0.0);
        }
        if (label != null) {
            inst.setClassValue(Arrays.asList(outcomeClasses).indexOf(label));
        }
        return inst;
    }

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
