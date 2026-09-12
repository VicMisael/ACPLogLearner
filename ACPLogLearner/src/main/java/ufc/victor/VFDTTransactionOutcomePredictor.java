package ufc.victor;

import com.yahoo.labs.samoa.instances.Attribute;
import com.yahoo.labs.samoa.instances.DenseInstance;
import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.Instances;
import com.yahoo.labs.samoa.instances.InstancesHeader;
import moa.classifiers.trees.HoeffdingTree;
import ufc.victor.experiment.AlpsFeatureProfile;
import ufc.victor.experiment.AlpsFeatures;
import ufc.victor.experiment.HistoricalMetrics;
import ufc.victor.experiment.ParticipantHistorySummary;
import ufc.victor.experiment.TransactionShape;
import ufc.victor.experiment.TransactionOutcomeClass;
import ufc.victor.protocol.coordinator.node.NodeId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

public class VFDTTransactionOutcomePredictor {

    private final HoeffdingTree vfdt;
    private final Instances datasetHeader;
    private final List<String> knownParticipantNodeIds;
    private final List<String> knownTransactionTypes;
    private final List<String> workloadFeatureNames;
    private final AlpsFeatureProfile featureProfile;
    private final TransactionOutcomeClass[] outcomeClasses = TransactionOutcomeClass.values();

    public VFDTTransactionOutcomePredictor(List<String> knownParticipantNodeIds) {
        this(knownParticipantNodeIds, List.of(), List.of(), AlpsFeatureProfile.GENERIC);
    }

    public VFDTTransactionOutcomePredictor(
            List<String> knownParticipantNodeIds,
            List<String> knownTransactionTypes,
            List<String> workloadFeatureNames,
            AlpsFeatureProfile featureProfile
    ) {
        this.knownParticipantNodeIds = List.copyOf(knownParticipantNodeIds);
        this.knownTransactionTypes = List.copyOf(knownTransactionTypes);
        this.workloadFeatureNames = List.copyOf(workloadFeatureNames);
        this.featureProfile = Objects.requireNonNull(featureProfile, "featureProfile");

        ArrayList<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("participant_count"));
        attributes.add(new Attribute("distributed"));
        attributes.add(new Attribute("read_operations"));
        attributes.add(new Attribute("write_operations"));
        attributes.add(new Attribute("insert_operations"));
        attributes.add(new Attribute("update_operations"));
        attributes.add(new Attribute("delete_operations"));
        attributes.add(new Attribute("estimated_rows_touched"));
        attributes.add(new Attribute("remote_operation_ratio"));
        attributes.add(new Attribute("read_only"));
        addHistoryAttributes(attributes, "global");
        addParticipantSummaryAttributes(attributes);
        if (featureProfile.includesWorkloadFeatures()) {
            for (String transactionType : this.knownTransactionTypes) {
                attributes.add(new Attribute("type_" + transactionType));
            }
            for (String featureName : this.workloadFeatureNames) {
                attributes.add(new Attribute("workload_" + featureName));
            }
        }
        if (featureProfile.includesNodeIdentity()) {
            for (String nodeId : this.knownParticipantNodeIds) {
                attributes.add(new Attribute("has_" + nodeId));
                addHistoryAttributes(attributes, nodeId);
            }
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
        return prediction(createInstance(features, null));
    }

    public TransactionOutcomeClass predict(AlpsFeatures features) {
        return prediction(createInstance(features, null));
    }

    private TransactionOutcomeClass prediction(Instance inst) {
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

    public void train(AlpsFeatures features, TransactionOutcomeClass label) {
        Instance inst = createInstance(features, label);
        vfdt.trainOnInstance(inst);
    }

    private Instance createInstance(LogFeatures features, TransactionOutcomeClass label) {
        DenseInstance inst = new DenseInstance(datasetHeader.numAttributes());
        inst.setWeight(1.0);
        inst.setDataset(datasetHeader);
        int attributeIndex = 0;
        inst.setValue(attributeIndex++, features.participantNodeIds().size());
        inst.setValue(attributeIndex++, features.participantNodeIds().size() > 1 ? 1.0 : 0.0);
        while (attributeIndex < datasetHeader.classIndex()) {
            inst.setValue(attributeIndex++, 0.0);
        }
        setLabel(inst, label);
        return inst;
    }

    private Instance createInstance(AlpsFeatures features, TransactionOutcomeClass label) {
        DenseInstance inst = new DenseInstance(datasetHeader.numAttributes());
        inst.setWeight(1.0);
        inst.setDataset(datasetHeader);
        int attributeIndex = 0;
        TransactionShape shape = features.transactionContext().shape();
        inst.setValue(attributeIndex++, features.participantCount());
        inst.setValue(attributeIndex++, features.transactionContext().distributed() ? 1.0 : 0.0);
        inst.setValue(attributeIndex++, shape.readOperationCount());
        inst.setValue(attributeIndex++, shape.writeOperationCount());
        inst.setValue(attributeIndex++, shape.insertOperationCount());
        inst.setValue(attributeIndex++, shape.updateOperationCount());
        inst.setValue(attributeIndex++, shape.deleteOperationCount());
        inst.setValue(attributeIndex++, shape.estimatedRowsTouched());
        inst.setValue(attributeIndex++, shape.remoteOperationRatio());
        inst.setValue(attributeIndex++, shape.readOnly() ? 1.0 : 0.0);
        attributeIndex = setHistory(inst, attributeIndex, features.globalHistory());
        attributeIndex = setParticipantSummary(
                inst, attributeIndex, features.selectedParticipantHistory()
        );
        if (featureProfile.includesWorkloadFeatures()) {
            for (String transactionType : knownTransactionTypes) {
                inst.setValue(
                        attributeIndex++,
                        features.transactionContext().transactionType().equals(transactionType) ? 1.0 : 0.0
                );
            }
            for (String featureName : workloadFeatureNames) {
                inst.setValue(
                        attributeIndex++,
                        features.transactionContext().workloadFeatures().getOrDefault(featureName, 0.0)
                );
            }
        }
        if (featureProfile.includesNodeIdentity()) {
            for (String nodeId : knownParticipantNodeIds) {
                inst.setValue(attributeIndex++, features.participantNodeIds().contains(nodeId) ? 1.0 : 0.0);
                HistoricalMetrics history = features.participantHistory()
                        .getOrDefault(new NodeId(nodeId), HistoricalMetrics.empty());
                attributeIndex = setHistory(inst, attributeIndex, history);
            }
        }
        setLabel(inst, label);
        return inst;
    }

    private void setLabel(Instance inst, TransactionOutcomeClass label) {
        if (label != null) {
            inst.setClassValue(Arrays.asList(outcomeClasses).indexOf(label));
        }
    }

    private static void addHistoryAttributes(ArrayList<Attribute> attributes, String prefix) {
        attributes.add(new Attribute(prefix + "_samples"));
        attributes.add(new Attribute(prefix + "_commit_rate"));
        attributes.add(new Attribute(prefix + "_latency_ms"));
        attributes.add(new Attribute(prefix + "_redo_bytes"));
        attributes.add(new Attribute(prefix + "_lsn_advanced"));
        attributes.add(new Attribute(prefix + "_redo_log_writes"));
        attributes.add(new Attribute(prefix + "_redo_log_waits"));
        attributes.add(new Attribute(prefix + "_protocol_log_writes"));
        attributes.add(new Attribute(prefix + "_redo_availability"));
        attributes.add(new Attribute(prefix + "_reset_rate"));
    }

    private static int setHistory(Instance inst, int index, HistoricalMetrics history) {
        inst.setValue(index++, history.sampleCount());
        inst.setValue(index++, history.commitRate());
        inst.setValue(index++, history.averageLatencyMs());
        inst.setValue(index++, history.averageRedoBytes());
        inst.setValue(index++, history.averageLsnAdvanced());
        inst.setValue(index++, history.averageRedoLogWrites());
        inst.setValue(index++, history.averageRedoLogWaits());
        inst.setValue(index++, history.averageProtocolLogWrites());
        inst.setValue(index++, history.redoAvailabilityRate());
        inst.setValue(index++, history.resetRate());
        return index;
    }

    private static void addParticipantSummaryAttributes(ArrayList<Attribute> attributes) {
        attributes.add(new Attribute("participants_history_coverage"));
        attributes.add(new Attribute("participants_mean_commit_rate"));
        attributes.add(new Attribute("participants_min_commit_rate"));
        attributes.add(new Attribute("participants_mean_latency_ms"));
        attributes.add(new Attribute("participants_max_latency_ms"));
        attributes.add(new Attribute("participants_mean_redo_bytes"));
        attributes.add(new Attribute("participants_max_redo_bytes"));
        attributes.add(new Attribute("participants_mean_lsn_advanced"));
        attributes.add(new Attribute("participants_max_lsn_advanced"));
        attributes.add(new Attribute("participants_mean_redo_log_writes"));
        attributes.add(new Attribute("participants_max_redo_log_writes"));
        attributes.add(new Attribute("participants_mean_redo_log_waits"));
        attributes.add(new Attribute("participants_max_redo_log_waits"));
        attributes.add(new Attribute("participants_mean_protocol_log_writes"));
        attributes.add(new Attribute("participants_max_protocol_log_writes"));
        attributes.add(new Attribute("participants_min_redo_availability"));
        attributes.add(new Attribute("participants_max_reset_rate"));
    }

    private static int setParticipantSummary(
            Instance inst,
            int index,
            ParticipantHistorySummary summary
    ) {
        inst.setValue(index++, summary.historyCoverage());
        inst.setValue(index++, summary.meanCommitRate());
        inst.setValue(index++, summary.minimumCommitRate());
        inst.setValue(index++, summary.meanLatencyMs());
        inst.setValue(index++, summary.maximumLatencyMs());
        inst.setValue(index++, summary.meanRedoBytes());
        inst.setValue(index++, summary.maximumRedoBytes());
        inst.setValue(index++, summary.meanLsnAdvanced());
        inst.setValue(index++, summary.maximumLsnAdvanced());
        inst.setValue(index++, summary.meanRedoLogWrites());
        inst.setValue(index++, summary.maximumRedoLogWrites());
        inst.setValue(index++, summary.meanRedoLogWaits());
        inst.setValue(index++, summary.maximumRedoLogWaits());
        inst.setValue(index++, summary.meanProtocolLogWrites());
        inst.setValue(index++, summary.maximumProtocolLogWrites());
        inst.setValue(index++, summary.minimumRedoAvailability());
        inst.setValue(index++, summary.maximumResetRate());
        return index;
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

    public List<String> getFeatureNames() {
        return IntStream.range(0, datasetHeader.classIndex())
                .mapToObj(index -> datasetHeader.attribute(index).name())
                .toList();
    }
}
