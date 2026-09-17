package ufc.victor;

import com.yahoo.labs.samoa.instances.Attribute;
import com.yahoo.labs.samoa.instances.DenseInstance;
import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.Instances;
import com.yahoo.labs.samoa.instances.InstancesHeader;
import moa.classifiers.trees.HoeffdingTree;
import ufc.victor.experiment.AlpsFeatureProfile;
import ufc.victor.experiment.AlpsFeatures;
import ufc.victor.experiment.CategoricalPrediction;
import ufc.victor.experiment.HistoricalMetrics;
import ufc.victor.experiment.ParticipantHistorySummary;
import ufc.victor.experiment.TransactionShape;
import ufc.victor.protocol.coordinator.node.NodeId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

public final class VfdtCategoricalModel<L extends Enum<L>> {

    private final HoeffdingTree vfdt;
    private final Instances datasetHeader;
    private final List<String> knownParticipantNodeIds;
    private final List<String> knownTransactionTypes;
    private final List<String> workloadFeatureNames;
    private final AlpsFeatureProfile featureProfile;
    private final Class<L> labelType;
    private final L[] labels;

    public VfdtCategoricalModel(
            List<String> knownParticipantNodeIds,
            List<String> knownTransactionTypes,
            List<String> workloadFeatureNames,
            AlpsFeatureProfile featureProfile,
            Class<L> labelType,
            String labelName
    ) {
        this.knownParticipantNodeIds = List.copyOf(knownParticipantNodeIds);
        this.knownTransactionTypes = List.copyOf(knownTransactionTypes);
        this.workloadFeatureNames = List.copyOf(workloadFeatureNames);
        this.featureProfile = Objects.requireNonNull(featureProfile, "featureProfile");
        this.labelType = Objects.requireNonNull(labelType, "labelType");
        this.labels = labelType.getEnumConstants();
        if (labels.length < 2) throw new IllegalArgumentException("labelType must have at least two values");
        if (labelName == null || labelName.isBlank()) {
            throw new IllegalArgumentException("labelName must not be blank");
        }

        ArrayList<Attribute> attributes = new ArrayList<>();
        addGenericAttributes(attributes);
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
        attributes.add(new Attribute(labelName, Arrays.stream(labels).map(Enum::name).toList()));

        datasetHeader = new Instances(labelName + "Data", attributes, 0);
        datasetHeader.setClassIndex(attributes.size() - 1);
        vfdt = new HoeffdingTree();
        vfdt.leafpredictionOption.setChosenLabel("MC");
        vfdt.gracePeriodOption.setValue(2);
        vfdt.splitConfidenceOption.setValue(1.0e-3f);
        vfdt.tieThresholdOption.setValue(0.2f);
        vfdt.setModelContext(new InstancesHeader(datasetHeader));
        vfdt.prepareForUse();
    }

    public CategoricalPrediction<L> predict(AlpsFeatures features) {
        double[] votes = vfdt.getVotesForInstance(createInstance(features, null));
        Map<L, Double> probabilities = probabilities(votes);
        L predicted = labels[0];
        for (L label : labels) {
            if (probabilities.get(label) > probabilities.get(predicted)) predicted = label;
        }
        return new CategoricalPrediction<>(predicted, probabilities, probabilities.get(predicted));
    }

    public void train(AlpsFeatures features, L label) {
        vfdt.trainOnInstance(createInstance(features, Objects.requireNonNull(label, "label")));
    }

    private Instance createInstance(AlpsFeatures features, L label) {
        Objects.requireNonNull(features, "features");
        DenseInstance instance = new DenseInstance(datasetHeader.numAttributes());
        instance.setWeight(1.0);
        instance.setDataset(datasetHeader);
        int index = setGenericFeatures(instance, features);
        if (featureProfile.includesWorkloadFeatures()) {
            for (String transactionType : knownTransactionTypes) {
                instance.setValue(index++, features.transactionContext().transactionType()
                        .equals(transactionType) ? 1.0 : 0.0);
            }
            for (String featureName : workloadFeatureNames) {
                instance.setValue(index++, features.transactionContext().workloadFeatures()
                        .getOrDefault(featureName, 0.0));
            }
        }
        if (featureProfile.includesNodeIdentity()) {
            for (String nodeId : knownParticipantNodeIds) {
                instance.setValue(index++, features.participantNodeIds().contains(nodeId) ? 1.0 : 0.0);
                index = setHistory(instance, index, features.participantHistory()
                        .getOrDefault(new NodeId(nodeId), HistoricalMetrics.empty()));
            }
        }
        if (label != null) instance.setClassValue(label.ordinal());
        return instance;
    }

    private int setGenericFeatures(Instance instance, AlpsFeatures features) {
        TransactionShape shape = features.transactionContext().shape();
        int index = 0;
        instance.setValue(index++, features.participantCount());
        instance.setValue(index++, features.transactionContext().distributed() ? 1.0 : 0.0);
        instance.setValue(index++, shape.readOperationCount());
        instance.setValue(index++, shape.writeOperationCount());
        instance.setValue(index++, shape.insertOperationCount());
        instance.setValue(index++, shape.updateOperationCount());
        instance.setValue(index++, shape.deleteOperationCount());
        instance.setValue(index++, shape.estimatedRowsTouched());
        instance.setValue(index++, shape.remoteOperationRatio());
        instance.setValue(index++, shape.readOnly() ? 1.0 : 0.0);
        index = setHistory(instance, index, features.globalHistory());
        return setParticipantSummary(instance, index, features.selectedParticipantHistory());
    }

    private Map<L, Double> probabilities(double[] votes) {
        EnumMap<L, Double> result = new EnumMap<>(labelType);
        double sum = 0.0;
        for (int index = 0; index < labels.length; index++) {
            if (index < votes.length && Double.isFinite(votes[index]) && votes[index] > 0.0) {
                sum += votes[index];
            }
        }
        for (int index = 0; index < labels.length; index++) {
            double vote = index < votes.length && Double.isFinite(votes[index])
                    ? Math.max(0.0, votes[index])
                    : 0.0;
            result.put(labels[index], sum == 0.0 ? 1.0 / labels.length : vote / sum);
        }
        return result;
    }

    private static void addGenericAttributes(ArrayList<Attribute> attributes) {
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

    private static int setHistory(Instance instance, int index, HistoricalMetrics history) {
        instance.setValue(index++, history.sampleCount());
        instance.setValue(index++, history.commitRate());
        instance.setValue(index++, history.averageLatencyMs());
        instance.setValue(index++, history.averageRedoBytes());
        instance.setValue(index++, history.averageLsnAdvanced());
        instance.setValue(index++, history.averageRedoLogWrites());
        instance.setValue(index++, history.averageRedoLogWaits());
        instance.setValue(index++, history.averageProtocolLogWrites());
        instance.setValue(index++, history.redoAvailabilityRate());
        instance.setValue(index++, history.resetRate());
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
            Instance instance,
            int index,
            ParticipantHistorySummary summary
    ) {
        instance.setValue(index++, summary.historyCoverage());
        instance.setValue(index++, summary.meanCommitRate());
        instance.setValue(index++, summary.minimumCommitRate());
        instance.setValue(index++, summary.meanLatencyMs());
        instance.setValue(index++, summary.maximumLatencyMs());
        instance.setValue(index++, summary.meanRedoBytes());
        instance.setValue(index++, summary.maximumRedoBytes());
        instance.setValue(index++, summary.meanLsnAdvanced());
        instance.setValue(index++, summary.maximumLsnAdvanced());
        instance.setValue(index++, summary.meanRedoLogWrites());
        instance.setValue(index++, summary.maximumRedoLogWrites());
        instance.setValue(index++, summary.meanRedoLogWaits());
        instance.setValue(index++, summary.maximumRedoLogWaits());
        instance.setValue(index++, summary.meanProtocolLogWrites());
        instance.setValue(index++, summary.maximumProtocolLogWrites());
        instance.setValue(index++, summary.minimumRedoAvailability());
        instance.setValue(index++, summary.maximumResetRate());
        return index;
    }

    public String learnedRules() {
        StringBuilder description = new StringBuilder();
        vfdt.getModelDescription(description, 0);
        return description.toString();
    }

    public int nodeCount() {
        return vfdt.getNodeCount();
    }

    public int treeDepth() {
        return vfdt.measureTreeDepth();
    }

    public List<String> featureNames() {
        return IntStream.range(0, datasetHeader.classIndex())
                .mapToObj(index -> datasetHeader.attribute(index).name())
                .toList();
    }
}
