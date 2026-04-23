package ufc.victor.experiment;

import ufc.victor.LogFeatures;

public class ParticipantSetFeatureExtractor {

    public LogFeatures extract(Scenario scenario) {
        return new LogFeatures(scenario.participantNodeIds().stream().sorted().toList());
    }
}
