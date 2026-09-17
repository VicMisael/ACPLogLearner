package ufc.victor.experiment;

public interface TrialStateController<SPEC, SNAPSHOT> {
    SNAPSHOT capture(SPEC specification) throws Exception;

    void restoreAndVerify(SNAPSHOT snapshot) throws Exception;
}
