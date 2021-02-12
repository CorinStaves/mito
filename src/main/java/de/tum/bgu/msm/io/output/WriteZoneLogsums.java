package de.tum.bgu.msm.io.output;

import de.tum.bgu.msm.data.MitoZone;
import de.tum.bgu.msm.data.ModeRestriction;
import de.tum.bgu.msm.data.Purpose;
import de.tum.bgu.msm.modules.tripDistribution.destinationChooser.zoneLogsumAverages;

import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import de.tum.bgu.msm.resources.Resources;
import de.tum.bgu.msm.util.MitoUtil;
import de.tum.bgu.msm.util.matrices.IndexedDoubleMatrix2D;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.log4j.Logger;

public class WriteZoneLogsums {

    private final static Logger logger = Logger.getLogger(WriteZoneLogsums.class);

    public static void writeAllLogsumData(Map<Integer, MitoZone> zones, List<Triple<Purpose, ModeRestriction, IndexedDoubleMatrix2D>> logsumMatrices) {
        String fileName = "logsumMatrices.csv";
        logger.info("Writing all logsum matrix data to a single file: " + fileName);
        String scenarioName = Resources.instance.getString(de.tum.bgu.msm.resources.Properties.SCENARIO_NAME);
        String file = Resources.instance.getBaseDirectory().toString() + "/scenOutput/" + scenarioName + "/zones/" + fileName;
        PrintWriter pw = MitoUtil.openFileForSequentialWriting(file, false);
        pw.println("purpose,modeRestriction,origin,destination,logsum");

        for (Triple<Purpose, ModeRestriction, IndexedDoubleMatrix2D> logsumMatrix : logsumMatrices) {
            Purpose purpose = logsumMatrix.getLeft();
            ModeRestriction modeRestriction = logsumMatrix.getMiddle();
            IndexedDoubleMatrix2D matrix = logsumMatrix.getRight();
            for (MitoZone origin : zones.values()) {
                int originId = origin.getId();
                for (MitoZone destination : zones.values()) {
                    int destinationId = destination.getId();
                    pw.println(purpose + "," + modeRestriction + "," + originId + "," + destinationId + "," + matrix.getIndexed(originId,destinationId));
                }
            }
        }
        pw.close();
    }

    public static void writeLogsumData(Map<Integer, MitoZone> zones, Triple<Purpose, ModeRestriction, IndexedDoubleMatrix2D> logsumMatrix) {
        Purpose purpose = logsumMatrix.getLeft();
        ModeRestriction modeRestriction = logsumMatrix.getMiddle();
        IndexedDoubleMatrix2D matrix = logsumMatrix.getRight();

        String fileName = purpose + "_" + modeRestriction + "_logsums.csv";
        logger.info("Writing logsum matrix to single file: " + fileName);

        String scenarioName = Resources.instance.getString(de.tum.bgu.msm.resources.Properties.SCENARIO_NAME);
        String file = Resources.instance.getBaseDirectory().toString() + "/scenOutput/" + scenarioName + "/zones/" + fileName;
        PrintWriter pw = MitoUtil.openFileForSequentialWriting(file, false);
        pw.println("origin,destination,logsum");

        for (MitoZone origin : zones.values()) {
            int originId = origin.getId();
            for (MitoZone destination : zones.values()) {
                int destinationId = destination.getId();
                pw.println(originId + "," + destinationId + "," + matrix.getIndexed(originId,destinationId));
            }
        }
        pw.close();

    }

}
