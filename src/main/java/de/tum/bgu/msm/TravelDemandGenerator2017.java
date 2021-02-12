package de.tum.bgu.msm;

import de.tum.bgu.msm.data.DataSet;
import de.tum.bgu.msm.io.output.SummarizeData;
import de.tum.bgu.msm.io.output.SummarizeDataToVisualize;
import de.tum.bgu.msm.io.output.TripGenerationWriter;
import de.tum.bgu.msm.modules.Module;
import de.tum.bgu.msm.modules.modeChoice.DominantCommuteMode;
import de.tum.bgu.msm.modules.modeChoice.ModeChoice;
import de.tum.bgu.msm.modules.modeChoice.ModeRestrictionChoice;
import de.tum.bgu.msm.modules.plansConverter.MatsimPopulationGenerator;
import de.tum.bgu.msm.modules.plansConverter.externalFlows.LongDistanceTraffic;
import de.tum.bgu.msm.modules.scaling.TripScaling;
import de.tum.bgu.msm.modules.timeOfDay.TimeOfDayChoice;
import de.tum.bgu.msm.modules.travelTimeBudget.DiscretionaryTravelTimeBudgetModule;
import de.tum.bgu.msm.modules.travelTimeBudget.MandatoryTravelTimeBudgetModule;
import de.tum.bgu.msm.modules.tripDistribution.DiscretionaryTripDistribution;
import de.tum.bgu.msm.modules.tripDistribution.MandatoryTripDistribution;
import de.tum.bgu.msm.modules.tripGeneration.DiscretionaryTripGeneration;
import de.tum.bgu.msm.modules.tripGeneration.MandatoryTripGeneration;
import de.tum.bgu.msm.modules.tripGeneration.TripsByPurposeGeneratorFactoryHurdle;
import de.tum.bgu.msm.resources.Properties;
import de.tum.bgu.msm.resources.Resources;
import org.apache.log4j.Logger;

import java.util.EnumSet;

import static de.tum.bgu.msm.data.Purpose.*;
import static de.tum.bgu.msm.data.Purpose.NHBO;

/**
 * Generates travel demand for the Microscopic Transport Orchestrator (MITO)
 *
 * @author Rolf Moeckel
 * Created on Sep 18, 2016 in Munich, Germany
 */
public final class TravelDemandGenerator2017 {

    private static final Logger logger = Logger.getLogger(TravelDemandGenerator2017.class);
    private final DataSet dataSet;

    private final Module mandatoryTripGeneration;
    private final Module mandatoryTravelTimeBudget;
    private final Module mandatoryDistribution;
    private final Module dominantCommuteMode;
    private final Module discretionaryTripGeneration;
    private final Module discretionaryTravelTimeBudget;
    private final Module discretionaryTripDistribution;
    private final Module modeRestriction;
    private final Module modeChoice;
    private final Module timeOfDayChoice;
    private final Module tripScaling;
    private final Module matsimPopulationGenerator;
    private final Module longDistanceTraffic;

    private TravelDemandGenerator2017(
            DataSet dataSet,
            Module mandatoryTripGeneration,
            Module mandatoryTravelTimeBudget,
            Module mandatoryDistribution,
            Module dominantCommuteMode,
            Module discretionaryTripGeneration,
            Module discretionaryTravelTimeBudget,
            Module discretionaryTripDistribution,
            Module modeRestriction,
            Module modeChoice,
            Module timeOfDayChoice,
            Module tripScaling,
            Module matsimPopulationGenerator,
            Module longDistanceTraffic) {

        this.dataSet = dataSet;
        this.mandatoryTripGeneration = mandatoryTripGeneration;
        this.mandatoryTravelTimeBudget = mandatoryTravelTimeBudget;
        this.mandatoryDistribution = mandatoryDistribution;
        this.dominantCommuteMode = dominantCommuteMode;
        this.discretionaryTripGeneration = discretionaryTripGeneration;
        this.discretionaryTravelTimeBudget = discretionaryTravelTimeBudget;
        this.discretionaryTripDistribution = discretionaryTripDistribution;
        this.modeRestriction = modeRestriction;
        this.modeChoice = modeChoice;
        this.timeOfDayChoice = timeOfDayChoice;
        this.tripScaling = tripScaling;
        this.matsimPopulationGenerator = matsimPopulationGenerator;
        this.longDistanceTraffic = longDistanceTraffic;
    }


    public static class Builder {

        private final DataSet dataSet;

        private Module mandatoryTripGeneration;
        private Module mandatoryTravelTimeBudget;
        private Module mandatoryDistribution;
        private Module dominantCommuteMode;
        private Module discretionaryTripGeneration;
        private Module discretionaryTravelTimeBudget;
        private Module discretionaryTripDistribution;
        private Module modeRestriction;
        private Module modeChoice;
        private Module timeOfDayChoice;
        private Module tripScaling;
        private Module matsimPopulationGenerator;
        private Module longDistanceTraffic;

        public Builder(DataSet dataSet) {
            this.dataSet = dataSet;
            mandatoryTripGeneration = new MandatoryTripGeneration(dataSet, new TripsByPurposeGeneratorFactoryHurdle());
            mandatoryTravelTimeBudget = new MandatoryTravelTimeBudgetModule(dataSet);
            mandatoryDistribution = new MandatoryTripDistribution(dataSet);
            dominantCommuteMode = new DominantCommuteMode(dataSet);
            discretionaryTripGeneration = new DiscretionaryTripGeneration(dataSet, new TripsByPurposeGeneratorFactoryHurdle());
            discretionaryTravelTimeBudget = new DiscretionaryTravelTimeBudgetModule(dataSet);
            discretionaryTripDistribution = new DiscretionaryTripDistribution(dataSet);
            modeRestriction = new ModeRestrictionChoice(dataSet);
            modeChoice = new ModeChoice(dataSet);
            timeOfDayChoice = new TimeOfDayChoice(dataSet);
            tripScaling = new TripScaling(dataSet);
            matsimPopulationGenerator = new MatsimPopulationGenerator(dataSet);
            if (Resources.instance.getBoolean(Properties.ADD_EXTERNAL_FLOWS, false)) {
                longDistanceTraffic = new LongDistanceTraffic(dataSet, Double.parseDouble(Resources.instance.getString(Properties.TRIP_SCALING_FACTOR)));
            }
        }

        public TravelDemandGenerator2017 build() {
            return new TravelDemandGenerator2017(dataSet,
                    mandatoryTripGeneration,
                    mandatoryTravelTimeBudget,
                    mandatoryDistribution,
                    dominantCommuteMode,
                    discretionaryTripGeneration,
                    discretionaryTravelTimeBudget,
                    discretionaryTripDistribution,
                    modeRestriction,
                    modeChoice,
                    timeOfDayChoice,
                    tripScaling,
                    matsimPopulationGenerator,
                    longDistanceTraffic);
        }

        public void setMandatoryTripGeneration(Module mandatoryTripGeneration) {
            this.mandatoryTripGeneration = mandatoryTripGeneration;
        }

        public void setTravelTimeBudget(Module mandatoryTravelTimeBudget) {
            this.mandatoryTravelTimeBudget = mandatoryTravelTimeBudget;
        }

        public void setMandatoryDistribution(Module mandatoryDistribution) {
            this.mandatoryDistribution = mandatoryDistribution;
        }

        public void setDominantCommuteMode(Module mandatoryDistribution) {
            this.dominantCommuteMode = dominantCommuteMode;
        }

        public void setDiscretionaryTripGeneration(Module discretionaryTripGeneration) {
            this.discretionaryTripGeneration = discretionaryTripGeneration;
        }

        public void setDiscretionaryTravelTimeBudget(Module discretionaryTravelTimeBudget) {
            this.discretionaryTravelTimeBudget = discretionaryTravelTimeBudget;
        }

        public void setDiscretionaryTripDistribution(Module discretionaryTripDistribution) {
            this.discretionaryTripDistribution = discretionaryTripDistribution;
        }

        public void setModeRestriction(Module modeRestriction) {
            this.modeRestriction = modeRestriction;
        }

        public void setModeChoice(Module modeChoice) {
            this.modeChoice = modeChoice;
        }

        public void setTimeOfDayChoice(Module timeOfDayChoice) {
            this.timeOfDayChoice = timeOfDayChoice;
        }

        public void setTripScaling(Module tripScaling) {
            this.tripScaling = tripScaling;
        }

        public void setMatsimPopulationGenerator(Module matsimPopulationGenerator) {
            this.matsimPopulationGenerator = matsimPopulationGenerator;
        }

        public void setLongDistanceTraffic(Module longDistanceTraffic) {
            this.longDistanceTraffic = longDistanceTraffic;
        }

        public DataSet getDataSet() {
            return dataSet;
        }

        public Module getMandatoryTripGeneration() {
            return mandatoryTripGeneration;
        }

        public Module getMandatoryTravelTimeBudget() {
            return mandatoryTravelTimeBudget;
        }

        public Module getMandatoryDistribution() {
            return mandatoryDistribution;
        }

        public Module getModeChoice() {
            return modeChoice;
        }

        public Module getTimeOfDayChoice() {
            return timeOfDayChoice;
        }

        public Module getTripScaling() {
            return tripScaling;
        }

        public Module getMatsimPopulationGenerator() {
            return matsimPopulationGenerator;
        }

        public Module getLongDistanceTraffic() {
            return longDistanceTraffic;
        }
    }

    public void generateTravelDemand(String scenarioName) {

        long startTime = System.currentTimeMillis();
        logger.info("Running Module: Mandatory Microscopic Trip Generation");
        mandatoryTripGeneration.run();
        if (dataSet.getTrips().isEmpty()) {
            logger.warn("No trips created. End of program.");
            return;
        }
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000;
        logger.info("Completed TG in " + duration + " seconds");

        logger.info("Running Module: Mandatory Travel Time Budget");
        mandatoryTravelTimeBudget.run();

        logger.info("Running Module: Mandatory trip distribution");
        mandatoryDistribution.run();

        logger.info("Running Module: Dominant commute mode");
        dominantCommuteMode.run();

        logger.info("Running Module: Discretionary trip generation");
        discretionaryTripGeneration.run();

        logger.info("Running Module: Mode Restriction");
        modeRestriction.run();

        logger.info("Running Module: Discretionary Travel Time Budget");
        discretionaryTravelTimeBudget.run();

        logger.info("Running Module: Discretionary trip distribution");
        discretionaryTripDistribution.run();

//        SummarizeData.writeOutSyntheticPopulationWithTrips(dataSet);
//        SummarizeData.writeOutTrips(dataSet, scenarioName);
        System.exit(0);

        logger.info("Running Module: Trip to Mode Assignment (Mode Choice)");
        modeChoice.run();

        logger.info("Running time of day choice");
        timeOfDayChoice.run();

        logger.info("Running trip scaling");
        tripScaling.run();

        matsimPopulationGenerator.run();

        if (Resources.instance.getBoolean(Properties.ADD_EXTERNAL_FLOWS, false)) {
            longDistanceTraffic.run();
        }

        TripGenerationWriter.writeTripsByPurposeAndZone(dataSet, scenarioName);
        SummarizeDataToVisualize.writeFinalSummary(dataSet, scenarioName);

        if (Resources.instance.getBoolean(Properties.PRINT_MICRO_DATA, true)) {
            SummarizeData.writeOutSyntheticPopulationWithTrips(dataSet);
            SummarizeData.writeOutTrips(dataSet, scenarioName);
        }
        if (Resources.instance.getBoolean(Properties.CREATE_CHARTS, true)) {
            //DistancePlots.writeDistanceDistributions(dataSet, scenarioName);
            //ModeChoicePlots.writeModeChoice(dataSet, scenarioName);
            SummarizeData.writeCharts(dataSet, scenarioName);
        }
        if (Resources.instance.getBoolean(Properties.WRITE_MATSIM_POPULATION, true)) {
            //SummarizeData.writeMatsimPlans(dataSet, scenarioName);
        }
    }
}
