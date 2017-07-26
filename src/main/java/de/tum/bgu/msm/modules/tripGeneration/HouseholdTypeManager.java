package de.tum.bgu.msm.modules.tripGeneration;

import com.pb.common.datafile.TableDataSet;
import de.tum.bgu.msm.data.DataSet;
import de.tum.bgu.msm.data.MitoHousehold;
import de.tum.bgu.msm.resources.Resources;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Nico on 20.07.2017.
 */
public class HouseholdTypeManager {

    private static Logger logger = Logger.getLogger(HouseholdTypeManager.class);

    private final DataSet dataSet;
    private final String purpose;

    private List<HouseholdType> householdTypes = new ArrayList();

    public HouseholdTypeManager(DataSet dataSet, String purpose) {
        this.dataSet = dataSet;
        this.purpose = purpose;
    }

    public void createHouseHoldTypeDefinitions() {
        String[] householdDefinitionToken = Resources.INSTANCE.getArray("hh.type." + purpose);
        String sizeToken = householdDefinitionToken[2];
        String[] sizePortions = sizeToken.split("\\.");
        String workerToken = householdDefinitionToken[3];
        String[] workerPortions = workerToken.split("\\.");
        String incomeToken = householdDefinitionToken[4];
        String[] incomePortions = incomeToken.split("\\.");
        String autoToken = householdDefinitionToken[5];
        String[] autoPortions = autoToken.split("\\.");
        String regionToken = householdDefinitionToken[6];
        String[] regionPortions = regionToken.split("\\.");

        householdTypes = createHouseholdTypes(sizePortions, workerPortions,
                incomePortions, autoPortions, regionPortions);

    }

    private List<HouseholdType> createHouseholdTypes(String[] sizePortions, String[] workerPortions,
                                                     String[] incomePortions, String[] autoPortions, String[] regionPortions) {
        List<HouseholdType> houseHoldTypes = new ArrayList<>();
        int id = 0;
        for (String sizeToken : sizePortions) {
            String[] sizeParts = sizeToken.split("-");
            for (String workerToken : workerPortions) {
                String[] workerParts = workerToken.split("-");
                for (String incomeToken : incomePortions) {
                    String[] incomeParts = incomeToken.split("-");
                    for (String autoToken : autoPortions) {
                        String[] autoParts = autoToken.split("-");
                        for (String regionToken : regionPortions) {
                            String[] regionParts = regionToken.split("-");
                            final int size_l = Integer.parseInt(sizeParts[0]);
                            final int size_h = Integer.parseInt(sizeParts[1]);
                            final int workers_l = Integer.parseInt(workerParts[0]) - 1;
                            final int workers_h = Integer.parseInt(workerParts[1]) - 1;
                            final int income_l = Integer.parseInt(incomeParts[0]);
                            final int income_h = Integer.parseInt(incomeParts[1]);
                            final int autos_l = Integer.parseInt(autoParts[0]) - 1;
                            final int autos_h = Integer.parseInt(autoParts[1]) - 1;
                            final int region_l = Integer.parseInt(regionParts[0]);
                            final int region_h = Integer.parseInt(regionParts[1]);

                            houseHoldTypes.add(new HouseholdType(id, size_l, size_h, workers_l, workers_h,
                                    income_l, income_h, autos_l, autos_h, region_l,
                                    region_h));
                            id++;
                        }
                    }
                }
            }
        }
        return houseHoldTypes;
    }

    Map<Integer, HouseholdType> assignHouseholdTypeOfEachSurveyRecord() {
        // Count number of household records per predefined type

        Map<Integer, HouseholdType> householdTypeBySample = new HashMap<>();
        TableDataSet travelSurveyHouseholdTable = dataSet.getTravelSurveyHouseholdTable();

        for (int row = 1; row <= travelSurveyHouseholdTable.getRowCount(); row++) {
            int hhSze = (int) travelSurveyHouseholdTable.getValueAt(row, "hhsiz");
            hhSze = Math.min(hhSze, 7);    // hhsiz 8 has only 19 records, aggregate with hhsiz 7
            int hhWrk = (int) travelSurveyHouseholdTable.getValueAt(row, "hhwrk");
            hhWrk = Math.min(hhWrk, 4);    // hhwrk 6 has 1 and hhwrk 5 has 7 records, aggregate with hhwrk 4
            int hhInc = (int) travelSurveyHouseholdTable.getValueAt(row, "incom");
            int hhVeh = (int) travelSurveyHouseholdTable.getValueAt(row, "hhveh");
            hhVeh = Math.min(hhVeh, 3);   // Auto-ownership model will generate groups 0, 1, 2, 3+ only.
            int region = (int) travelSurveyHouseholdTable.getValueAt(row, "urbanSuburbanRural");
            int sampleId = (int) travelSurveyHouseholdTable.getValueAt(row, "sampn");
            HouseholdType type = determineHouseholdType(hhSze, hhWrk, hhInc, hhVeh, region);
            householdTypeBySample.put(sampleId, type);

        }
        // analyze if every household type has a sufficient number of records
        cancelOutInsufficientRecords(householdTypeBySample);
        return householdTypeBySample;
    }

    HouseholdType determineHouseholdType(MitoHousehold hh) {
        int incCategory = translateIncomeIntoCategory(hh.getIncome());
        return determineHouseholdType(hh.getHhSize(), hh.getNumberOfWorkers(),
                incCategory, hh.getAutos(), dataSet.getZones().get(hh.getHomeZone()).getRegion());
    }

    HouseholdType determineHouseholdType(int hhSze, int hhWrk, int hhInc, int hhVeh, int hhReg) {

        hhSze = Math.min(hhSze, 7);
        hhWrk = Math.min(hhWrk, 4);

        int hhAut;
        String autoDef = selectAutoMode();

        if (autoDef.equalsIgnoreCase("autos")) {
            hhAut = Math.min(hhVeh, 3);
        } else {
            if (hhVeh < hhWrk) {
                hhAut = 0;        // fewer autos than workers
            } else if (hhVeh == hhWrk) {
                hhAut = 1;  // equal number of autos and workers
            } else {
                hhAut = 2;                      // more autos than workers
            }
        }

        for (HouseholdType type : householdTypes) {
            if (type.applies(hhSze, hhWrk, hhInc, hhAut, hhReg)) {
                return type;
            }
        }
        logger.error("Could not define household type: " + hhSze + " " + hhWrk + " " + hhInc + " " + hhVeh + " " + hhReg);
        return null;
    }

    private String selectAutoMode() {
        // return autos or autoSufficiency depending on mode chosen
        String autoMode = "autos";
        if (purpose.equalsIgnoreCase("HBW") || purpose.equalsIgnoreCase("NHBW")) autoMode = "autoSufficiency";
        return autoMode;
    }

    private void cancelOutInsufficientRecords(Map<Integer, HouseholdType> householdTypeBySample) {
        for (Map.Entry<Integer, HouseholdType> entry : householdTypeBySample.entrySet()) {
            if (entry.getValue().getNumberOfRecords() < 30) {
                entry.setValue(null);  // marker that this hhTypeDef is not worth analyzing
            }
        }
    }

    private int translateIncomeIntoCategory(int hhIncome) {
        // translate income in absolute dollars into household travel survey income categories

        if (hhIncome < 10000) return 1;
        else if (hhIncome >= 10000 && hhIncome < 15000) return 2;
        else if (hhIncome >= 15000 && hhIncome < 30000) return 3;
        else if (hhIncome >= 30000 && hhIncome < 40000) return 4;
        else if (hhIncome >= 40000 && hhIncome < 50000) return 5;
        else if (hhIncome >= 50000 && hhIncome < 60000) return 6;
        else if (hhIncome >= 60000 && hhIncome < 75000) return 7;
        else if (hhIncome >= 75000 && hhIncome < 100000) return 8;
        else if (hhIncome >= 100000 && hhIncome < 125000) return 9;
        else if (hhIncome >= 125000 && hhIncome < 150000) return 10;
        else if (hhIncome >= 150000 && hhIncome < 200000) return 11;
        else if (hhIncome >= 200000) return 12;
        logger.error("Unknown HTS income: " + hhIncome);
        return -1;
    }
}
