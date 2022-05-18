using System.Collections.Generic;
using System.Linq;

/*
*
* Author:  lmw.gh.2020@gmail.com, all rights reserved, October 2020
* License: Apache License, Version 2.0
*
* https://opensource.org/licenses/Apache-2.0 
*
*/

namespace CObs
{
    public class ScenarioParameters
    {
        public double IFR                           { get; private set; }
        public int    MedianTimeFromExposureToTest  { get; private set; }
        public int    MedianTimeToAdmission         { get; private set; }
        public int    MedianHospitalizationDuration { get; private set; }
        public int    MedianTimeToMortality         { get; private set; }
        public double MedianSerialInterval          { get; private set; }
        public double TransmissibilityWindowToSI    { get; private set; }

        public ScenarioParameters(
             double pIFR
            ,int    pMedianTimeFromExposureToTest
            ,int    pMedianTimeToAdmission
            ,int    pMedianHospitalizationDuration
            ,int    pMedianTimeToMortality
            ,double pMedianSerialInterval
            ,double pTransmissibilityWindowToSI
        ) {
            IFR                           = pIFR;
            MedianTimeToAdmission         = pMedianTimeToAdmission;
            MedianHospitalizationDuration = pMedianHospitalizationDuration;
            MedianTimeToMortality         = pMedianTimeToMortality;
            MedianTimeFromExposureToTest  = pMedianTimeFromExposureToTest;
            MedianSerialInterval          = pMedianSerialInterval;
            TransmissibilityWindowToSI    = pTransmissibilityWindowToSI;
        }
    }

    public class AllScenarios
    {
        /* civic population size for dataset (to 2 significant figures) */
        public const int      Population = 10000000;

        public double[]       IFRValues                           { get; private set; }
        public int[]          MedianTimeFromExposureToTestValues  { get; private set; }
        public int[]          MedianTimeToAdmissionValues         { get; private set; }
        public int[]          MedianHospitalizationDurationValues { get; private set; }
        public int[]          MedianTimeToMortalityValues         { get; private set; }
        public double[]       MedianSerialIntervalValues          { get; private set; }
        public double[]       TransmissibilityWindowToSIValues    { get; private set; }

        public List<Scenario> Scenarios                           { get; private set; }

        public AllScenarios()
        {
            /* parameter ranges pulled from a manual (non-systematic) review of the literature */
            IFRValues                           = new double[] { 0.006 ,0.008 ,0.01 };
            MedianTimeFromExposureToTestValues  = new int[]    { 4     ,5     ,7    };
            MedianTimeToAdmissionValues         = new int[]    { 7     ,10    ,14   };
            MedianHospitalizationDurationValues = new int[]    { 7     ,10    ,14   };
            MedianTimeToMortalityValues         = new int[]    { 26    ,30    ,34   };
            MedianSerialIntervalValues          = new double[] { 3.5   ,4.5   ,7    };
            TransmissibilityWindowToSIValues    = new double[] { 0.8   ,0.6   ,0.5  };

            Scenarios                           = new List<Scenario>();
        }

        public void GenerateScenarioParameters(BaseDays pBaseDays)
        {
            var allParameterSets =
                from IFR                           in IFRValues
                from MedianTimeFromExposureToTest  in MedianTimeFromExposureToTestValues
                from MedianTimeToAdmission         in MedianTimeToAdmissionValues
                from MedianHospitalizationDuration in MedianHospitalizationDurationValues
                from MedianTimeToMortality         in MedianTimeToMortalityValues
                from MedianSerialInterval          in MedianSerialIntervalValues
                from TransmissibilityWindowToSI    in TransmissibilityWindowToSIValues
                select new {
                     IFR
                    ,MedianTimeFromExposureToTest
                    ,MedianTimeToAdmission
                    ,MedianHospitalizationDuration
                    ,MedianTimeToMortality
                    ,MedianSerialInterval
                    ,TransmissibilityWindowToSI
                };

            foreach (var parameters in allParameterSets)
            {
                /*
                    Exclude scenarios with no gap from test-to-admission or admission-to-mortality.
                    Such scenarios are unrealistic, and would complicate model region handover
                    logic.
                */

                if (
                    parameters.MedianTimeToMortality > parameters.MedianTimeToAdmission
                &&  parameters.MedianTimeToAdmission > parameters.MedianTimeFromExposureToTest
                ) {
                    Scenarios.Add(
                        new Scenario(
                             pBaseDays
                            ,new ScenarioParameters(
                                 parameters.IFR
                                ,parameters.MedianTimeFromExposureToTest
                                ,parameters.MedianTimeToAdmission
                                ,parameters.MedianHospitalizationDuration
                                ,parameters.MedianTimeToMortality
                                ,parameters.MedianSerialInterval
                                ,parameters.TransmissibilityWindowToSI
                            )
                        )
                    );
                }
            }
        }
    }
}
