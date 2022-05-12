using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;

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
    public enum SourceRowValidationStatus
    {
         OK
        ,WrongNumberOfColumns
        ,DateUnreadable
        ,DateNotContiguous
        ,DNCUnreadable
        ,DNCNegative
        ,TestsUnreadable
        ,TestsNegative
        ,PositivityUnreadable
        ,PositivityNotBetweenZeroAndOneHundred
        ,MortalityUnreadable
        ,MortalityNegative
        ,HospitalizationsUnreadable
        ,HospitalizationsNegative
    }

    public class SourceValidationStatus
    {
        public bool                      SourceOK  { get; private set; }
        public int                       RowNumber { get; private set; }
        public SourceRowValidationStatus RowStatus { get; private set; }

        public SourceValidationStatus(
             bool                      pSourceOK
            ,int                       pIndex
            ,SourceRowValidationStatus pRowStatus
        ) {
            SourceOK  = pSourceOK;
            RowNumber = pIndex + 1;
            RowStatus = pRowStatus;
        }
    }

    public class DayRaw
    {
        public int      TimelineIndex    { get; private set; }
        public DateTime Date             { get; private set; }
        public int      DNC              { get; private set; }
        public int      Tests            { get; private set; }
        public double   Positivity       { get; private set; }
        public int      Mortality        { get; private set; }
        public int      Hospitalizations { get; private set; }

        public DayRaw(int pIndex, List<string> pRaw)
        {
            TimelineIndex    = pIndex;

            Date             = DateTime.Parse(pRaw[0]);
            DNC              = Int32.Parse(   pRaw[1]);
            Tests            = Int32.Parse(   pRaw[2]);
            Positivity       = Double.Parse(  pRaw[3]);
            Mortality        = Int32.Parse(   pRaw[4]);
            Hospitalizations = Int32.Parse(   pRaw[5]);
        }
    }

    public class DayRolling
    {
        public DayRaw Raw                           { get; private set; }
        public double Rolling5DayDNC                { get; private set; }
        public double Rolling5DayTests              { get; private set; }
        public double Rolling5DayPositivity         { get; private set; }
        public double Rolling5DayMortality          { get; private set; }
        public double Rolling5DayHospitalizations   { get; private set; }
        public double Rolling101DayMortality        { get; private set; }
        public double Rolling101DayHospitalizations { get; private set; }

        public DayRolling(int pIndex, List<DayRaw> pDaysRaw)
        {
            /* compute rolling averages */
            int minIndexShort = (
                (pIndex - 2)  > 0
            ) ? (pIndex - 2)  : 0;

            int minIndexLong  = (
                (pIndex - 50) > 0
            ) ? (pIndex - 50) : 0;

            int maxIndexShort = (
                (pIndex + 2)  < (pDaysRaw.Count - 1)
            ) ? (pIndex + 2)  : (pDaysRaw.Count - 1);

            int maxIndexLong  = (
                (pIndex + 50) < (pDaysRaw.Count - 1)
            ) ? (pIndex + 50) : (pDaysRaw.Count - 1);

            List<DayRaw> daysShort = pDaysRaw.Where(
                raw => (raw.TimelineIndex >= minIndexShort && raw.TimelineIndex <= maxIndexShort)
            ).ToList();

            List<DayRaw> daysLong  = pDaysRaw.Where(
                raw => (raw.TimelineIndex >= minIndexLong  && raw.TimelineIndex <= maxIndexLong)
            ).ToList();

            double m = (double)daysShort.Count;
            double n = (double)daysLong.Count;

            Raw                           = pDaysRaw[pIndex];

            Rolling5DayDNC                = daysShort.Select(raw => raw.DNC             ).Sum() / m;
            Rolling5DayTests              = daysShort.Select(raw => raw.Tests           ).Sum() / m;
            Rolling5DayPositivity         = daysShort.Select(raw => raw.Positivity      ).Sum() / m;
            Rolling5DayMortality          = daysShort.Select(raw => raw.Mortality       ).Sum() / m;
            Rolling5DayHospitalizations   = daysShort.Select(raw => raw.Hospitalizations).Sum() / m;
            Rolling101DayMortality        = daysShort.Select(raw => raw.Mortality       ).Sum() / n;
            Rolling101DayHospitalizations = daysShort.Select(raw => raw.Hospitalizations).Sum() / n;
        }
    }

    public class BaseDays
    {
        public List<DayRaw>     DaysRaw     { get; private set; }
        public List<DayRolling> DaysRolling { get; private set; }

        public BaseDays()
        {
            DaysRaw     = new List<DayRaw>();
            DaysRolling = new List<DayRolling>();
        }

        private SourceRowValidationStatus validateSourceRow(
             List<string> pRow
            ,DateTime?    pLastDate
        ) {
            /* validate a row of source data */
            DateTime date;
            int      dnc;
            int      tests;
            double   positivity;
            int      mortality;
            int      hospitalizations;

            if (pRow.Count != 6)
            {
                return SourceRowValidationStatus.WrongNumberOfColumns;
            }

            if (!DateTime.TryParse(pRow[0], out date))
            {
                return SourceRowValidationStatus.DateUnreadable;
            }

            if (
                (pLastDate != null)
            &&  (date.Date != pLastDate.Value.Date.AddDays(1))
            ) {
                return SourceRowValidationStatus.DateNotContiguous;
            }

            if (!int.TryParse(pRow[1], out dnc))
            {
                return SourceRowValidationStatus.DNCUnreadable;
            }

            if (dnc < 0)
            {
                return SourceRowValidationStatus.DNCNegative;
            }

            if (!int.TryParse(pRow[2], out tests))
            {
                return SourceRowValidationStatus.TestsUnreadable;
            }

            if (tests < 0)
            {
                return SourceRowValidationStatus.TestsNegative;
            }

            if (!double.TryParse(pRow[3], out positivity))
            {
                return SourceRowValidationStatus.PositivityUnreadable;
            }

            if (!(positivity >= 0 && positivity <= 100))
            {
                return SourceRowValidationStatus.PositivityNotBetweenZeroAndOneHundred;
            }

            if (!int.TryParse(pRow[4], out mortality))
            {
                return SourceRowValidationStatus.MortalityUnreadable;
            }

            if (mortality < 0)
            {
                return SourceRowValidationStatus.MortalityNegative;
            }

            if (!int.TryParse(pRow[5], out hospitalizations))
            {
                return SourceRowValidationStatus.HospitalizationsUnreadable;
            }

            if (hospitalizations < 0)
            {
                return SourceRowValidationStatus.HospitalizationsNegative;
            }

            return SourceRowValidationStatus.OK;
        }

        public SourceValidationStatus ReadDaysRaw(string pFileName)
        {
            int       index    = 0;
            DateTime? lastDate = null;

            /* read raw source data */
            using (StreamReader sr = new StreamReader(pFileName))
            {
                string? line;

                while ((line = sr.ReadLine()) != null)
                {
                    List<string> row = line.Split(',').ToList().Select(
                        entry => entry.Trim()
                    ).ToList();

                    SourceRowValidationStatus status = validateSourceRow(row, lastDate);

                    if (status != SourceRowValidationStatus.OK)
                    {
                        /* report validation error */
                        return new SourceValidationStatus(false, index, status);
                    }

                    DayRaw day = new DayRaw(index, row);

                    DaysRaw.Add(day);

                    lastDate = day.Date;

                    index++;
                }
            }

            /* populate rolling averages */
            foreach (DayRaw raw in DaysRaw)
            {
                DaysRolling.Add(new DayRolling(raw.TimelineIndex, DaysRaw));
            }

            /* report successful load */
            return new SourceValidationStatus(true, 0, SourceRowValidationStatus.OK);
        }
    }
}
