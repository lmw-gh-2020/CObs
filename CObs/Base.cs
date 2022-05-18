using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using EventStore.Client;

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
        ,ExceptionReadingEvents
        ,WrongNumberOfColumns
        ,DateUnreadable
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
            RowNumber = pIndex;
            RowStatus = pRowStatus;
        }
    }

    public class DayRaw
    {
        public int      TimelineIndex    { get; set; }
        public DateTime Date             { get; private set; }
        public int      DNC              { get; private set; }
        public int      Tests            { get; private set; }
        public double   Positivity       { get; private set; }
        public int      Mortality        { get; private set; }
        public int      Hospitalizations { get; private set; }

        public static bool Compare(DayRaw a, DayRaw b)
        {
            /*
                Equal date implies equal eventual timeline index, (but timeline
                index may be lazy-initialised).
            */
            return
                a.Date             == b.Date
            &&  a.DNC              == b.DNC
            &&  a.Tests            == b.Tests
            &&  a.Positivity       == b.Positivity
            &&  a.Mortality        == b.Mortality
            &&  a.Hospitalizations == b.Hospitalizations;
        }

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

        public DayRaw(int pIndex, SourceDay pDay)
        {
            TimelineIndex    = pIndex;

            Date             = DateTime.Parse(pDay.Date!);
            DNC              = pDay.DNC;
            Tests            = pDay.Tests;
            Positivity       = pDay.Positivity;
            Mortality        = pDay.Mortality;
            Hospitalizations = pDay.Hospitalizations;
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
        public IReadEventsAsync             ReadAdapter   { get; private set; }
        public Dictionary<DateTime, DayRaw> DaysRawByDate { get; private set; }
        public List<DayRaw>                 DaysRaw       { get; private set; }
        public List<DayRolling>             DaysRolling   { get; private set; }

        public BaseDays(IReadEventsAsync pReadAdapter)
        {
            ReadAdapter = pReadAdapter;

            DaysRawByDate = new Dictionary<DateTime, DayRaw>();
            DaysRaw       = new List<DayRaw>();
            DaysRolling   = new List<DayRolling>();
        }

        public BaseDays(IReadEventsAsync pReadAdapter, List<DayRaw> pDaysRaw)
        {
            ReadAdapter   = pReadAdapter;
            DaysRaw       = pDaysRaw;

            DaysRawByDate = new Dictionary<DateTime, DayRaw>();
            DaysRolling   = new List<DayRolling>();
        }

        private static SourceRowValidationStatus ValidateSourceRow(List<string> pRow)
        {
            /* validate a row of source data */
            if (pRow.Count != 6)
            {
                return SourceRowValidationStatus.WrongNumberOfColumns;
            }

            if (!DateTime.TryParse(pRow[0], out DateTime _))
            {
                return SourceRowValidationStatus.DateUnreadable;
            }

            if (!int.TryParse(pRow[1], out int dnc))
            {
                return SourceRowValidationStatus.DNCUnreadable;
            }

            if (dnc < 0)
            {
                return SourceRowValidationStatus.DNCNegative;
            }

            if (!int.TryParse(pRow[2], out int tests))
            {
                return SourceRowValidationStatus.TestsUnreadable;
            }

            if (tests < 0)
            {
                return SourceRowValidationStatus.TestsNegative;
            }

            if (!double.TryParse(pRow[3], out double positivity))
            {
                return SourceRowValidationStatus.PositivityUnreadable;
            }

            if (!(positivity >= 0 && positivity <= 100))
            {
                return SourceRowValidationStatus.PositivityNotBetweenZeroAndOneHundred;
            }

            if (!int.TryParse(pRow[4], out int mortality))
            {
                return SourceRowValidationStatus.MortalityUnreadable;
            }

            if (mortality < 0)
            {
                return SourceRowValidationStatus.MortalityNegative;
            }

            if (!int.TryParse(pRow[5], out int hospitalizations))
            {
                return SourceRowValidationStatus.HospitalizationsUnreadable;
            }

            if (hospitalizations < 0)
            {
                return SourceRowValidationStatus.HospitalizationsNegative;
            }

            return SourceRowValidationStatus.OK;
        }

        public static SourceRowValidationStatus ValidateSourceEvent(SourceDay pDay)
        {
            /* validate a source data event */
            if (!DateTime.TryParse(pDay.Date, out DateTime _))
            {
                return SourceRowValidationStatus.DateUnreadable;
            }

            if (pDay.DNC < 0)
            {
                return SourceRowValidationStatus.DNCNegative;
            }

            if (pDay.Tests < 0)
            {
                return SourceRowValidationStatus.TestsNegative;
            }

            if (!(pDay.Positivity >= 0 && pDay.Positivity <= 100))
            {
                return SourceRowValidationStatus.PositivityNotBetweenZeroAndOneHundred;
            }

            if (pDay.Mortality < 0)
            {
                return SourceRowValidationStatus.MortalityNegative;
            }

            if (pDay.Hospitalizations < 0)
            {
                return SourceRowValidationStatus.HospitalizationsNegative;
            }

            return SourceRowValidationStatus.OK;
        }

        public void AddDaysByDate(List<DayRaw> pDaysRaw)
        {
            foreach (var day in pDaysRaw) { DaysRawByDate.Add(day.Date, day); }
        }

        public SourceRowValidationStatus AddDay(List<string> pDay)
        {
            var status = ValidateSourceRow(pDay);

            if (status == SourceRowValidationStatus.OK)
            {
                var day = new DayRaw(0, pDay);

                DaysRawByDate.Add(day.Date, day);
            }

            return status;
        }

        public bool ValidateTimelineContiguityAndSeedIndex()
        {
            bool         contiguous = true;
            List<DayRaw> daysRaw    = DaysRawByDate
                .OrderBy(day => day.Key)
                .Select( day => day.Value)
                .ToList();

            if (daysRaw.Count > 1)
            {
                bool     first = true;
                DateTime date  = daysRaw[0].Date;

                foreach (var day in daysRaw)
                {
                    if (first) { first = false; continue; }

                    if (day.Date != date.AddDays(1)) { contiguous = false; break; }

                    date = date.AddDays(1);
                }
            }

            if (contiguous)
            {
                int index = 0;

                foreach (var day in daysRaw)
                {
                    day.TimelineIndex = index;

                    index++;
                }

                DaysRaw = daysRaw;
            }

            return contiguous;
        }

        public async Task<IReadActionResult> ReadDaysRawAsync()
        {
            var readResult = await ReadAdapter.ReadDaysRawAsync(this);

            if (readResult == null)
            {
                return new ReadActionResult(
                     false
                    ,"(unexpected null read result)"
                    ,new SourceValidationStatus(
                         false
                        ,0
                        ,SourceRowValidationStatus.ExceptionReadingEvents
                    )
                    ,0
                    ,Uuid.Empty
                    ,DateTime.Today
                );
            }

            return readResult;
        }

        public void PopulateRolling()
        {
            /* populate rolling averages */
            foreach (DayRaw raw in DaysRaw)
            {
                DaysRolling.Add(new DayRolling(raw.TimelineIndex, DaysRaw));
            }
        }
    }
}
